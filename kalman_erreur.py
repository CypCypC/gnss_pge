import json
import math
import time
import threading
import uuid
from dataclasses import dataclass, asdict

import numpy as np
import paho.mqtt.client as mqtt


# ============================================================
# MQTT CONFIG
#   - INPUT: localhost (IMU + GNSS)
#   - OUTPUT: neocampus (EST)
# ============================================================
MQTT_IN_HOST = "127.0.0.1"
MQTT_IN_PORT = 1883
MQTT_IN_USERNAME = None
MQTT_IN_PASSWORD = None
MQTT_IN_KEEPALIVE = 60

MQTT_OUT_HOST = "neocampus.univ-tlse3.fr"
MQTT_OUT_PORT = 10883
MQTT_OUT_USERNAME = "test"
MQTT_OUT_PASSWORD = "test"
MQTT_OUT_KEEPALIVE = 120

TOPIC_IMU = "TestTopic/VACOP/localisation/imu"
TOPIC_GPS = "TestTopic/VACOP/localisation/gnss"
TOPIC_EST = "TestTopic/VACOP/localisation/eskf"

MQTT_CLIENT_ID_IN = f"eskf_in_{uuid.uuid4().hex[:8]}"
MQTT_CLIENT_ID_OUT = f"eskf_out_{uuid.uuid4().hex[:8]}"

DEBUG = True

# Ignore GPS if older than filter time by more than this
MAX_GPS_LAG_S = 0.30

# Cap dt for IMU propagation (avoid huge jumps)
MAX_DT = 0.2

# If your accel includes gravity (most IMUs do), set True.
ACCEL_INCLUDES_GRAVITY = True
G = 9.81

# Publish rate
PUB_HZ = 10.0

# ============================================================
# GNSS dynamic confidence
# ============================================================
# If your GNSS field "vAcc_m" is actually *velocity accuracy* (m/s), set True.
# If it's *vertical accuracy* (m), set False to keep sigma_v fixed.
VACC_IS_VELOCITY_ACCURACY = True

# Smooth GNSS accuracies (EMA): avoids jumps in R
USE_GNSS_ACC_EMA = True
GNSS_ACC_EMA_ALPHA = 0.2  # 0.1..0.3 typical

# Safety clamps
HACC_MIN_M, HACC_MAX_M = 0.2, 50.0      # 20 cm .. 50 m
VACC_MIN, VACC_MAX = 0.05, 20.0         # if velocity acc: 5 cm/s .. 20 m/s

# ============================================================
# Helpers
# ============================================================
EARTH_RADIUS_M = 6378137.0  # WGS84 approx


def wrap_pi(a: float) -> float:
    return (a + math.pi) % (2.0 * math.pi) - math.pi


def wrap_360(deg: float) -> float:
    return (deg % 360.0 + 360.0) % 360.0


def latlon_to_local_EN(lat_deg: float, lon_deg: float, lat0_deg: float, lon0_deg: float):
    # Small area approximation (good for city-scale)
    lat = math.radians(lat_deg)
    lon = math.radians(lon_deg)
    lat0 = math.radians(lat0_deg)
    lon0 = math.radians(lon0_deg)
    dlat = lat - lat0
    dlon = lon - lon0
    north = dlat * EARTH_RADIUS_M
    east = dlon * EARTH_RADIUS_M * math.cos(lat0)
    return float(east), float(north)


def get_msg_ts_ms(d: dict) -> int:
    """
    Supported formats:
      - timestamp (seconds float)  -> ms
      - ts_sensor_ms
      - ts_ms
    """
    if "timestamp" in d:
        return int(float(d["timestamp"]) * 1000)
    if "ts_sensor_ms" in d:
        return int(d["ts_sensor_ms"])
    if "ts_ms" in d:
        return int(d["ts_ms"])
    return int(time.time() * 1000)


def get_vec3(d: dict, key: str):
    v = d.get(key, None)
    if not isinstance(v, dict):
        return None
    try:
        return float(v["x"]), float(v["y"]), float(v["z"])
    except Exception:
        return None


def body_xy_to_EN(ax_body: float, ay_body: float, yaw_rad: float):
    cy = math.cos(yaw_rad)
    sy = math.sin(yaw_rad)
    # same mapping as your original
    aE = sy * ax_body + cy * ay_body
    aN = cy * ax_body - sy * ay_body
    return aE, aN


# ============================================================
# Output message
# ============================================================
@dataclass
class EstMsg:
    ts_ms: int
    lat: float | None
    lon: float | None
    x_e: float | None
    y_n: float | None
    vx_e: float
    vy_n: float
    yaw_deg: float
    bax_e: float
    bay_n: float
    P_latlon: list
    P_en_m: list
    hAcc_m_used: float | None
    vAcc_used: float | None


# ============================================================
# ESKF 2D (Nominal state in LAT/LON, yaw not estimated)
# State: [lat, lon, vE, vN, b_aE, b_aN]
# Error-state covariance stored with lat/lon errors in radians.
# ============================================================
class ESKF2D_LatLon_NoYaw:
    def __init__(self):
        # nominal
        self.lat_deg = None
        self.lon_deg = None
        self.v = np.zeros(2)     # [vE, vN] (m/s)
        self.yaw = 0.0           # copied from heading_deg (rad)
        self.b_a = np.zeros(2)   # [b_aE, b_aN] (m/s^2)

        # covariance on error-state: [dlat, dlon, dvE, dvN, dbaE, dbaN]
        self.P = np.eye(6) * 10.0

        self.t_filter_ms = None
        self.t_last_imu_ms = None
        self.t_last_gps_ms = None

        # origin for derived EN output
        self.origin_set = False
        self.lat0 = None
        self.lon0 = None
        self.origin_samples_needed = 50
        self._gps_lat_buf = []
        self._gps_lon_buf = []

        # noises (fallback defaults)
        self.sigma_a = 0.35         # accel noise (m/s^2)
        self.sigma_ba = 0.08        # accel bias RW (m/s^2 / sqrt(s))
        self.sigma_lat_gps_m = 1.5  # fallback GPS horiz sigma (m)
        self.sigma_v_gps = 0.4      # fallback GPS vel sigma (m/s)

        self.max_dt = MAX_DT

        # dynamic GNSS accuracy (EMA)
        self.hAcc_ema = None
        self.vAcc_ema = None
        self.ema_alpha = GNSS_ACC_EMA_ALPHA
        self.last_hAcc_used = None
        self.last_vAcc_used = None

        # linearization matrices
        self.F = np.eye(6)
        self.Gm = np.zeros((6, 4))
        self.H_gps = np.zeros((4, 6))
        self.I6 = np.eye(6)

        # z = [lat_rad, lon_rad, vE, vN]
        self.H_gps[0, 0] = 1.0
        self.H_gps[1, 1] = 1.0
        self.H_gps[2, 2] = 1.0
        self.H_gps[3, 3] = 1.0

    def _ensure_origin(self, lat_deg: float, lon_deg: float) -> bool:
        if self.origin_set:
            return True

        self._gps_lat_buf.append(lat_deg)
        self._gps_lon_buf.append(lon_deg)
        n = len(self._gps_lat_buf)

        if DEBUG and n in (1, 5, 10, 25, self.origin_samples_needed):
            print(f"[GPS] collecting origin samples: {n}/{self.origin_samples_needed}", flush=True)

        if n < self.origin_samples_needed:
            return False

        self.lat0 = float(np.mean(self._gps_lat_buf))
        self.lon0 = float(np.mean(self._gps_lon_buf))
        self.origin_set = True

        if DEBUG:
            print(f"[GPS] origin set: lat0={self.lat0:.8f}, lon0={self.lon0:.8f}", flush=True)
        return True

    def propagate_with_imu(self, imu: dict) -> bool:
        t_ms = get_msg_ts_ms(imu)

        if self.t_last_imu_ms is None:
            self.t_last_imu_ms = t_ms
            self.t_filter_ms = t_ms
            if "heading_deg" in imu:
                self.yaw = wrap_pi(math.radians(float(imu["heading_deg"])))
            return False

        dt = (t_ms - self.t_last_imu_ms) / 1000.0
        if dt <= 0.0:
            return False
        if dt > self.max_dt:
            dt = self.max_dt

        self.t_last_imu_ms = t_ms
        self.t_filter_ms = t_ms

        if "heading_deg" in imu:
            self.yaw = wrap_pi(math.radians(float(imu["heading_deg"])))

        acc = get_vec3(imu, "accel_mps2")
        if acc is None:
            return False

        ax_b, ay_b, az_b = acc
        if ACCEL_INCLUDES_GRAVITY:
            az_b = az_b - G  # unused in 2D

        aE, aN = body_xy_to_EN(ax_b, ay_b, self.yaw)
        a = np.array([aE, aN]) - self.b_a

        # --- nominal propagation ---
        self.v = self.v + a * dt

        if self.lat_deg is not None and self.lon_deg is not None:
            lat_rad = math.radians(self.lat_deg)
            cos_lat = max(1e-9, math.cos(lat_rad))

            dlat_rad = (self.v[1] / EARTH_RADIUS_M) * dt
            dlon_rad = (self.v[0] / (EARTH_RADIUS_M * cos_lat)) * dt

            self.lat_deg += math.degrees(dlat_rad)
            self.lon_deg += math.degrees(dlon_rad)

        # --- covariance propagation ---
        F = self.F
        F[:] = self.I6

        if self.lat_deg is not None:
            lat_rad = math.radians(self.lat_deg)
            cos_lat = max(1e-9, math.cos(lat_rad))

            F[0, 3] = dt / EARTH_RADIUS_M
            F[1, 2] = dt / (EARTH_RADIUS_M * cos_lat)

            tan_lat = math.tan(lat_rad)
            F[1, 0] = dt * (self.v[0] * tan_lat) / (EARTH_RADIUS_M * cos_lat)

        F[2, 4] = -dt
        F[3, 5] = -dt

        Gm = self.Gm
        Gm.fill(0.0)

        Gm[2, 0] = dt
        Gm[3, 1] = dt

        sdt = math.sqrt(dt)
        Gm[4, 2] = sdt
        Gm[5, 3] = sdt

        Q = np.diag([
            self.sigma_a**2,
            self.sigma_a**2,
            self.sigma_ba**2,
            self.sigma_ba**2,
        ])

        self.P = F @ self.P @ F.T + Gm @ Q @ Gm.T
        return True

    def _get_dynamic_gnss_sigmas(self, gps: dict, lat_rad_nom: float):
        # read accuracies (fallback to defaults)
        hAcc_m = float(gps.get("hAcc_m", self.sigma_lat_gps_m))

        if VACC_IS_VELOCITY_ACCURACY:
            vAcc = float(gps.get("vAcc_m", self.sigma_v_gps))  # assume m/s
        else:
            vAcc = float(self.sigma_v_gps)  # fixed velocity sigma

        # clamp
        hAcc_m = float(np.clip(hAcc_m, HACC_MIN_M, HACC_MAX_M))
        if VACC_IS_VELOCITY_ACCURACY:
            vAcc = float(np.clip(vAcc, VACC_MIN, VACC_MAX))

        # smooth
        if USE_GNSS_ACC_EMA:
            if self.hAcc_ema is None:
                self.hAcc_ema = hAcc_m
                self.vAcc_ema = vAcc
            else:
                a = self.ema_alpha
                self.hAcc_ema = (1 - a) * self.hAcc_ema + a * hAcc_m
                self.vAcc_ema = (1 - a) * self.vAcc_ema + a * vAcc
            hAcc_m = float(self.hAcc_ema)
            vAcc = float(self.vAcc_ema)

        # convert to lat/lon radians
        sigma_lat_rad = hAcc_m / EARTH_RADIUS_M
        cos_lat = max(1e-9, math.cos(lat_rad_nom))
        sigma_lon_rad = hAcc_m / (EARTH_RADIUS_M * cos_lat)

        # store for debug/message
        self.last_hAcc_used = hAcc_m
        self.last_vAcc_used = vAcc

        return sigma_lat_rad, sigma_lon_rad, vAcc

    def update_with_gps(self, gps: dict):
        t_ms = get_msg_ts_ms(gps)

        if self.t_filter_ms is None:
            return

        if self.t_last_gps_ms is not None and t_ms <= self.t_last_gps_ms:
            return

        lag_s = (self.t_filter_ms - t_ms) / 1000.0
        if lag_s > MAX_GPS_LAG_S:
            if DEBUG:
                print(f"[GPS] ignored (too old): lag={lag_s:.3f}s", flush=True)
            return

        self.t_last_gps_ms = t_ms

        lat = float(gps.get("lat", gps.get("latitude")))
        lon = float(gps.get("lon", gps.get("longitude")))

        # init nominal from averaged origin
        if self.lat_deg is None or self.lon_deg is None:
            if not self._ensure_origin(lat, lon):
                return
            self.lat_deg = float(self.lat0)
            self.lon_deg = float(self.lon0)

            # tighten initial pos covariance a bit (meters->rad)
            sigma_pos_rad = self.sigma_lat_gps_m / EARTH_RADIUS_M
            self.P[0:2, 0:2] = np.eye(2) * (sigma_pos_rad ** 2)

            if DEBUG:
                print(f"[GPS] filter initialized at lat/lon = ({self.lat_deg:.8f},{self.lon_deg:.8f})", flush=True)
            return

        # measurement
        lat_rad_meas = math.radians(lat)
        lon_rad_meas = math.radians(lon)

        lat_rad_nom = math.radians(self.lat_deg)
        lon_rad_nom = math.radians(self.lon_deg)

        ve = float(gps.get("ve_ms", 0.0))
        vn = float(gps.get("vn_ms", 0.0))

        z = np.array([lat_rad_meas, lon_rad_meas, ve, vn])
        h = np.array([lat_rad_nom, lon_rad_nom, self.v[0], self.v[1]])
        r = z - h
        r[1] = wrap_pi(r[1])

        H = self.H_gps

        # --- dynamic R from GNSS confidence ---
        sigma_lat_rad, sigma_lon_rad, sigma_v = self._get_dynamic_gnss_sigmas(gps, lat_rad_nom)

        R = np.diag([
            sigma_lat_rad**2,
            sigma_lon_rad**2,
            sigma_v**2,
            sigma_v**2,
        ])

        # Kalman update
        S = H @ self.P @ H.T + R
        K = self.P @ H.T @ np.linalg.solve(S, np.eye(4))
        dx = K @ r

        # inject correction into nominal
        self.lat_deg += math.degrees(dx[0])
        self.lon_deg += math.degrees(dx[1])
        self.v += dx[2:4]
        self.b_a += dx[4:6]

        I = self.I6
        self.P = (I - K @ H) @ self.P @ (I - K @ H).T + K @ R @ K.T

        if DEBUG:
            # one line showing the confidence used
            print(f"[GPS] R from hAcc={self.last_hAcc_used:.2f}m, "
                  f"vAcc={'{:.2f}'.format(self.last_vAcc_used) + ('m/s' if VACC_IS_VELOCITY_ACCURACY else 'm/s(fixed)')}",
                  flush=True)

    def make_est_msg(self) -> EstMsg:
        ts = int(self.t_filter_ms if self.t_filter_ms is not None else time.time() * 1000)

        P_ll_rad = 0.5 * (self.P[0:2, 0:2] + self.P[0:2, 0:2].T)
        rad2deg = 180.0 / math.pi
        P_ll_deg = (P_ll_rad * (rad2deg**2)).astype(float).tolist()

        P_en_m = None
        x_e = None
        y_n = None

        if self.origin_set and self.lat0 is not None and self.lon0 is not None and self.lat_deg is not None and self.lon_deg is not None:
            x_e, y_n = latlon_to_local_EN(self.lat_deg, self.lon_deg, self.lat0, self.lon0)

            lat_rad = math.radians(self.lat_deg)
            cos_lat = max(1e-9, math.cos(lat_rad))

            # E = R*cos(lat)*dlon, N = R*dlat
            J = np.array([
                [0.0, EARTH_RADIUS_M * cos_lat],
                [EARTH_RADIUS_M, 0.0],
            ])
            P_en = J @ P_ll_rad @ J.T
            P_en_m = (0.5 * (P_en + P_en.T)).astype(float).tolist()

        return EstMsg(
            ts_ms=ts,
            lat=float(self.lat_deg) if self.lat_deg is not None else None,
            lon=float(self.lon_deg) if self.lon_deg is not None else None,
            x_e=float(x_e) if x_e is not None else None,
            y_n=float(y_n) if y_n is not None else None,
            vx_e=float(self.v[0]),
            vy_n=float(self.v[1]),
            yaw_deg=float(wrap_360(math.degrees(self.yaw))),
            bax_e=float(self.b_a[0]),
            bay_n=float(self.b_a[1]),
            P_latlon=P_ll_deg,
            P_en_m=P_en_m if P_en_m is not None else [[None, None], [None, None]],
            hAcc_m_used=float(self.last_hAcc_used) if self.last_hAcc_used is not None else None,
            vAcc_used=float(self.last_vAcc_used) if self.last_vAcc_used is not None else None,
        )


# ============================================================
# Runner: SUB local, PUB neocampus
# ============================================================
class Runner:
    def __init__(self):
        self.eskf = ESKF2D_LatLon_NoYaw()
        self.lock = threading.Lock()

        self.client_in = None
        self.client_out = None
        self.stop_evt = threading.Event()

        self.pub_hz = PUB_HZ
        self._last_est_dict = None

        self.last_print = 0.0
        self.print_hz = 1.0

    # ---------- INPUT (localhost) callbacks ----------
    def on_connect_in(self, client, userdata, flags, reason_code, properties):
        print(f"[IN ] connected to localhost (code={reason_code})", flush=True)
        client.subscribe(TOPIC_IMU)
        client.subscribe(TOPIC_GPS)
        print(f"[IN ] subscribed: {TOPIC_IMU}, {TOPIC_GPS}", flush=True)

    def on_disconnect_in(self, client, userdata, disconnect_flags, reason_code, properties):
        print(f"[IN ] disconnected (code={reason_code})", flush=True)

    def on_message_in(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            return

        with self.lock:
            if msg.topic == TOPIC_IMU:
                self.eskf.propagate_with_imu(data)
            elif msg.topic == TOPIC_GPS:
                self.eskf.update_with_gps(data)

            est = self.eskf.make_est_msg()
            self._last_est_dict = asdict(est)

            if DEBUG:
                now = time.time()
                if now - self.last_print >= 1.0 / max(1e-9, self.print_hz) and self.eskf.t_filter_ms is not None:
                    self.last_print = now
                    if est.lat is not None and est.lon is not None:
                        en = ""
                        if est.x_e is not None and est.y_n is not None:
                            en = f" EN=({est.x_e:+.1f},{est.y_n:+.1f})m"
                        conf = ""
                        if est.hAcc_m_used is not None:
                            conf = f" hAcc={est.hAcc_m_used:.2f}m"
                            if est.vAcc_used is not None:
                                conf += f" vAcc={est.vAcc_used:.2f}" + ("m/s" if VACC_IS_VELOCITY_ACCURACY else "(fixed)")
                        print(
                            f"[EST] latlon=({est.lat:.7f},{est.lon:.7f}){en} "
                            f"v=({est.vx_e:+.2f},{est.vy_n:+.2f}) yaw={est.yaw_deg:6.1f}{conf}",
                            flush=True,
                        )

    # ---------- OUTPUT (neocampus) publish loop ----------
    def publisher_loop(self):
        period = 1.0 / float(self.pub_hz)
        next_t = time.time()

        while not self.stop_evt.is_set():
            now = time.time()
            if now < next_t:
                time.sleep(min(0.01, next_t - now))
                continue
            next_t += period

            with self.lock:
                est_dict = self._last_est_dict
                out = self.client_out

            if est_dict is None or out is None:
                continue

            try:
                out.publish(TOPIC_EST, json.dumps(est_dict), qos=0, retain=False)
            except Exception:
                pass

    def start(self):
        # --- client_in (localhost) ---
        cin = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=MQTT_CLIENT_ID_IN,
            clean_session=True,
        )
        if MQTT_IN_USERNAME is not None:
            cin.username_pw_set(MQTT_IN_USERNAME, MQTT_IN_PASSWORD)
        cin.reconnect_delay_set(min_delay=1, max_delay=5)
        cin.on_connect = self.on_connect_in
        cin.on_disconnect = self.on_disconnect_in
        cin.on_message = self.on_message_in

        # --- client_out (neocampus) ---
        cout = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=MQTT_CLIENT_ID_OUT,
            clean_session=True,
        )
        cout.username_pw_set(MQTT_OUT_USERNAME, MQTT_OUT_PASSWORD)
        cout.reconnect_delay_set(min_delay=1, max_delay=10)

        self.client_in = cin
        self.client_out = cout

        print(f"[RUN] IN  connect -> {MQTT_IN_HOST}:{MQTT_IN_PORT} id={MQTT_CLIENT_ID_IN}", flush=True)
        cin.connect(MQTT_IN_HOST, MQTT_IN_PORT, MQTT_IN_KEEPALIVE)

        print(f"[RUN] OUT connect -> {MQTT_OUT_HOST}:{MQTT_OUT_PORT} id={MQTT_CLIENT_ID_OUT}", flush=True)
        cout.connect(MQTT_OUT_HOST, MQTT_OUT_PORT, MQTT_OUT_KEEPALIVE)

        print(f"[RUN] Publishing EST -> {TOPIC_EST} @ {PUB_HZ:.1f} Hz (to neocampus)", flush=True)
        print(f"[RUN] Dynamic GNSS: VACC_IS_VELOCITY_ACCURACY={VACC_IS_VELOCITY_ACCURACY}, "
              f"EMA={USE_GNSS_ACC_EMA} alpha={GNSS_ACC_EMA_ALPHA}", flush=True)

        pub_th = threading.Thread(target=self.publisher_loop, daemon=True)
        pub_th.start()

        # Two network loops (one per client)
        th_in = threading.Thread(target=cin.loop_forever, daemon=True)
        th_out = threading.Thread(target=cout.loop_forever, daemon=True)
        th_in.start()
        th_out.start()

        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            print("\n[RUN] stopping...", flush=True)
        finally:
            self.stop_evt.set()
            try:
                cin.disconnect()
            except Exception:
                pass
            try:
                cout.disconnect()
            except Exception:
                pass


def main():
    Runner().start()


if __name__ == "__main__":
    main()
