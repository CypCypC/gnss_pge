# pip install pyserial pyubx2 paho-mqtt

import json
import time
import serial
import paho.mqtt.client as mqtt
from pyubx2 import UBXReader, UBX_PROTOCOL, NMEA_PROTOCOL

# -------- GNSS ----------
GNSS_PORT = "/dev/ttyACM0"
GNSS_BAUD = 115200

# -------- MQTT -
MQTT_BROKER = "127.0.0.1"
MQTT_PORT = 1883
MQTT_USERNAME = None
MQTT_PASSWORD = None
MQTT_KEEPALIVE = 60


TOPIC_GNSS = "TestTopic/VACOP/localisation/gnss"

PUBLISH_EVERY_SEC = 0.2   # 5 Hz
STATUS_EVERY_SEC = 2.0

def classify_solution(fix_type, carr_soln):
    if fix_type is None:
        return "UNKNOWN"
    if fix_type < 2:
        return "NO_FIX"
    if carr_soln == 2:
        return "RTK_FIXED"
    if carr_soln == 1:
        return "RTK_FLOAT"
    if fix_type == 3:
        return "FIX_3D"
    if fix_type == 2:
        return "FIX_2D"
    return "UNKNOWN"

def safe_int(x):
    try:
        return int(x) if x is not None else None
    except Exception:
        return None

def safe_float(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None

def coords_ok(lat, lon):
    if lat is None or lon is None:
        return False
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return False
    return True

def make_mqtt_client():
    c = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    c.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    def on_connect(client, userdata, flags, reason_code, properties):
        print(f"[MQTT] on_connect: {reason_code}")

    def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
        print(f"[MQTT] on_disconnect: {reason_code}")

    c.on_connect = on_connect
    c.on_disconnect = on_disconnect
    c.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
    c.loop_start()
    return c

def main():
    mqtt_client = make_mqtt_client()

    ser = serial.Serial(GNSS_PORT, GNSS_BAUD, timeout=1)
    ubr = UBXReader(ser, protfilter=UBX_PROTOCOL | NMEA_PROTOCOL)

    print(f"[GNSS] Listening on {GNSS_PORT} @ {GNSS_BAUD} (UBX+NMEA)")
    print(f"[GNSS] Publishing to {TOPIC_GNSS}")

    last_pub = 0.0
    last_status = 0.0

    # Dernières infos connues
    last_lat = None
    last_lon = None
    last_alt_m = None
    last_source = None

    last_fixType = None
    last_carrSoln = None
    last_hAcc_m = None
    last_vAcc_m = None
    last_solution = "UNKNOWN"

    try:
        while True:
            raw, msg = ubr.read()
            now = time.time()

            if msg is None:
                # optionnel: statut périodique même sans message
                if now - last_status >= STATUS_EVERY_SEC:
                    payload = {
                        "timestamp": now,
                        "source": last_source,
                        "latitude": None,
                        "longitude": None,
                        "alt_m": None,
                        "fixType": last_fixType,
                        "carrSoln": last_carrSoln,
                        "solution": last_solution,
                        "hAcc_m": last_hAcc_m,
                        "vAcc_m": last_vAcc_m,
                        "note": "no decoded msg yet"
                    }
                    mqtt_client.publish(TOPIC_GNSS, json.dumps(payload), qos=0, retain=False)
                    print("[STATUS] no msg decoded yet")
                    last_status = now
                continue

            ident = getattr(msg, "identity", "")

            # --- UBX NAV-PVT (meilleure source qualité) ---
            if ident == "NAV-PVT":
                last_fixType = safe_int(getattr(msg, "fixType", None))
                last_carrSoln = safe_int(getattr(msg, "carrSoln", None))

                hAcc_mm = getattr(msg, "hAcc", None)
                vAcc_mm = getattr(msg, "vAcc", None)
                last_hAcc_m = (safe_float(hAcc_mm) / 1000.0) if hAcc_mm is not None else None
                last_vAcc_m = (safe_float(vAcc_mm) / 1000.0) if vAcc_mm is not None else None
                last_solution = classify_solution(last_fixType, last_carrSoln)

                # lat/lon UBX: 1e-7 deg
                last_lat = safe_float(getattr(msg, "lat", None))
                last_lon = safe_float(getattr(msg, "lon", None))
                if last_lat is not None: last_lat *= 1e-7
                if last_lon is not None: last_lon *= 1e-7

                hmsl_mm = getattr(msg, "hMSL", None)
                last_alt_m = (safe_float(hmsl_mm) / 1000.0) if hmsl_mm is not None else None

                last_source = "NAV-PVT"

            # --- NMEA fallback (position) ---
            elif ident in ("GNGGA", "GPGGA", "GNRMC", "GPRMC"):
                lat = safe_float(getattr(msg, "lat", None))
                lon = safe_float(getattr(msg, "lon", None))
                if coords_ok(lat, lon):
                    last_lat, last_lon = lat, lon
                    last_source = ident

            # --- Publication throttled ---
            if now - last_pub < PUBLISH_EVERY_SEC:
                continue

            # Si pas encore de coordonnée valide, on envoie un statut (moins souvent)
            if not coords_ok(last_lat, last_lon):
                if now - last_status >= STATUS_EVERY_SEC:
                    payload = {
                        "timestamp": now,
                        "source": last_source,
                        "latitude": None,
                        "longitude": None,
                        "alt_m": last_alt_m,
                        "fixType": last_fixType,
                        "carrSoln": last_carrSoln,
                        "solution": last_solution,
                        "hAcc_m": last_hAcc_m,
                        "vAcc_m": last_vAcc_m,
                        "note": "no valid coords yet"
                    }
                    mqtt_client.publish(TOPIC_GNSS, json.dumps(payload), qos=0, retain=False)
                    print("[STATUS] no valid coords yet | fixType=", last_fixType, "sol=", last_solution)
                    last_status = now
                last_pub = now
                continue

            # Payload normal (coords OK)
            payload = {
                "timestamp": now,
                "source": last_source,
                "latitude": round(last_lat, 7),
                "longitude": round(last_lon, 7),
                "alt_m": last_alt_m,

                "fixType": last_fixType,
                "carrSoln": last_carrSoln,
                "solution": last_solution,
                "hAcc_m": last_hAcc_m,
                "vAcc_m": last_vAcc_m,
            }

            mqtt_client.publish(TOPIC_GNSS, json.dumps(payload), qos=0, retain=False)
            print(f"[PUB] lat={payload['latitude']:.7f} lon={payload['longitude']:.7f} sol={payload['solution']} src={payload['source']}")
            last_pub = now

    except KeyboardInterrupt:
        print("\n[STOP] Ctrl+C")
    finally:
        try:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        except Exception:
            pass
        try:
            ser.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
