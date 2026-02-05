#!/usr/bin/env python3
"""
ZED-F9P (UBX + NMEA) -> MQTT + logs locaux robustes (Raspberry-ready)

✅ Publie la position à fréquence fixe (ex: 5 Hz) sur TOPIC_POS
✅ Publie RAWX (mesures satellites) à fréquence réduite (ex: 1 Hz) sur TOPIC_RAWX
✅ Log local:
   - UBX brut .ubx
   - RAWX .csv
   - PVT .csv
✅ Flush + fsync réguliers (limite la perte en cas de coupure d’alim)

Deps:
  pip install pyserial pyubx2 paho-mqtt
"""

from __future__ import annotations

import csv
import json
import os
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from serial import Serial
from pyubx2 import UBXReader, UBX_PROTOCOL, NMEA_PROTOCOL
import paho.mqtt.client as mqtt

# =========================
# -------- CONFIG ---------
# =========================

# GNSS (Raspberry / Linux)
PORT = "/dev/ttyACM0"
BAUDRATE = 115200

# MQTT
MQTT_BROKER = "neocampus.univ-tlse3.fr"
MQTT_PORT = 10883
MQTT_USERNAME = "test"
MQTT_PASSWORD = "test"
MQTT_KEEPALIVE = 60

TOPIC_BASE = "TestTopic/VACOP/localisation/gnss"
TOPIC_POS = TOPIC_BASE + "/pos"
TOPIC_RAWX = TOPIC_BASE + "/rawx"

# Rates
PUBLISH_POS_EVERY_SEC = 0.2   # 5 Hz position stable
PUBLISH_RAWX_EVERY_SEC = 1.0  # 1 Hz RAWX (beaucoup plus lourd)

# Logging robustness
FLUSH_EVERY_SEC = 1.0         # flush + fsync toutes les 1 s
LOG_DIR = "."                 # dossier de sortie (ex: "/home/pi/logs")

# =========================
# -------- HELPERS --------
# =========================

def is_finite_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and not (x != x or x in (float("inf"), -float("inf")))

def is_valid_latlon(lat: Any, lon: Any) -> bool:
    if not is_finite_number(lat) or not is_finite_number(lon):
        return False
    return -90.0 <= float(lat) <= 90.0 and -180.0 <= float(lon) <= 180.0

def safe_round(x: Optional[float], nd: int) -> Optional[float]:
    if x is None:
        return None
    return round(float(x), nd)

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def fsync_file(f) -> None:
    try:
        f.flush()
        os.fsync(f.fileno())
    except Exception:
        # On ne veut pas faire planter le script pour un fsync
        pass

# =========================
# -------- MQTT --------
# =========================

def make_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    def on_connect(c, userdata, flags, reason_code, properties):
        if reason_code == 0:
            print(f"[MQTT] Connecté à {MQTT_BROKER}:{MQTT_PORT}")
        else:
            print(f"[MQTT] Échec connexion, reason_code={reason_code}")

    def on_disconnect(c, userdata, disconnect_flags, reason_code, properties):
        print(f"[MQTT] Déconnecté, reason_code={reason_code}")

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    # Reconnexion auto (Paho gère la boucle)
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
    client.loop_start()
    return client

# =========================
# -------- MAIN --------
# =========================

def main() -> None:
    ensure_dir(LOG_DIR)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    raw_ubx_path = os.path.join(LOG_DIR, f"gnss_raw_{timestamp}.ubx")
    rawx_csv_path = os.path.join(LOG_DIR, f"rawx_{timestamp}.csv")
    pvt_csv_path = os.path.join(LOG_DIR, f"pvt_{timestamp}.csv")

    mqtt_client = make_mqtt_client()

    # State "dernière info connue"
    last_lat: Optional[float] = None
    last_lon: Optional[float] = None
    last_height_m: Optional[float] = None
    last_fixType: Optional[int] = None
    last_source: Optional[str] = None

    # Dernier RAWX connu (on publie à fréquence réduite)
    last_rawx_payload: Optional[Dict[str, Any]] = None

    # Rate limit
    last_pub_pos = 0.0
    last_pub_rawx = 0.0

    # Flush/fsync pacing
    last_flush = 0.0

    print("ZED-F9P -> MQTT + logs locaux")
    print(f"   Serial: {PORT} @ {BAUDRATE}")
    print(f"   MQTT:   {MQTT_BROKER}:{MQTT_PORT}")
    print(f"   Topics: {TOPIC_POS} (pos), {TOPIC_RAWX} (rawx)")
    print("")

    with open(raw_ubx_path, "ab") as fubx, \
         open(rawx_csv_path, "w", newline="") as frawx, \
         open(pvt_csv_path, "w", newline="") as fpvt, \
         Serial(PORT, BAUDRATE, timeout=1) as ser:

        rawx_writer = csv.writer(frawx)
        pvt_writer = csv.writer(fpvt)

        rawx_writer.writerow([
            "time",
            "gnssId", "svId", "sigId", "freqId",
            "prMes", "cpMes", "doppler", "cno"
        ])
        pvt_writer.writerow(["time", "lat", "lon", "height_m", "fixType", "source"])

        ubr = UBXReader(ser, protfilter=UBX_PROTOCOL | NMEA_PROTOCOL)

        try:
            while True:
                raw, msg = ubr.read()
                if msg is None:
                    # Même si rien ne vient, on peut tenter de publier à fréquence fixe (avec dernier état)
                    pass
                else:
                    now = time.time()

                    # Sauvegarde UBX brut (inclut NMEA aussi, car 'raw' = bytes reçus)
                    if raw:
                        fubx.write(raw)

                    ident = getattr(msg, "identity", "")

                    # ----------- RXM-RAWX (mesures satellites) -----------
                    if ident == "RXM-RAWX":
                        measurements = []
                        try:
                            num = int(msg.numMeas)
                        except Exception:
                            num = 0

                        for i in range(1, num + 1):
                            sat = {
                                "gnssId": getattr(msg, f"gnssId_{i:02d}", None),
                                "svId": getattr(msg, f"svId_{i:02d}", None),
                                "sigId": getattr(msg, f"sigId_{i:02d}", None),
                                "freqId": getattr(msg, f"freqId_{i:02d}", None),
                                "prMes": getattr(msg, f"prMes_{i:02d}", None),
                                "cpMes": getattr(msg, f"cpMes_{i:02d}", None),
                                "doppler": getattr(msg, f"doMes_{i:02d}", None),
                                "cno": getattr(msg, f"cno_{i:02d}", None),
                            }
                            measurements.append(sat)
                            rawx_writer.writerow([
                                now,
                                sat["gnssId"], sat["svId"], sat["sigId"], sat["freqId"],
                                sat["prMes"], sat["cpMes"], sat["doppler"], sat["cno"]
                            ])

                        # On stocke le dernier RAWX complet; publication rate-limited plus bas
                        last_rawx_payload = {
                            "type": "RAWX",
                            "timestamp": now,
                            "numMeas": len(measurements),
                            "measurements": measurements,
                        }

                    # ----------- NAV-PVT (solution de position) -----------
                    elif ident == "NAV-PVT":
                        # lat/lon en 1e-7 deg ; height en mm
                        try:
                            fixType = int(msg.fixType)
                        except Exception:
                            fixType = None

                        # On accepte à partir de 2 (2D) comme dans ton 1er code
                        if fixType is not None and fixType >= 2:
                            lat = msg.lat * 1e-7
                            lon = msg.lon * 1e-7
                            height_m = msg.height * 1e-3

                            if is_valid_latlon(lat, lon):
                                last_lat = float(lat)
                                last_lon = float(lon)
                                last_height_m = float(height_m)
                                last_fixType = fixType
                                last_source = "UBX-NAV-PVT"

                                pvt_writer.writerow([now, last_lat, last_lon, last_height_m, last_fixType, last_source])

                    # ----------- NMEA fallback (GGA/RMC) -----------
                    elif ident in ("GNGGA", "GPGGA", "GNRMC", "GPRMC"):
                        lat = getattr(msg, "lat", None)
                        lon = getattr(msg, "lon", None)

                        # PyUBX2 retourne généralement lat/lon déjà en degrés décimaux
                        if is_valid_latlon(lat, lon):
                            last_lat = float(lat)
                            last_lon = float(lon)
                            last_height_m = last_height_m  # pas toujours dispo dans ces phrases
                            last_fixType = last_fixType
                            last_source = ident

                # ============ Publication à fréquence fixe ============
                now_pub = time.time()

                # ---- Publish position @ 5 Hz ----
                if now_pub - last_pub_pos >= PUBLISH_POS_EVERY_SEC:
                    if is_valid_latlon(last_lat, last_lon):
                        payload_pos = {
                            "type": "GNSS",
                            "timestamp": now_pub,
                            "lat": safe_round(last_lat, 7),
                            "lon": safe_round(last_lon, 7),
                            "height_m": safe_round(last_height_m, 2),
                            "fixType": last_fixType,
                            "source": last_source,
                        }
                        mqtt_client.publish(TOPIC_POS, json.dumps(payload_pos), qos=0, retain=False)
                        last_pub_pos = now_pub
                        print(f"[POS] {payload_pos}")
                    else:
                        # On avance quand même l’horloge de publication pour éviter spam CPU
                        last_pub_pos = now_pub
                        print("[POS] Coordonnées invalides (pas de publish)")

                # ---- Publish RAWX @ 1 Hz ----
                if last_rawx_payload is not None and (now_pub - last_pub_rawx >= PUBLISH_RAWX_EVERY_SEC):
                    mqtt_client.publish(TOPIC_RAWX, json.dumps(last_rawx_payload), qos=0, retain=False)
                    last_pub_rawx = now_pub
                    print(f"[RAWX] Publié numMeas={last_rawx_payload.get('numMeas')}")

                # ============ Flush + fsync réguliers ============
                if now_pub - last_flush >= FLUSH_EVERY_SEC:
                    fsync_file(fubx)
                    fsync_file(frawx)
                    fsync_file(fpvt)
                    last_flush = now_pub

        except KeyboardInterrupt:
            print("\n Arrêt utilisateur (Ctrl+C)")
        finally:
            try:
                # flush final
                fsync_file(fubx)
                fsync_file(frawx)
                fsync_file(fpvt)
            except Exception:
                pass
            mqtt_client.loop_stop()
            mqtt_client.disconnect()

    print("\n Fichiers générés :")
    print(f" - {raw_ubx_path}")
    print(f" - {rawx_csv_path}")
    print(f" - {pvt_csv_path}")


if __name__ == "__main__":
    main()
