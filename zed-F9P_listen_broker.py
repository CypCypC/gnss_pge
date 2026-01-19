# pip install pyserial pyubx2 paho-mqtt

from serial import Serial
from pyubx2 import UBXReader
import paho.mqtt.client as mqtt
import time
import json

# --------- GNSS (u-blox) ---------
PORT = "COM6"
BAUDRATE = 115200

# --------- MQTT ---------
MQTT_BROKER = "127.0.0.1"  # <- IP/hostname du broker (ex: "192.168.1.50")
MQTT_PORT = 1883
MQTT_USERNAME = None
MQTT_PASSWORD = None
MQTT_KEEPALIVE = 60

TOPIC = "TestTopic/VACOP/localisation/gnss"
PUBLISH_EVERY_SEC = 0.2  # 5 Hz max (évite de spam)

def make_mqtt_client() -> mqtt.Client:
    # API v2 (supprime le warning de dépréciation)
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

    if MQTT_USERNAME is not None:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            print(f"[MQTT] Connecté à {MQTT_BROKER}:{MQTT_PORT}")
        else:
            print(f"[MQTT] Échec connexion, reason_code={reason_code}")

    def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
        print(f"[MQTT] Déconnecté, reason_code={reason_code}")

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
    client.loop_start()
    return client

def main():
    mqtt_client = make_mqtt_client()
    last_pub = 0.0

    with Serial(PORT, BAUDRATE, timeout=1) as ser:
        ubr = UBXReader(ser)  # pas de protfilter -> compatible

        print("Écoute ZED-F9P (UBX/NMEA) + publication MQTT... Ctrl+C pour arrêter")

        try:
            while True:
                raw, msg = ubr.read()
                if msg is None:
                    continue

                ident = getattr(msg, "identity", "")
                lat = None
                lon = None

                # ---- 1) UBX NAV-PVT ----
                if ident == "NAV-PVT":
                    lat = msg.lat * 1e-7
                    lon = msg.lon * 1e-7

                # ---- 2) NMEA GGA / RMC ----
                elif ident in ("GNGGA", "GPGGA", "GNRMC", "GPRMC"):
                    lat = getattr(msg, "lat", None) or getattr(msg, "latitude", None)
                    lon = getattr(msg, "lon", None) or getattr(msg, "longitude", None)

                # si on n'a pas réussi à extraire lat/lon, on ignore
                if lat is None or lon is None:
                    continue

                now = time.time()
                if now - last_pub < PUBLISH_EVERY_SEC:
                    continue

                payload = {
                    "lat": round(float(lat), 7),
                    "lon": round(float(lon), 7),
                    "timestamp": now,
                    "source": ident  # optionnel: te dit si c'était UBX ou NMEA
                }

                mqtt_client.publish(TOPIC, json.dumps(payload), qos=0, retain=False)
                last_pub = now

                print(f"Published -> {payload}")

        except KeyboardInterrupt:
            print("\nArrêt du script.")
        finally:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()

if __name__ == "__main__":
    main()
