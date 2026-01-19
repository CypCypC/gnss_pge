# pip install pyserial pyubx2 paho-mqtt

from serial import Serial
from pyubx2 import UBXReader
import paho.mqtt.client as mqtt
import time
import json

# --------- GNSS (u-blox) ---------
PORT = "COM5"
BAUDRATE = 115200

# --------- MQTT ---------
MQTT_BROKER = "127.0.0.1"   # ⚠️ si ton broker est ailleurs: ex "192.168.1.50"
MQTT_PORT = 1883
MQTT_USERNAME = None
MQTT_PASSWORD = None
MQTT_KEEPALIVE = 60

TOPIC = "TestTopic/VACOP/localisation/gnss"
PUBLISH_EVERY_SEC = 0.2   # 5 Hz max

def make_mqtt_client() -> mqtt.Client:
    # ✅ API v2 pour enlever le DeprecationWarning
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

    if MQTT_USERNAME is not None:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    # Callbacks v2: signature différente
    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            print(f"[MQTT] Connecté à {MQTT_BROKER}:{MQTT_PORT}")
        else:
            print(f"[MQTT] Échec connexion, reason_code={reason_code}")

    def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
        print(f"[MQTT] Déconnecté, reason_code={reason_code}")

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    # Connexion
    client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
    client.loop_start()
    return client

def main():
    mqtt_client = make_mqtt_client()
    last_pub = 0.0

    with Serial(PORT, BAUDRATE, timeout=1) as ser:
        ubr = UBXReader(ser)

        print("Écoute du ZED-F9P + publication MQTT... (Ctrl+C pour arrêter)")

        try:
            while True:
                raw, msg = ubr.read()
                if not msg or getattr(msg, "identity", "") != "NAV-PVT":
                    continue

                lat = msg.lat * 1e-7
                lon = msg.lon * 1e-7
                ts = time.time()

                if ts - last_pub < PUBLISH_EVERY_SEC:
                    continue

                payload = {
                    "lat": round(lat, 7),
                    "lon": round(lon, 7),
                    "timestamp": ts
                }

                mqtt_client.publish(TOPIC, json.dumps(payload), qos=0, retain=False)
                last_pub = ts

                print(f"Published -> {payload}")

        except KeyboardInterrupt:
            print("\nArrêt du script.")
        finally:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()

if __name__ == "__main__":
    main()
