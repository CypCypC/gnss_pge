#!/usr/bin/env python3
# pip install smbus2 paho-mqtt

import math
import time
import json
from smbus2 import SMBus
import paho.mqtt.client as mqtt

# ==========================
# MQTT CONFIG
# ==========================
MQTT_BROKER = "127.0.0.1"
MQTT_PORT = 1883
MQTT_USERNAME = None
MQTT_PASSWORD = None
MQTT_KEEPALIVE = 60

TOPIC = "TestTopic/VACOP/localisation/imu"
PUBLISH_EVERY_SEC = 0.01  # 100 Hz

# ==========================
# I2C ADDRESSES
# ==========================
BUS_ID = 1
ADDR_LSM303D = 0x1D
ADDR_GYRO = 0x6B

# ==========================
# PHYSICAL CONSTANTS
# ==========================
ACC_COUNTS_PER_G = 17000.0
G = 9.80665

GYRO_DPS_PER_LSB = 0.07
DEG2RAD = math.pi / 180.0

# ==========================
# LSM303D REGISTERS
# ==========================
CTRL1 = 0x20
CTRL2 = 0x21
CTRL5 = 0x24
CTRL6 = 0x25
CTRL7 = 0x26

OUT_X_L_A = 0x28
OUT_X_L_M = 0x08

# ==========================
# GYRO REGISTERS
# ==========================
GYRO_CTRL1 = 0x20
GYRO_OUT_X_L = 0x28

# ==========================
# MAG CALIBRATION
# ==========================
M_MIN = (-32767, -32767, -32767)
M_MAX = ( 32767,  32767,  32767)

FROM_VECTOR = (1.0, 0.0, 0.0)


# ==========================
# UTILS
# ==========================
def to_int16(lo, hi):
    v = hi << 8 | lo
    return v - 65536 if v > 32767 else v


class Vec3:
    def __init__(self, x, y, z):
        self.x, self.y, self.z = x, y, z

    def dot(self, o):
        return self.x*o.x + self.y*o.y + self.z*o.z

    def norm(self):
        return math.sqrt(self.x*self.x + self.y*self.y + self.z*self.z)

    def normalize(self):
        n = self.norm()
        return self if n == 0 else Vec3(self.x/n, self.y/n, self.z/n)


def cross(a, b):
    return Vec3(
        a.y*b.z - a.z*b.y,
        a.z*b.x - a.x*b.z,
        a.x*b.y - a.y*b.x
    )


# ==========================
# IMU DRIVER
# ==========================
class IMU:
    def __init__(self, bus):
        self.bus = bus

        self.cx = (M_MIN[0] + M_MAX[0]) / 2
        self.cy = (M_MIN[1] + M_MAX[1]) / 2
        self.cz = (M_MIN[2] + M_MAX[2]) / 2

    def init(self):
        # Accel
        self.bus.write_byte_data(ADDR_LSM303D, CTRL1, 0x57)
        self.bus.write_byte_data(ADDR_LSM303D, CTRL2, 0x00)

        # Mag
        self.bus.write_byte_data(ADDR_LSM303D, CTRL5, 0x64)
        self.bus.write_byte_data(ADDR_LSM303D, CTRL6, 0x20)
        self.bus.write_byte_data(ADDR_LSM303D, CTRL7, 0x00)

        # Gyro L3GD20H: normal mode, XYZ enable
        self.bus.write_byte_data(ADDR_GYRO, GYRO_CTRL1, 0x0F)

    def read_accel(self):
        d = self.bus.read_i2c_block_data(ADDR_LSM303D, OUT_X_L_A | 0x80, 6)
        return Vec3(
            to_int16(d[0], d[1]),
            to_int16(d[2], d[3]),
            to_int16(d[4], d[5]),
        )

    def read_mag(self):
        d = self.bus.read_i2c_block_data(ADDR_LSM303D, OUT_X_L_M | 0x80, 6)
        return Vec3(
            to_int16(d[0], d[1]),
            to_int16(d[2], d[3]),
            to_int16(d[4], d[5]),
        )

    def read_gyro(self):
        d = self.bus.read_i2c_block_data(ADDR_GYRO, GYRO_OUT_X_L | 0x80, 6)
        return Vec3(
            to_int16(d[0], d[1]),
            to_int16(d[2], d[3]),
            to_int16(d[4], d[5]),
        )

    def heading(self):
        a = self.read_accel()
        m = self.read_mag()
        mc = Vec3(m.x - self.cx, m.y - self.cy, m.z - self.cz)

        E = cross(mc, a).normalize()
        N = cross(a, E).normalize()

        f = Vec3(*FROM_VECTOR)
        h = math.degrees(math.atan2(E.dot(f), N.dot(f)))
        if h < 0:
            h += 360

        return h, a, m, mc


# ==========================
# MAIN LOOP
# ==========================
def main():
    mqtt_client = mqtt.Client()
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
    mqtt_client.loop_start()

    with SMBus(BUS_ID) as bus:
        imu = IMU(bus)
        imu.init()

        print("IMU → MQTT (SI units)")

        while True:
            heading, a, m, mc = imu.heading()
            g = imu.read_gyro()

            accel_mps2 = {
                "x": a.x / ACC_COUNTS_PER_G * G,
                "y": a.y / ACC_COUNTS_PER_G * G,
                "z": a.z / ACC_COUNTS_PER_G * G,
            }

            gyro_rads = {
                "x": g.x * GYRO_DPS_PER_LSB * DEG2RAD,
                "y": g.y * GYRO_DPS_PER_LSB * DEG2RAD,
                "z": g.z * GYRO_DPS_PER_LSB * DEG2RAD,
            }

            payload = {
                "timestamp": time.time(),
                "heading_deg": round(heading, 2),
                "accel_mps2": accel_mps2,
                "gyro_rads": gyro_rads,
            }

            mqtt_client.publish(TOPIC, json.dumps(payload))
            time.sleep(PUBLISH_EVERY_SEC)


if __name__ == "__main__":
    main()
