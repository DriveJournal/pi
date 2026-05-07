"""
Fake Honda CAN publisher for local testing.

Loads Honda.dbc and emits realistic raw CAN frames for a small set of
driving-relevant messages, with values that change over time as if the
car were moving. Each frame is encoded with cantools (the same library
that will be used to decode them server-side), so the bytes you see in
subscribe_test.py will be byte-perfect against the DBC.

Messages simulated (Honda Civic):
    344  ENGINE_DATA        speed, RPM, odometer
    380  POWERTRAIN_DATA    pedal, RPM, gas/brake pressed
    464  WHEEL_SPEEDS       per-wheel speeds (kph)
    490  VEHICLE_DYNAMICS   lateral / longitudinal accel
    330  STEERING_SENSORS   steering angle + rate
    401  GEARBOX            gear position (D)
    777  CAR_SPEED          speed in kph + mph
    316  GAS_PEDAL          throttle position
    1029 DOORS_STATUS       all doors closed

Examples:
    python fake_publisher.py                    # 10 Hz cycle (90 msg/s total)
    python fake_publisher.py --rate 50          # 50 Hz cycle
    python fake_publisher.py --no-gps
    python fake_publisher.py --vehicle test-car
"""

import argparse
import json
import math
import random
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import cantools
import paho.mqtt.client as mqtt
# from dotenv import load_dotenv     # only needed for EMQX mode below
# import os


# --- Broker config ---------------------------------------------------------
# === LOCAL MOSQUITTO MODE (active) ===
MQTT_HOST     = "localhost"
MQTT_PORT     = 1883
MQTT_USERNAME = None
MQTT_PASSWORD = None
MQTT_TOPIC    = "car/telemetry/raw"
USE_TLS       = False

# === EMQX CLOUD MODE (commented out) =====================================
# To switch back, comment out the LOCAL block above and uncomment this one
# (also uncomment the load_dotenv / os imports at the top of the file).
#
# load_dotenv()
# MQTT_HOST     = os.getenv("mqtt_server", "localhost")
# MQTT_PORT     = int(os.getenv("mqtt_port", "1883"))
# MQTT_USERNAME = os.getenv("mqtt_username")
# MQTT_PASSWORD = os.getenv("mqtt_password")
# MQTT_TOPIC    = os.getenv("mqtt_topic", "car/telemetry/raw")
# USE_TLS       = MQTT_PORT == 8883


# --- DBC loading -----------------------------------------------------------
DBC_PATH = Path(__file__).parent / "Honda.dbc"
db = cantools.database.load_file(str(DBC_PATH))

# Names of messages we'll simulate every cycle.
SIMULATED = [
    "ENGINE_DATA",
    "POWERTRAIN_DATA",
    "WHEEL_SPEEDS",
    "VEHICLE_DYNAMICS",
    "STEERING_SENSORS",
    "GEARBOX",
    "CAR_SPEED",
    "GAS_PEDAL",
    "DOORS_STATUS",
]


# --- Vehicle simulator ------------------------------------------------------
class VehicleSim:
    """Smoothly oscillating Honda Civic state."""

    def __init__(self):
        self.t0 = time.time()
        self.counter = 0
        self.odometer_m = 0.0  # meters

    def tick(self, dt):
        self.counter = (self.counter + 1) % 4
        self.odometer_m += (self.speed_kph / 3.6) * dt

    @property
    def t(self):
        return time.time() - self.t0

    @property
    def speed_kph(self):
        # 5..75 kph wave with small noise
        return max(0.0, 40 + 35 * math.sin(self.t / 30) + random.uniform(-1, 1))

    @property
    def rpm(self):
        return max(800.0, 800 + self.speed_kph * 35) + random.uniform(-30, 30)

    @property
    def throttle_pct(self):
        return max(0.0, min(100.0, 40 + 30 * math.sin(self.t / 12)))

    @property
    def gas_pressed(self):
        return 1 if self.throttle_pct > 5 else 0

    @property
    def brake_pressed(self):
        return 1 if self.throttle_pct < 3 else 0

    @property
    def steer_angle_deg(self):
        return 30 * math.sin(self.t / 7)

    @property
    def steer_rate_deg_s(self):
        return 30 * math.cos(self.t / 7) / 7

    @property
    def lat_accel(self):
        return self.steer_angle_deg * self.speed_kph / 1500

    @property
    def long_accel(self):
        return (self.throttle_pct - 50) / 25.0  # m/s^2

    # Per-wheel speeds, slightly perturbed from chassis speed
    def wheel_speeds(self):
        v = self.speed_kph
        return (
            v + random.uniform(-0.2, 0.2),
            v + random.uniform(-0.2, 0.2),
            v + random.uniform(-0.2, 0.2),
            v + random.uniform(-0.2, 0.2),
        )


# --- Build signal dicts for each message -----------------------------------
def signals_for(name, sim):
    """Return a dict of signal-name -> physical value for `name`."""
    if name == "ENGINE_DATA":
        return {
            "XMISSION_SPEED": sim.speed_kph,
            "ENGINE_RPM": int(sim.rpm),
            "XMISSION_SPEED2": sim.speed_kph,
            "ODOMETER": (int(sim.odometer_m / 10)) % 256 * 10,
            "COUNTER": sim.counter,
            "CHECKSUM": 0,
        }
    if name == "POWERTRAIN_DATA":
        return {
            "PEDAL_GAS": int(sim.throttle_pct * 2.55),
            "ENGINE_RPM": int(sim.rpm),
            "GAS_PRESSED": sim.gas_pressed,
            "ACC_STATUS": 0,
            "BOH_17C": 0,
            "BRAKE_SWITCH": sim.brake_pressed,
            "BOH2_17C": 0,
            "BRAKE_PRESSED": sim.brake_pressed,
            "BOH3_17C": 0,
            "COUNTER": sim.counter,
            "CHECKSUM": 0,
        }
    if name == "WHEEL_SPEEDS":
        fl, fr, rl, rr = sim.wheel_speeds()
        return {
            "WHEEL_SPEED_FL": fl,
            "WHEEL_SPEED_FR": fr,
            "WHEEL_SPEED_RL": rl,
            "WHEEL_SPEED_RR": rr,
            "CHECKSUM": 0,
        }
    if name == "VEHICLE_DYNAMICS":
        return {
            "LAT_ACCEL": sim.lat_accel,
            "LONG_ACCEL": sim.long_accel,
            "COUNTER": sim.counter,
            "CHECKSUM": 0,
        }
    if name == "STEERING_SENSORS":
        return {
            "STEER_ANGLE": sim.steer_angle_deg,
            "STEER_ANGLE_RATE": sim.steer_rate_deg_s,
            "STEER_SENSOR_STATUS_1": 0,
            "STEER_SENSOR_STATUS_2": 0,
            "STEER_SENSOR_STATUS_3": 0,
            "STEER_WHEEL_ANGLE": sim.steer_angle_deg,
            "COUNTER": sim.counter,
            "CHECKSUM": 0,
        }
    if name == "GEARBOX":
        # 8 = D in this DBC (see VAL_ 401 GEAR_SHIFTER)
        return {
            "GEAR_SHIFTER": 8,
            "BOH": 0,
            "GEAR2": 0,
            "GEAR": 4,         # D
            "ZEROS_BOH": 0,
            "COUNTER": sim.counter,
            "CHECKSUM": 0,
        }
    if name == "CAR_SPEED":
        return {
            "ROUGH_CAR_SPEED": int(sim.speed_kph * 0.621371),  # mph
            "CAR_SPEED": sim.speed_kph,
            "ROUGH_CAR_SPEED_3": sim.speed_kph,
            "ROUGH_CAR_SPEED_2": int(sim.speed_kph * 0.621371),
            "LOCK_STATUS": 0,
            "COUNTER": sim.counter,
            "CHECKSUM": 0,
            "IMPERIAL_UNIT": 0,
        }
    if name == "GAS_PEDAL":
        return {
            "CAR_GAS": int(sim.throttle_pct * 2.55),
            "COUNTER": sim.counter,
            "CHECKSUM": 0,
        }
    if name == "DOORS_STATUS":
        return {
            "DOOR_OPEN_FL": 0,
            "DOOR_OPEN_FR": 0,
            "DOOR_OPEN_RL": 0,
            "DOOR_OPEN_RR": 0,
            "TRUNK_OPEN": 0,
            "COUNTER": sim.counter,
            "CHECKSUM": 0,
        }
    raise ValueError(f"unknown message {name}")


def fake_gps():
    return {
        "lon": -122.4194 + random.uniform(-0.001, 0.001),
        "lat": 37.7749 + random.uniform(-0.001, 0.001),
    }


# --- MQTT helper -----------------------------------------------------------
def _make_client(client_id):
    try:
        return mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
            client_id=client_id,
        )
    except (TypeError, AttributeError):
        return mqtt.Client(client_id=client_id)


# --- Main ------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(description="Fake Honda CAN MQTT publisher.")
    p.add_argument("--rate", type=float, default=10.0,
                   help="cycles per second (default 10); each cycle emits "
                        "one frame per simulated message")
    p.add_argument("--vehicle", default="fake-civic",
                   help="vehicle_id field (default fake-civic)")
    p.add_argument("--no-gps", action="store_true",
                   help="omit gps coordinates (gps field becomes null)")
    args = p.parse_args()

    interval = 1.0 / args.rate
    include_gps = not args.no_gps

    client = _make_client("fake-publisher")
    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    if USE_TLS:
        client.tls_set()
    client.connect(MQTT_HOST, MQTT_PORT)
    client.loop_start()

    scheme = "mqtts" if USE_TLS else "mqtt"
    print(f"[fake] connected to {scheme}://{MQTT_HOST}:{MQTT_PORT}")
    print(f"[fake] publishing to '{MQTT_TOPIC}' "
          f"({len(SIMULATED)} msgs × {args.rate} Hz = "
          f"{int(len(SIMULATED) * args.rate)} msg/s) -- ctrl-c to stop")

    def stop(*_):
        print("\n[fake] stopping")
        client.loop_stop()
        client.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    sim = VehicleSim()
    sent = 0
    last_log = time.time()

    while True:
        sim.tick(interval)
        gps = fake_gps() if include_gps else None

        for name in SIMULATED:
            msg_def = db.get_message_by_name(name)
            try:
                raw = db.encode_message(name, signals_for(name, sim))
            except Exception as e:
                print(f"[fake] encode error for {name}: {e}", file=sys.stderr)
                continue

            payload = {
                "vehicle_id": args.vehicle,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "arbitration_id": msg_def.frame_id,
                "dlc": msg_def.length,
                "data": list(raw),
                "gps": gps,
            }
            client.publish(MQTT_TOPIC, json.dumps(payload), qos=0)
            sent += 1

        if time.time() - last_log >= 5:
            print(f"[fake] sent={sent} speed={sim.speed_kph:.1f}kph "
                  f"rpm={int(sim.rpm)} throttle={sim.throttle_pct:.0f}%",
                  file=sys.stderr)
            last_log = time.time()

        time.sleep(interval)


if __name__ == "__main__":
    main()
