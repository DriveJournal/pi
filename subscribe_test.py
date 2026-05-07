"""
Throwaway MQTT subscriber for sanity-checking the telemetry stream.

Usage:
    python subscribe_test.py
"""

import json
import sys
import time
from collections import Counter

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


REQUIRED_KEYS = {
    "vehicle_id", "timestamp", "arbitration_id", "dlc", "data", "gps",
}

stats = Counter()
last_report = time.time()


def _make_client(client_id):
    try:
        return mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
            client_id=client_id,
        )
    except (TypeError, AttributeError):
        return mqtt.Client(client_id=client_id)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[sub] connected, subscribing to '{MQTT_TOPIC}'")
        client.subscribe(MQTT_TOPIC, qos=0)
    else:
        print(f"[sub] connection refused, rc={rc}", file=sys.stderr)


def on_message(client, userdata, msg):
    global last_report
    stats["total"] += 1

    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception as e:
        stats["bad_json"] += 1
        print(f"[sub] BAD JSON: {e} -- raw: {msg.payload[:80]!r}",
              file=sys.stderr)
        return

    missing = REQUIRED_KEYS - set(payload.keys())
    if missing:
        stats["missing_keys"] += 1
        print(f"[sub] MISSING KEYS {missing} in: {payload}", file=sys.stderr)
        return

    stats["valid"] += 1
    stats[f"id_{payload['arbitration_id']}"] += 1
    print(json.dumps(payload, indent=2))

    now = time.time()
    if now - last_report >= 5:
        print(
            f"[sub] --- 5s report --- "
            f"valid={stats['valid']} "
            f"bad_json={stats['bad_json']} "
            f"missing_keys={stats['missing_keys']}",
            file=sys.stderr,
        )
        last_report = now


client = _make_client("test-subscriber")

if MQTT_USERNAME and MQTT_PASSWORD:
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

if USE_TLS:
    client.tls_set()

client.on_connect = on_connect
client.on_message = on_message

scheme = "mqtts" if USE_TLS else "mqtt"
print(f"[sub] connecting to {scheme}://{MQTT_HOST}:{MQTT_PORT} ... ctrl-c to exit")
client.connect(MQTT_HOST, MQTT_PORT)

try:
    client.loop_forever()
except KeyboardInterrupt:
    print("\n[sub] final stats:")
    for key, count in sorted(stats.items()):
        print(f"  {key}: {count}")
    client.disconnect()
