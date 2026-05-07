"""
DriveJournal Pi-side telemetry forwarder.

Reads raw CAN frames + GPS coordinates and publishes them as JSON to an
MQTT broker. No DBC decoding here — that happens server-side in the
.NET worker.

Configuration is read from .env in the working directory:
    mqtt_server      hostname of the broker (e.g. o14989e1.ala.us-east-1.emqxsl.com)
    mqtt_port        broker port (8883 for TLS, 1883 for plain)
    mqtt_username    auth username
    mqtt_password    auth password
    mqtt_topic       topic to publish to (default: car/telemetry/raw)
    vehicle_id       per-car identifier embedded in payload (default: default-car)
    desired_arbitration_ids   optional comma-separated hex ids; empty = allow all

Payload contract (one message per CAN frame):
{
  "vehicle_id":      str,
  "timestamp":       str (ISO-8601 UTC),
  "arbitration_id":  int (decimal),
  "dlc":             int,
  "data":            [int, ...]            # 0..8 bytes, each 0-255
  "gps":             {"lon": float, "lat": float} | null
}
"""

import os
import json
import signal
import sys
import time
from datetime import datetime, timezone

import can
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from gps3 import agps3
import coordTransform_py.coordTransform_utils as transform


load_dotenv()

# --- Config ----------------------------------------------------------------
MQTT_HOST     = os.getenv("mqtt_server", "localhost")
MQTT_PORT     = int(os.getenv("mqtt_port", "1883"))
MQTT_USERNAME = os.getenv("mqtt_username")
MQTT_PASSWORD = os.getenv("mqtt_password")
MQTT_TOPIC    = os.getenv("mqtt_topic", "car/telemetry/raw")
VEHICLE_ID    = os.getenv("vehicle_id", "default-car")
USE_TLS       = MQTT_PORT == 8883  # TLS on standard MQTT-secure port

_DESIRED_IDS_RAW = os.getenv("desired_arbitration_ids", "")
ALLOWED_IDS = (
    {int(s, 16) for s in _DESIRED_IDS_RAW.split(",") if s.strip()}
    if _DESIRED_IDS_RAW else set()
)


# --- GPS -------------------------------------------------------------------
gps_socket = agps3.GPSDSocket()
data_stream = agps3.DataStream()
gps_socket.connect()
gps_socket.watch()

current_gps = None  # {"lon": float, "lat": float} or None


def update_gps():
    """Pull one GPS sample and update current_gps. Non-fatal on failure."""
    global current_gps
    try:
        new_data = gps_socket.next()
        if new_data:
            data_stream.unpack(new_data)
            if data_stream.lon != "n/a" and data_stream.lat != "n/a":
                lon, lat = transform.wgs84_to_gcj02(
                    float(data_stream.lon), float(data_stream.lat)
                )
                current_gps = {"lon": lon, "lat": lat}
    except Exception as e:
        print(f"[gps] error: {e}", file=sys.stderr)


# --- CAN bus setup ---------------------------------------------------------
os.system("sudo ip link set can0 down")
os.system(
    "sudo ip link set can0 up type can bitrate 500000 "
    "dbitrate 4000000 restart-ms 1000 berr-reporting on fd on"
)
bus = can.interface.Bus(channel="can0", interface="socketcan")


# --- MQTT client -----------------------------------------------------------
def _make_client(client_id):
    """Construct a paho client that works on both 1.x and 2.x."""
    try:
        return mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
            client_id=client_id,
        )
    except (TypeError, AttributeError):
        return mqtt.Client(client_id=client_id)


client = _make_client(f"{VEHICLE_ID}-pi")

if MQTT_USERNAME and MQTT_PASSWORD:
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

if USE_TLS:
    # Uses the system CA store, which works for public hosted brokers.
    client.tls_set()


def on_connect(c, userdata, flags, rc):
    if rc == 0:
        scheme = "mqtts" if USE_TLS else "mqtt"
        print(f"[mqtt] connected to {scheme}://{MQTT_HOST}:{MQTT_PORT}, "
              f"publishing to '{MQTT_TOPIC}'")
    else:
        print(f"[mqtt] connection refused, rc={rc}", file=sys.stderr)


client.on_connect = on_connect
client.connect(MQTT_HOST, MQTT_PORT)
client.loop_start()


# --- CAN listener ----------------------------------------------------------
class RawCanPublisher(can.Listener):
    def __init__(self, allowed_ids):
        super().__init__()
        self.allowed_ids = allowed_ids  # empty set => allow all

    def on_message_received(self, msg):
        if self.allowed_ids and msg.arbitration_id not in self.allowed_ids:
            return
        payload = {
            "vehicle_id": VEHICLE_ID,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "arbitration_id": msg.arbitration_id,
            "dlc": msg.dlc,
            "data": list(msg.data),
            "gps": current_gps,
        }
        client.publish(MQTT_TOPIC, json.dumps(payload), qos=0)


listener = RawCanPublisher(ALLOWED_IDS)
notifier = can.Notifier(bus, [listener])


# --- Graceful shutdown -----------------------------------------------------
def shutdown(*_):
    print("[main] shutting down...")
    try:
        notifier.stop()
    finally:
        client.loop_stop()
        client.disconnect()
    sys.exit(0)


signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)


# --- Main loop -------------------------------------------------------------
try:
    while True:
        update_gps()
        time.sleep(0.01)
except KeyboardInterrupt:
    shutdown()
