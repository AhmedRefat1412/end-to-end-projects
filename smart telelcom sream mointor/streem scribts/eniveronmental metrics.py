import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
import random

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "telecom.tower.environment"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5
)

TOWERS = [
    {"id": "TOWER_1", "region": "Cairo"},
    {"id": "TOWER_2", "region": "Giza"},
    {"id": "TOWER_3", "region": "Alex"}
]

tower_states = {}
for tower in TOWERS:
    tower_states[tower["id"]] = {
        "temperature_c": random.randint(25, 35),
        "humidity_pct": random.randint(30, 60),
        "wind_speed_kmh": random.randint(5, 20)
    }

print("🌡️ Unified Environment Metrics Producer Started...")

while True:
    document = {"event_time": datetime.now(timezone.utc).isoformat(), "towers": []}

    for tower in TOWERS:
        state = tower_states[tower["id"]]

        state["temperature_c"] = round(state["temperature_c"] + random.uniform(-0.5, 0.5), 1)
        state["humidity_pct"] = round(max(0, min(100, state["humidity_pct"] + random.uniform(-2, 2))), 1)
        state["wind_speed_kmh"] = round(max(0, state["wind_speed_kmh"] + random.uniform(-1, 1)), 1)

        tower_doc = {
            "tower_id": tower["id"],
            "region": tower["region"],
            "temperature_c": state["temperature_c"],
            "humidity_pct": state["humidity_pct"],
            "wind_speed_kmh": state["wind_speed_kmh"]
        }
        document["towers"].append(tower_doc)

    producer.send(
        topic=TOPIC,
        key="ALL_TOWERS",
        value=document
    )

    print("Sent Unified Environment Document:", document)
    time.sleep(10)
