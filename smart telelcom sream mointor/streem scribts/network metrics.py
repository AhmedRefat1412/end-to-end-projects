import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
import random

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "telecom.tower.network"

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
        "latency_ms": random.randint(80, 150),
        "throughput_mbps": random.randint(20, 60),
        "packet_loss": round(random.uniform(0, 0.05), 3),
        "active_users": random.randint(50, 200)
    }

print(" Unified Network Metrics Producer Started...")

while True:
    document = {"event_time": datetime.now(timezone.utc).isoformat(), "towers": []}

    for tower in TOWERS:
        state = tower_states[tower["id"]]

        state["latency_ms"] = max(10, state["latency_ms"] + random.randint(-5, 5))
        state["throughput_mbps"] = max(1, state["throughput_mbps"] + random.randint(-5, 5))
        state["packet_loss"] = max(0, round(state["packet_loss"] + random.uniform(-0.01, 0.01), 3))
        state["active_users"] = max(0, state["active_users"] + random.randint(-5, 5))

        tower_doc = {
            "tower_id": tower["id"],
            "region": tower["region"],
            "latency_ms": state["latency_ms"],
            "throughput_mbps": state["throughput_mbps"],
            "packet_loss": state["packet_loss"],
            "active_users": state["active_users"]
        }
        document["towers"].append(tower_doc)

    producer.send(
        topic=TOPIC,
        key="ALL_TOWERS",
        value=document
    )

    print("Sent Unified Network Document:", document)
    time.sleep(10)
