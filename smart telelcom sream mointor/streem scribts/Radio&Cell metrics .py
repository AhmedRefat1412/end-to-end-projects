import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
import random

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "telecom.tower.radio"

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

# Stateful
tower_states = {}
for tower in TOWERS:
    tower_states[tower["id"]] = {
        "signal_dbm": random.randint(-110, -90),
        "cell_load_pct": random.randint(30, 60),
        "handover_rate": round(random.uniform(0.0, 2.0), 2),
        "drop_call_rate": round(random.uniform(0.0, 1.0), 2)
    }

print("📡 Unified Radio Metrics Producer Started...")

while True:
    document = {"event_time": datetime.now(timezone.utc).isoformat(), "towers": []}

    for tower in TOWERS:
        state = tower_states[tower["id"]]

        state["signal_dbm"] = max(-120, min(-50, state["signal_dbm"] + random.randint(-2, 2)))
        state["cell_load_pct"] = max(0, min(100, state["cell_load_pct"] + random.randint(-5, 5)))
        state["handover_rate"] = max(0, round(state["handover_rate"] + random.uniform(-0.2, 0.2), 2))
        state["drop_call_rate"] = max(0, round(state["drop_call_rate"] + random.uniform(-0.1, 0.1), 2))

        tower_doc = {
            "tower_id": tower["id"],
            "region": tower["region"],
            "signal_dbm": state["signal_dbm"],
            "cell_load_pct": state["cell_load_pct"],
            "handover_rate": state["handover_rate"],
            "drop_call_rate": state["drop_call_rate"]
        }
        document["towers"].append(tower_doc)

    producer.send(
        topic=TOPIC,
        key="ALL_TOWERS",
        value=document
    )

    print("Sent Unified Radio Document:", document)
    time.sleep(10)
