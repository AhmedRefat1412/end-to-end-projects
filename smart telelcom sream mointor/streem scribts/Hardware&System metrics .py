import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
import random

# ===============================
# Kafka Config
# ===============================
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "telecom.tower.system"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5
)

# ===============================
# Towers Initial State
# ===============================
TOWERS = [
    {"id": "TOWER_1", "region": "Cairo"},
    {"id": "TOWER_2", "region": "Giza"},
    {"id": "TOWER_3", "region": "Alex"}
]

# لكل tower نخزن state
tower_states = {}
for tower in TOWERS:
    tower_states[tower["id"]] = {
        "cpu_pct": random.randint(20, 40),
        "memory_pct": random.randint(30, 50),
        "power_kw": round(random.uniform(2.5, 3.5), 2),
        "battery_level_pct": random.randint(70, 100)
    }

print("🖥️ Unified System Metrics Producer Started...")

# ===============================
# Streaming Loop
# ===============================
while True:
    document = {"event_time": datetime.now(timezone.utc).isoformat(), "towers": []}

    for tower in TOWERS:
        state = tower_states[tower["id"]]

        # Update each metric gradually
        state["cpu_pct"] = min(100, max(0, state["cpu_pct"] + random.randint(-5, 5)))
        state["memory_pct"] = min(100, max(0, state["memory_pct"] + random.randint(-3, 3)))
        state["power_kw"] = round(max(0, state["power_kw"] + random.uniform(-0.2, 0.2)), 2)
        state["battery_level_pct"] = min(100, max(0, state["battery_level_pct"] - random.uniform(0, 1)))

        # Append tower data to document
        tower_doc = {
            "tower_id": tower["id"],
            "region": tower["region"],
            "cpu_pct": state["cpu_pct"],
            "memory_pct": state["memory_pct"],
            "power_kw": state["power_kw"],
            "battery_level_pct": round(state["battery_level_pct"], 2)
        }
        document["towers"].append(tower_doc)

    # Send unified document as one message
    producer.send(
        topic=TOPIC,
        key="ALL_TOWERS",
        value=document
    )

    print("Sent Unified Document:", document)
    time.sleep(10)
