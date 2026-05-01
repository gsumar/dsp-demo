from flask import Flask, request
import redis
import json
import threading
from kafka import KafkaConsumer
import time
import numpy as np

# ------------------------
# GLOBAL STATE
# ------------------------

campaign_cache = {}

latencies = []
latencies_lock = threading.Lock()
MAX_LATENCIES = 5000


# ------------------------
# KAFKA CONSUMER
# ------------------------

def consume_campaigns():
    consumer = None

    while True:
        try:
            if consumer is None:
                print("Connecting to Kafka...")
                consumer = KafkaConsumer(
                    "campaigns",
                    bootstrap_servers="kafka:9092",
                    group_id="bidder-group",
                    auto_offset_reset="earliest",
                    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
                )
                print("Kafka consumer connected")

            for msg in consumer:
                campaign = msg.value
                campaign_cache[campaign["id"]] = campaign
                print("Updated campaign:", campaign)

        except Exception as e:
            print(f"Consumer error: {e}, retrying in 3s...")
            consumer = None
            time.sleep(3)


# ------------------------
# METRICS
# ------------------------

def report_metrics():
    print("METRICS THREAD STARTED")

    while True:
        time.sleep(5)

        with latencies_lock:
            if len(latencies) < 10:
                continue

            arr = np.array(latencies[-1000:])

        print("---- LATENCY ----", flush=True)
        print(f"p50: {np.percentile(arr, 50):.2f} ms", flush=True)
        print(f"p95: {np.percentile(arr, 95):.2f} ms", flush=True)
        print(f"p99: {np.percentile(arr, 99):.2f} ms", flush=True)


# ------------------------
# FLASK APP
# ------------------------

app = Flask(__name__)

r = redis.Redis(host="redis", port=6379, decode_responses=True)


@app.route("/bid", methods=["POST"])
def bid():
    start = time.time()

    data = request.json
    user_segment = r.get(f"user:{data['user_id']}")

    if not user_segment:
        latency = (time.time() - start) * 1000

        with latencies_lock:
            latencies.append(latency)
            if len(latencies) > MAX_LATENCIES:
                latencies.pop(0)

        return {"bid": 0}

    for campaign in campaign_cache.values():
        if campaign["segment"] == user_segment:
            if campaign["bid"] >= data.get("floor_price", 0):

                latency = (time.time() - start) * 1000

                with latencies_lock:
                    latencies.append(latency)
                    if len(latencies) > MAX_LATENCIES:
                        latencies.pop(0)

                return {
                    "bid": campaign["bid"],
                    "campaign_id": campaign["id"]
                }

    latency = (time.time() - start) * 1000

    with latencies_lock:
        latencies.append(latency)
        if len(latencies) > MAX_LATENCIES:
            latencies.pop(0)

    return {"bid": 0}


@app.route("/health")
def health():
    return "ok"


# ------------------------
# STARTUP
# ------------------------

if __name__ == "__main__":
    threading.Thread(target=consume_campaigns, daemon=True).start()
    threading.Thread(target=report_metrics, daemon=True).start()

    app.run(host="0.0.0.0", port=8000)