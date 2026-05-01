from flask import Flask, request, jsonify
import redis
import json
from kafka import KafkaConsumer
import json
import threading

campaign_cache = {}

def consume_campaigns():
    consumer = KafkaConsumer(
        "campaigns",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    for msg in consumer:
        campaign = msg.value
        campaign_cache[campaign["id"]] = campaign
        print("Updated campaign:", campaign)

threading.Thread(target=consume_campaigns, daemon=True).start()
app = Flask(__name__)

r = redis.Redis(host="redis", port=6379, decode_responses=True)


@app.route("/bid", methods=["POST"])
def bid():
    data = request.json
    user_segment = r.get(f"user:{data['user_id']}")

    for campaign in campaign_cache.values():
        if campaign["segment"] == user_segment:
            if campaign["bid"] >= data.get("floor_price", 0):
                return {
                    "bid": campaign["bid"],
                    "campaign_id": campaign["id"]
                }

    return {"bid": 0}


@app.route("/health")
def health():
    return "ok"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)