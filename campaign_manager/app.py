from flask import Flask, request, jsonify
import sqlite3
import redis
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

app = Flask(__name__)

# DB local
conn = sqlite3.connect("campaigns.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS campaigns (
    id TEXT PRIMARY KEY,
    segment TEXT,
    bid REAL
)
""")
conn.commit()

# Redis (para sincronizar con bidder)
r = redis.Redis(host="redis", port=6379, decode_responses=True)


@app.route("/campaign", methods=["POST"])
def create_campaign():
    data = request.json

    campaign = {
        "id": data["id"],
        "segment": data["segment"],
        "bid": data["bid"]
    }

    cursor.execute(
        "INSERT OR REPLACE INTO campaigns (id, segment, bid) VALUES (?, ?, ?)",
        (campaign["id"], campaign["segment"], campaign["bid"])
    )
    conn.commit()

    producer.send("campaigns", campaign)
    producer.flush()

    return {"status": "event sent"}


@app.route("/campaigns", methods=["GET"])
def list_campaigns():
    cursor.execute("SELECT * FROM campaigns")
    rows = cursor.fetchall()

    return jsonify([
        {"id": r[0], "segment": r[1], "bid": r[2]}
        for r in rows
    ])


@app.route("/health")
def health():
    return "ok"


app.run(host="0.0.0.0", port=7000)