from fastapi import FastAPI, Request, HTTPException, status, WebSocket, WebSocketDisconnect
import redis
import time
import json
import asyncio
from confluent_kafka import Producer
from redis import asyncio as aioredis 

app = FastAPI(title="BotGuard ML Ingestion API")

# Connect to Redis to check if ML worker has banned the IP
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# KAFKA SETUP
kafka_conf = {'bootstrap.servers': 'localhost:9092'}
kafka_producer = Producer(kafka_conf)
KAFKA_TOPIC = "api-traffic-logs"

def delivery_report(err, msg):
    if err is None:
        pass # Silently log to keep console clean

def broadcast_event(event_type: str, ip: str):
    message = json.dumps({"event": event_type, "ip": ip, "timestamp": time.time()})
    redis_client.publish("dashboard_updates", message)

@app.get("/checkout")
async def process_checkout(request: Request):
    client_ip = request.client.host
    
    # 1. Check if ML has banned this IP
    if redis_client.get(f"blacklisted:{client_ip}"):
        broadcast_event("ml_blocked_attempt", client_ip)
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="IP Blacklisted by ML Anomaly Detection")

    # 2. Log it to Kafka for the ML Worker
    log_data = {"ip": client_ip, "timestamp": time.time(), "endpoint": "/checkout"}
    kafka_producer.produce(KAFKA_TOPIC, value=json.dumps(log_data), callback=delivery_report)
    kafka_producer.poll(0)

    # 3. Update dashboard that this was ALLOWED!
    broadcast_event("allowed", client_ip)
    
    return {"status": "success", "message": "Checkout complete!", "ip": client_ip}

# WebSocket Endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    async_redis = await aioredis.from_url("redis://localhost:6379", decode_responses=True)
    pubsub = async_redis.pubsub()
    await pubsub.subscribe("dashboard_updates")

    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message:
                await websocket.send_text(message["data"])
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        print("Dashboard disconnected")
    finally:
        await pubsub.unsubscribe("dashboard_updates")
        await async_redis.close()