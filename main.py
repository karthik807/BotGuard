from fastapi import FastAPI, Request, HTTPException, status, WebSocket, WebSocketDisconnect
import redis
import time
import json
import asyncio
from confluent_kafka import Producer
from redis import asyncio as aioredis # We need async redis for WebSockets

app = FastAPI(title="BotGuard API Gateway")

# Standard synchronous Redis for the Rate Limiter
redis_client = redis.Redis(host='localhost', port=6380, db=0, decode_responses=True)

RATE_LIMIT = 50
WINDOW_SECONDS = 60

kafka_conf = {'bootstrap.servers': 'localhost:9092'}
kafka_producer = Producer(kafka_conf)
KAFKA_TOPIC = "api-traffic-logs"

def delivery_report(err, msg):
    if err is None:
        print(f"✅ Message delivered to Kafka Topic: {msg.topic()}")

# --- NEW: Broadcasting Function ---
def broadcast_event(event_type: str, ip: str):
    """Publishes an event to the Redis 'dashboard_updates' channel"""
    message = json.dumps({"event": event_type, "ip": ip, "timestamp": time.time()})
    redis_client.publish("dashboard_updates", message)

def check_rate_limit(ip_address: str):
    redis_key = f"rate_limit:{ip_address}"
    current_count = redis_client.get(redis_key)
    
    if current_count and int(current_count) >= RATE_LIMIT:
        # Broadcast that a block occurred!
        broadcast_event("blocked", ip_address)
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too Many Requests. You are being rate limited."
        )
    
    pipeline = redis_client.pipeline()
    pipeline.incr(redis_key)
    if not current_count:
        pipeline.expire(redis_key, WINDOW_SECONDS)
    pipeline.execute()

@app.get("/checkout")
async def process_checkout(request: Request):
    client_ip = request.client.host
    check_rate_limit(client_ip)
    
    log_data = {
        "ip": client_ip,
        "timestamp": time.time(),
        "endpoint": "/checkout",
        "user_agent": request.headers.get("user-agent", "unknown")
    }
    
    kafka_producer.produce(KAFKA_TOPIC, value=json.dumps(log_data), callback=delivery_report)
    kafka_producer.poll(0) 
    
    # Broadcast that a successful request occurred!
    broadcast_event("allowed", client_ip)
    return {"status": "success", "message": "Checkout complete!", "ip": client_ip}

# --- NEW: WebSocket Endpoint ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    # We use aioredis because WebSockets require asynchronous communication
    async_redis = await aioredis.from_url("redis://localhost:6380", decode_responses=True)
    pubsub = async_redis.pubsub()
    await pubsub.subscribe("dashboard_updates")
    
    try:
        while True:
            # Listen for messages on the Redis channel
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message:
                # Send the message to the React frontend
                await websocket.send_text(message["data"])
            await asyncio.sleep(0.1) # Small pause to prevent CPU pegging
    except WebSocketDisconnect:
        print("Dashboard disconnected")
    finally:
        await pubsub.unsubscribe("dashboard_updates")
        await async_redis.close()