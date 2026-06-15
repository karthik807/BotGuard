import json
import time
import redis
import numpy as np
from collections import defaultdict
from confluent_kafka import Consumer
from sklearn.ensemble import IsolationForest

print("🧠 Initializing ML Model (Isolation Forest)...")

# 1. Train model on [request_count, avg_interval, std_dev_interval]
# Humans: low request count, LONG intervals (6-25s), HIGH std_dev (erratic timing)
# Bots:   high request count, SHORT intervals (~1.5s), NEAR-ZERO std_dev (robotic precision)
baseline_human_traffic = np.array([
    [2, 12.0, 4.5],
    [3, 18.0, 5.2],
    [2, 25.0, 7.1],
    [3,  9.0, 3.8],
    [2, 15.0, 6.0],
    [4, 20.0, 8.3],
    [2, 10.0, 3.2],
    [3, 14.0, 4.9],
])

model = IsolationForest(contamination=0.1, random_state=42)
model.fit(baseline_human_traffic)
print(f"✅ Model trained. Sample bot score [20, 1.5, 0.0]: {model.score_samples([[20, 1.5, 0.0]])[0]:.3f} (more negative = more anomalous)")

redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'ml-bot-detector',
    'auto.offset.reset': 'latest'
})
consumer.subscribe(['api-traffic-logs'])

traffic_buffer = defaultdict(list)
print("🎧 ML Worker listening to Kafka stream for stealthy bots...")

while True:
    msg = consumer.poll(1.0)
    if msg is None or msg.error(): continue
        
    log = json.loads(msg.value().decode('utf-8'))
    ip = log['ip']
    current_time = log['timestamp']
    
    
    traffic_buffer[ip].append(current_time)
    traffic_buffer[ip] = [ts for ts in traffic_buffer[ip] if current_time - ts < 60]
        
    # Buffer 5 requests to establish a pattern
    if len(traffic_buffer[ip]) >= 3:
        # Feature Engineering
        intervals = [traffic_buffer[ip][i] - traffic_buffer[ip][i-1] for i in range(1, len(traffic_buffer[ip]))]
        avg_interval = sum(intervals) / len(intervals)
        std_dev = np.std(intervals) # <--- THE MAGIC BULLET    
        features = np.array([[len(traffic_buffer[ip]), avg_interval, std_dev]])
        prediction = model.predict(features)
        score = model.score_samples(features)[0]
        print(f"📊 IP {ip} | count={len(traffic_buffer[ip])} avg_interval={avg_interval:.2f}s std_dev={std_dev:.4f}s | score={score:.3f} | {'🚨 ANOMALY' if prediction[0] == -1 else '✅ normal'}")
            
        # Predict: -1 means Anomaly
        if prediction[0] == -1:
            print(f"🚨 ML PATTERN ANOMALY DETECTED! Banning IP: {ip} (Variance: {std_dev:.4f}s)")
                
            redis_client.set(f"blacklisted:{ip}", "true", ex=200)
            message = json.dumps({"event": "ml_blocked", "ip": ip, "timestamp": current_time})
            redis_client.publish("dashboard_updates", message)
                
            traffic_buffer[ip] = []