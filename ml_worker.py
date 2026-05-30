import json
import time
import numpy as np
import pandas as pd
from collections import defaultdict
import redis
from confluent_kafka import Consumer, KafkaException
from sklearn.ensemble import IsolationForest

# 1. Connect to our Redis Bouncer
redis_client = redis.Redis(host='localhost', port=6380, db=0, decode_responses=True)

# 2. Connect to our Redpanda (Kafka) Queue
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'ml-bot-detector-group', # This identifies our worker
    'auto.offset.reset': 'latest'        # We only care about new traffic
}
kafka_consumer = Consumer(kafka_conf)
kafka_consumer.subscribe(["api-traffic-logs"])

# 3. Create our Machine Learning Model
# We use an Isolation Forest. It is great for finding "weird" data points 
# without needing a pre-labeled dataset of "good vs bad" bots.
# contamination=0.1 means we assume about 10% of traffic might be anomalies
model = IsolationForest(contamination=0.1, random_state=42)

# --- NEW WARM-UP CODE ---
print("Warming up the ML model with simulated human traffic...")
# We generate 100 fake users with messy, random click times (high variance)
fake_human_features = []
for _ in range(100):
    # Random speed between 0.5 and 5 seconds, high variance
    speed = np.random.uniform(0.5, 5.0)
    variance = np.random.uniform(0.5, 3.0) 
    fake_human_features.append([speed, variance])

# We also add 5 fake bots with rigid timing (low variance)
fake_bot_features = [[1.0, 0.0], [0.5, 0.01], [2.0, 0.0], [0.1, 0.0], [1.5, 0.02]]

# Combine them and train the model!
training_data = fake_human_features + fake_bot_features
model.fit(training_data)
print("Model trained! Now it knows what humans look like.")
# ------------------------

# Dictionary to hold the recent history of timestamps for each IP
# E.g., ip_history["127.0.0.1"] = [1685000000.1, 1685000001.2, 1685000002.0]
ip_history = defaultdict(list)

def block_ip(ip: str):
    """ Tells Redis to block this IP for 1 hour (3600 seconds) """
    redis_key = f"rate_limit:{ip}"
    # Setting the counter artificially high guarantees they get the 429 error
    redis_client.set(redis_key, 9999) 
    redis_client.expire(redis_key, 3600)
    print(f"🚨 BOUNCER ALERT: ML Model blocked bot IP -> {ip}")
    # --- NEW: Broadcast the ML block event ---
    message = json.dumps({"event": "blocked_by_ml", "ip": ip, "timestamp": time.time()})
    redis_client.publish("dashboard_updates", message)

def extract_features(timestamps: list):
    """ 
    Feature Engineering: Converts raw timestamps into math the ML model can understand.
    Bots click with rigid timing. Humans are random.
    """
    if len(timestamps) < 5:
        # Not enough data to judge yet
        return None 
    
    # Calculate the time difference between each click
    time_diffs = np.diff(timestamps)
    
    # Feature 1: Average speed of clicks
    avg_speed = np.mean(time_diffs)
    # Feature 2: Variance (how robotic/rigid is the timing?)
    variance = np.var(time_diffs)
    
    # Return a 2D array, which is what scikit-learn requires
    return [[avg_speed, variance]]

print("🤖 ML Worker Started. Listening for traffic...")

try:
    while True:
        # Check the queue for new messages, waiting up to 1 second
        msg = kafka_consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            print(f"Kafka Error: {msg.error()}")
            continue
            
        # Parse the JSON log data we sent from FastAPI
        log_data = json.loads(msg.value().decode('utf-8'))
        ip = log_data['ip']
        timestamp = log_data['timestamp']
        
        # Add the new timestamp to this IP's history
        ip_history[ip].append(timestamp)
        
        # Keep only the last 20 requests to save memory (our rolling window)
        if len(ip_history[ip]) > 20:
            ip_history[ip].pop(0)
            
        # Do the math (Feature Engineering)
        features = extract_features(ip_history[ip])
        
        # If we have enough data (5+ requests), run it through the ML model
        if features:
            # We use fit_predict to analyze the data on the fly. 
            # It returns -1 if it thinks the data is an anomaly (a bot)
            prediction = model.predict(features)
            
            if prediction[0] == -1:
                block_ip(ip)
                # Clear the history so we don't spam the block command
                ip_history[ip] = [] 
                
except KeyboardInterrupt:
    print("Shutting down ML Worker...")
finally:
    # Always cleanly close the connection when stopping the script
    kafka_consumer.close()