# Databricks notebook source
from confluent_kafka import Producer
import random
import time
from datetime import datetime, timedelta
import uuid


conf_dict = {
  "bootstrap.servers": "pkc-ymrq7.us-east-2.aws.confluent.cloud:9092",
  "security.protocol": "SASL_SSL",
  "sasl.mechanisms": "PLAIN",
  "sasl.username": dbutils.secrets.get(scope="confluent", key="username"),
  "sasl.password": dbutils.secrets.get(scope="confluent", key="password"),
  "session.timeout.ms": 45000
}

# COMMAND ----------

producer = Producer(conf_dict)

def generate_event():
    user_id = random.randint(1, 100)
    product_id = random.randint(1, 100)
    action = random.choice(["view", "add_to_cart", "purchase"])
    current_time = datetime.utcnow()

    if random.randint(1, 20) == 1:
        event_timestamp = current_time - timedelta(minutes=30)
    else:
        event_timestamp = current_time

    event = {
        "user_id": user_id,
        "product_id": product_id,
        "action": action,
        "event_timestamp": event_timestamp.isoformat()
    }
    return event

while True:
    for _ in range(10):
        event = generate_event()
        producer.produce("clickstream", key=str(uuid.uuid4()), value=str(event))
    producer.flush() # triggers an immediate sending of any buffered messages to the broker

# COMMAND ----------


