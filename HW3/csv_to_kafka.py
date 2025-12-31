import csv
import json
import time
from kafka import KafkaProducer
from glob import glob
from kafka.errors import NoBrokersAvailable

time.sleep(30)

for i in range(10):
    print("attempt = ",i)
    try:
        producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        break
    except NoBrokersAvailable:
        print("Kafka not ready, retrying...")
        time.sleep(5)
else:
    raise RuntimeError("Kafka not available")

topic = "sales_stream"

files = sorted(glob("/data/*.csv"))

for file in files:
    with open(file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            producer.send(topic, row)
            time.sleep(0.01)

producer.flush()