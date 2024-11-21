from kafka import KafkaProducer
import json
import time
import os

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

current_path = os.getcwd()
file_path = f'{current_path}/data/hospital-utilization-trends.csv'
batch_size = 100

with open(file_path, 'r') as file:
    headers = file.readline()  # Skip headers
    lines = file.readlines()

for i in range(0, len(lines), batch_size):
    batch = lines[i:i + batch_size]
    producer.send('hospital_trends', {'data': batch})
    print(f"Batch {i // batch_size + 1} sent to Kafka")
    time.sleep(10)

print("All data sent to Kafka.")