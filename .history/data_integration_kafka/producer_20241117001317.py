from kafka import KafkaProducer
import json
import time
from datetime import datetime
import threading


# Fonction pour sérialiser les messages en JSON
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Nom du topic Kafka
topic_name = 'Ghis_topic'

# Fonction qui envoie des messages dans Kafka
def send_messages(thread_id, num_messages, sleep_time = 0):
    for i in range(num_messages):
        message = {"thread": thread_id, "message_number": i, "text": "A fake text", "timestamp": datetime.now().timestamp()}
        producer.send(topic_name, value=message)
        print(f"Thread {thread_id} sent: {message}")
        time.sleep(sleep_time)

if __name__ == '__main__':
    # Create a kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=json_serializer  # JSON serializer
    )

    threads = []
    num_threads = 500
    num_messages = 100000
    sleep_time = 0

    # Create and start the threads
    for i in range(num_threads):
        thread = threading.Thread(target=send_messages, args=(i, num_messages, sleep_time))
        threads.append(thread)
        thread.start()

    # Wait for all the threads to complete
    for thread in threads:
        thread.join()

    #  Close the producer
    producer.flush()  # Make sure all the messages are sent
    producer.close()

