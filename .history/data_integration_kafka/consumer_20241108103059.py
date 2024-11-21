from kafka import KafkaConsumer
import json
import time
from datetime import datetime

# Fonction pour désérialiser les messages JSON
def json_deserializer(data):
    return json.loads(data.decode('utf-8'))

if __name__ == '__main__':
    # Créer un Kafka Consumer
    consumer = KafkaConsumer(
        'Ghisla',  # Nom du topic à consommer
        bootstrap_servers=['localhost:9092'],  # Adresse du broker Kafka
        auto_offset_reset='earliest',  # Lire les messages depuis le début du topic
        enable_auto_commit=True,  # Confirme automatiquement les messages lus
        group_id='consumer-group-1',  # Groupe de consommateurs
        value_deserializer=json_deserializer  # Désérialisation en JSON
    )
    # Consommer les messages du topic
    print("En attente de messages...")

    for message in consumer:
        current_timestamp = datetime.now().timestamp()
        message_value = message.value
        message_ts = message_value.get("timestamp")
        ts_delta = (current_timestamp - message_ts) if message_ts is not None else -1
        #print(f"Message reçu: {message.value}")
        print(f"Message reçu: {message.value} with processing time {ts_delta} Seconds")