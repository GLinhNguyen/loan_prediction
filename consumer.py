from kafka import KafkaConsumer
import json
from etl import train_preprocessor

consumer = KafkaConsumer(
    "loan-applications",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("🚀 Waiting for messages...")
for message in consumer:
    loan_data = message.value
    print(f"Consumed: {loan_data}")
    
    transformed_data = train_preprocessor(loan_data)  # Call ETL
    print(f"Transformed Data: {transformed_data}")
