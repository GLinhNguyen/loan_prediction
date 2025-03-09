# spark_streaming_consumer.py
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.ml.classification import GBTClassificationModel
from pymongo import MongoClient
from etl import get_spark_session, train_preprocessor
import json
import pandas as pd

class SparkStreamingConsumer:
    def __init__(self):
        self.spark = get_spark_session("KafkaSparkStreaming")
        self.consumer = KafkaConsumer(
            "loan-applications",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        self.model = GBTClassificationModel.load("hdfs://localhost:9000/models/gbt_model")
        self.mongo_client = MongoClient("mongodb://localhost:27017/")
        self.db = self.mongo_client["predictions_db"]
        self.collection = self.db["predictions"]

    def process_message(self, loan_data):
        # Convert to Spark DataFrame
        df = self.spark.createDataFrame(pd.DataFrame([loan_data]))
        
        # Preprocess data
        transformed_data, _ = train_preprocessor(df)
        
        # Make prediction
        prediction = self.model.transform(transformed_data)
        prediction_value = prediction.select("prediction").collect()[0]["prediction"]
        
        # Prepare data for MongoDB
        result = loan_data.copy()
        result["prediction"] = int(prediction_value)
        
        # Store in MongoDB
        self.collection.insert_one(result)
        print(f"Stored in MongoDB: {result}")

    def run(self):
        print("🚀 Waiting for messages...")
        for message in self.consumer:
            loan_data = message.value
            print(f"Consumed: {loan_data}")
            self.process_message(loan_data)

if __name__ == "__main__":
    consumer = SparkStreamingConsumer()
    consumer.run()