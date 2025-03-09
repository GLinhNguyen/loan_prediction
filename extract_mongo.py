# extract_mongo.py
from pymongo import MongoClient
import pandas as pd

def extract_mongo_data():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["predictions_db"]
    collection = db["predictions"]

    # Extract data
    data = list(collection.find({}, {"_id": 0}))
    df = pd.DataFrame(data)
    
    # Save to CSV
    output_path = "/tmp/mongo_data.csv"
    df.to_csv(output_path, index=False)
    print(f"Extracted data saved to {output_path}")

if __name__ == "__main__":
    extract_mongo_data()