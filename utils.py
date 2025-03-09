# utils.py
from pymongo import MongoClient
import pandas as pd
import subprocess
import psutil
import os

def get_mongo_data(limit=100):
    client = MongoClient("mongodb://localhost:27017/")
    db = client["predictions_db"]
    collection = db["predictions"]
    data = list(collection.find({}, {"_id": 0}).sort([("$natural", -1)]).limit(limit))
    return pd.DataFrame(data)

def check_process_status(process_name):
    for proc in psutil.process_iter(['pid', 'name']):
        if process_name.lower() in proc.info['name'].lower():
            return True, proc.pid
    return False, None

def start_process(script_name):
    if not check_process_status(script_name)[0]:
        subprocess.Popen(["python", script_name])
        return f"Started {script_name}"
    return f"{script_name} already running"