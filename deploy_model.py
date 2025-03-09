# deploy_model.py
import shutil
import os

def deploy_model():
    model_path = "hdfs://localhost:9000/models/rf_model"
    prod_path = "/opt/models/production_model"
    
    if os.path.exists(prod_path):
        shutil.rmtree(prod_path)
    
    shutil.copytree(model_path, prod_path)
    print(f"Deployed new model to {prod_path}")

if __name__ == "__main__":
    deploy_model()