# model_training.py
from etl import get_spark_session, train_preprocessor
from pyspark.ml.classification import RandomForestClassifier

def train_model():
    spark = get_spark_session("ModelTraining")
    df = spark.read.csv("E:/third year/Scalable & Distributed/Project/Loan_default.csv", 
                       header=True, inferSchema=True)
    
    # Preprocess data
    scaled_data, preprocessor_model = train_preprocessor(df)
    
    # Split data
    train_data, test_data = scaled_data.randomSplit([0.8, 0.2], seed=42)
    
    # Train model
    rf = RandomForestClassifier(
        featuresCol="scaledFeatures",
        labelCol="label",
        numTrees=100,
        maxDepth=10
    )
    rf_model = rf.fit(train_data)
    
    # Save models
    rf_model.write().overwrite().save("hdfs://localhost:9000/models/rf_model")
    preprocessor_model.write().overwrite().save("hdfs://localhost:9000/models/preprocessor")
    
    print("Model training completed and saved")

if __name__ == "__main__":
    train_model()