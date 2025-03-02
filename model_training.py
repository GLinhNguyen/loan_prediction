## Model Development
from pyspark.ml.classification import RandomForestClassifier

from etl import scaledData
from etl import scaledFeatures

def train_model():
    # --- Step 1: Prepare the Data ---
    train_data, test_data = scaledData.randomSplit([0.8 , 0.2], seed=42)

    # Random Forest Classifier
    rf = RandomForestClassifier(featuresCol= scaledData.select("scaledFeatures"), labelCol="label", numTrees=100, maxDepth=10)
   

    rf_model = rf.fit(train_data)
  

    # Save the Decision Tree model
    rf_model.write().overwrite().save("E:/third year/Scalable & Distributed/Project/rf_model")
