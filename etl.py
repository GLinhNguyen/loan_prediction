# etl.py
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder, Imputer, MinMaxScaler
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, sum, when, lit

def get_spark_session(app_name="LoanPrediction"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/predictions_db.predictions") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/predictions_db.predictions") \
        .getOrCreate()

def train_preprocessor(df):
    # Handle null values
    df = df.withColumn("HasDependents", when(col("HasDependents").isNull(), lit("Unknown")).otherwise(col("HasDependents"))) \
           .withColumn("LoanPurpose", when(col("LoanPurpose").isNull(), lit("Unknown")).otherwise(col("LoanPurpose"))) \
           .withColumn("HasCoSigner", when(col("HasCoSigner").isNull(), lit("Unknown")).otherwise(col("HasCoSigner"))) \
           .withColumn("Default", when(col("Default").isNull(), lit(-1)).otherwise(col("Default")))

    # Define column types
    categorical_cols = ["Education", "EmploymentType", "MaritalStatus", "HasMortgage", 
                       "HasDependents", "LoanPurpose", "HasCoSigner"]
    numerical_cols = ["Age", "Income", "LoanAmount", "CreditScore", "MonthsEmployed",
                     "NumCreditLines", "InterestRate", "LoanTerm", "DTIRatio"]
    target_col = "Default"

    # Impute missing numerical values
    imputer = Imputer(inputCols=numerical_cols, outputCols=numerical_cols)
    df = imputer.fit(df).transform(df)

    # Create preprocessing pipeline
    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_index", handleInvalid='skip') for c in categorical_cols]
    encoders = [OneHotEncoder(inputCol=f"{c}_index", outputCol=f"{c}_encoded") for c in categorical_cols]
    assembler_inputs = [f"{c}_encoded" for c in categorical_cols] + numerical_cols
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
    label_indexer = StringIndexer(inputCol=target_col, outputCol="label")
    scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")

    pipeline = Pipeline(stages=indexers + encoders + [assembler, label_indexer, scaler])
    required_columns = categorical_cols + numerical_cols + [target_col]
    df = df.select(*required_columns)
    
    # Transform data
    transformed_model = pipeline.fit(df)
    scaled_data = transformed_model.transform(df)
    
    return scaled_data, transformed_model

if __name__ == "__main__":
    spark = get_spark_session()
    df = spark.read.csv("E:/third year/Scalable & Distributed/Project/Loan_default.csv", header=True, inferSchema=True)
    scaled_data, _ = train_preprocessor(df)
    scaled_data.write.csv("cleaned_data.csv", header=True)