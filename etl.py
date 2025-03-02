from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.ml.feature import StringIndexer, VectorAssembler,OneHotEncoder, Imputer, MinMaxScaler
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, sum, when, lit
from pyspark.sql import SparkSession


spark = SparkSession.builder \
        .appName("ModelTraining") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "100") \
        .getOrCreate()

df = spark.read.csv("E:/third year/Scalable & Distributed/Project/Loan_default.csv", header=True, inferSchema=True)

def train_preprocessor(df):
    # Count null values for each column
    null_counts = df.select(
        [sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]
    )

    # Collect the null counts as a dictionary
    null_columns = null_counts.first().asDict()

    # Filter columns with non-zero null counts
    null_columns_with_counts = {col_name: count for col_name, count in null_columns.items() if count > 0}

    # Print results
    if null_columns_with_counts:
        print("Columns with null values and their counts:")
        for col_name, count in null_columns_with_counts.items():
            print(f"{col_name}: {count}")
    else:
        print("No columns with null values found.")

        df = df.withColumn(
        "HasDependents",
        when(col("HasDependents").isNull(), lit("Unknown")).otherwise(col("HasDependents"))
    ).withColumn(
        "LoanPurpose",
        when(col("LoanPurpose").isNull(), lit("Unknown")).otherwise(col("LoanPurpose"))
    ).withColumn(
        "HasCoSigner",
        when(col("HasCoSigner").isNull(), lit("Unknown")).otherwise(col("HasCoSigner"))
    ).withColumn(
        "Default",
        when(col("Default").isNull(), lit(-1)).otherwise(col("Default"))     
    )
        
    # Identify categorical and numerical features
    categorical_cols = [
        "Education", "EmploymentType", "MaritalStatus",  # Updated categorical columns
        "HasMortgage", "HasDependents", "LoanPurpose", "HasCoSigner"
    ]
    numerical_cols = [
        "Age","Income", "LoanAmount", "CreditScore", "MonthsEmployed",  # Updated numerical columns
        "NumCreditLines", "InterestRate", "LoanTerm", "DTIRatio"
    ]
    target_col = "default"  # Updated target column


    # Impute missing values (if any exist)
    imputer = Imputer(inputCols=numerical_cols, outputCols=numerical_cols)
    df = imputer.fit(df).transform(df)


    # Convert categorical features to numerical using StringIndexer and OneHotEncoder
    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_index", handleInvalid='skip') for c in categorical_cols]
    encoders = [OneHotEncoder(inputCol=f"{c}_index", outputCol=f"{c}_encoded") for c in categorical_cols]

    # Assemble features into a vector
    assembler_inputs = [f"{c}_encoded" for c in categorical_cols] + numerical_cols
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")


    # StringIndexer for the target variable
    label_indexer = StringIndexer(inputCol=target_col, outputCol="label")

    # Create a pipeline
    pipeline = Pipeline(stages=indexers + encoders + [assembler, label_indexer])
    required_columns = categorical_cols + numerical_cols + [target_col]
    df = df.select(*required_columns)
    # Fit and transform the data
    df_transformed = pipeline.fit(df).transform(df)

    # Show the transformed data with features
    df_transformed.select("features", "label").show(truncate=False)

    scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")

    # Fit the scaler to the data
    scalerModel = scaler.fit(df_transformed)

    # Transform the data
    scaledData = scalerModel.transform(df_transformed)

    # Show the scaled features
    scaledData.select("features", "scaledFeatures").show(truncate=False)

    return scaledData
df_cleaned.write.csv("cleaned_data.csv", header=True)

scaledData = train_preprocessor(df)