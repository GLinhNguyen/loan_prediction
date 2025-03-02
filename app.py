import streamlit as st
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder, VectorAssembler, MinMaxScaler
from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pandas as pd
import json
from kafka import KafkaProducer, KafkaConsumer
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TestSpark") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

# Kafka Configuration
KAFKA_TOPIC = "loan_applications"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Function to preprocess the data
def preprocess_data(inputs):
    data = spark.createDataFrame([inputs])
    imputer = Imputer(inputCols=numerical_cols, outputCols=numerical_cols)
    df_imputed = imputer.fit(data).transform(data)
    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_index", handleInvalid='keep') for c in categorical_cols]
    encoders = [OneHotEncoder(inputCol=f"{c}_index", outputCol=f"{c}_encoded") for c in categorical_cols]
    assembler_inputs = [f"{c}_encoded" for c in categorical_cols] + numerical_cols
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
    scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")
    pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler])
    df_transformed = pipeline.fit(df_imputed).transform(df_imputed)
    return df_transformed

# Function to predict loan default
def predict_default(inputs):
    transformed_data = preprocess_data(inputs)
    prediction = dt_model.transform(transformed_data)
    return prediction.select("prediction").head()[0]

# Sidebar Form
st.sidebar.title("Submit Loan Details")
with st.sidebar.form(key="loan_form"):
    age = st.number_input("Age", min_value=18, max_value=100)
    income = st.number_input("Income", min_value=0)
    loan_amount = st.number_input("Loan Amount", min_value=0)
    credit_score = st.number_input("Credit Score", min_value=300, max_value=850)
    months_employed = st.number_input("Months Employed", min_value=0)
    num_credit_lines = st.number_input("Number of Credit Lines", min_value=0)
    interest_rate = st.number_input("Interest Rate", min_value=0.0)
    loan_term = st.number_input("Loan Term (in months)", min_value=12, max_value=360)
    dti_ratio = st.number_input("DTI Ratio", min_value=0.0)
    education = st.selectbox("Education", ["High School", "Undergraduate", "Graduate"])
    employment_type = st.selectbox("Employment Type", ["Full-time", "Part-time", "Self-employed", "Unemployed"])
    marital_status = st.selectbox("Marital Status", ["Single", "Married", "Divorced", "Widowed"])
    has_mortgage = st.selectbox("Has Mortgage", ["Yes", "No"])
    has_dependents = st.selectbox("Has Dependents", ["Yes", "No"])
    loan_purpose = st.selectbox("Loan Purpose", ["Home", "Car", "Education", "Debt Consolidation", "Other"])
    has_cosigner = st.selectbox("Has Co-Signer", ["Yes", "No"])

    inputs = {
        "Age": age,
        "Income": income,
        "LoanAmount": loan_amount,
        "CreditScore": credit_score,
        "MonthsEmployed": months_employed,
        "NumCreditLines": num_credit_lines,
        "InterestRate": interest_rate,
        "LoanTerm": loan_term,
        "DTIRatio": dti_ratio,
        "Education": education,
        "EmploymentType": employment_type,
        "MaritalStatus": marital_status,
        "HasMortgage": 1 if has_mortgage == "Yes" else 0,
        "HasDependents": 1 if has_dependents == "Yes" else 0,
        "LoanPurpose": loan_purpose,
        "HasCoSigner": 1 if has_cosigner == "Yes" else 0
    }

    submit_button = st.form_submit_button(label="Predict Default")

    if submit_button:
        producer.send(KAFKA_TOPIC, value=inputs)
        st.sidebar.success("Loan application submitted to Kafka queue.")

# Real-time Loan Monitoring
st.title("Real-Time Loan Approval Processing")
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset='earliest'
)

data_placeholder = st.empty()
real_time_data = pd.DataFrame()

for message in consumer:
    new_row = message.value
    prediction = predict_default(new_row)
    new_row["Prediction"] = "Approved" if prediction == 1.0 else "Denied"
    real_time_data = pd.concat([real_time_data, pd.DataFrame([new_row])], ignore_index=True)

    with data_placeholder.container():
        st.write(real_time_data)
        if new_row["Prediction"] == "Approved":
            st.success("Loan Approved.")
        else:
            st.error("Loan Denied.")

