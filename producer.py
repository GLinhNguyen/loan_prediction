from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

loan_purposes = ["Home", "Auto", "Other"]
education_levels = ["High School", "Bachelor's", "Master's", "PhD"]
employment_types = ["Full-time", "Part-time", "Unemployed"]
marital_statuses = ["Single", "Married", "Divorced"]
loan_statuses = ["Approved", "Denied"]

while True:
    data = {
        "LoanID": f"{random.randint(1000, 9999)}-{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}",
        "Age": random.randint(21, 70),
        "Income": random.randint(30000, 150000),
        "LoanAmount": random.randint(5000, 50000),
        "CreditScore": random.randint(300, 850),
        "MonthsEmployed": random.randint(0, 360),
        "NumCreditLines": random.randint(1, 10),
        "InterestRate": round(random.uniform(2.5, 25.0), 2),
        "LoanTerm": random.choice([12, 24, 36, 48, 60]),
        "DTIRatio": round(random.uniform(0.1, 0.8), 2),
        "Education": random.choice(education_levels),
        "EmploymentType": random.choice(employment_types),
        "MaritalStatus": random.choice(marital_statuses),
        "HasMortgage": random.choice(["Yes", "No"]),
        "HasDependents": random.choice(["Yes", "No"]),
        "LoanPurpose": random.choice(loan_purposes),
        "HasCoSigner": random.choice(["Yes", "No"]),
        "LoanStatus": random.choice(loan_statuses),
    }
    

    producer.send("loan-applications", data)
    print("✅ Sent loan application:", data)
    time.sleep(1)
  
