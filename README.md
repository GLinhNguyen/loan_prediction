~~~
loan_prediction_project/
├── etl.py
├── kafka_producer.py
├── spark_streaming_consumer.py
├── model_training.py
├── extract_mongo.py
├── deploy_model.py
├── airflow_dag.py
├── app.py           # New Streamlit entry file
├── utils.py         # Helper functions for Streamlit
└── requirements.txt
~~~
1. pip install -r requirements.txt

2. In separate terminal: 
python kafka_producer.py
python spark_streaming_consumer.py

3. streamlit run app.py