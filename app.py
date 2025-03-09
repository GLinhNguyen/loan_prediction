# app.py
import streamlit as st
import pandas as pd
import time
from utils import get_mongo_data, check_process_status, start_process

st.set_page_config(page_title="Loan Prediction Dashboard", layout="wide")

def main():
    st.title("Loan Prediction System Dashboard")

    # Sidebar for controls
    st.sidebar.header("System Controls")
    if st.sidebar.button("Start Producer"):
        status = start_process("kafka_producer.py")
        st.sidebar.write(status)
    if st.sidebar.button("Start Consumer"):
        status = start_process("spark_streaming_consumer.py")
        st.sidebar.write(status)

    # Status section
    col1, col2 = st.columns(2)
    with col1:
        producer_status, producer_pid = check_process_status("kafka_producer")
        st.metric("Producer Status", "Running" if producer_status else "Stopped", 
                 f"PID: {producer_pid}" if producer_pid else "")
    with col2:
        consumer_status, consumer_pid = check_process_status("spark_streaming_consumer")
        st.metric("Consumer Status", "Running" if consumer_status else "Stopped",
                 f"PID: {consumer_pid}" if consumer_pid else "")

    # Real-time predictions
    st.header("Recent Predictions")
    placeholder = st.empty()
    
    # Metrics
    col3, col4 = st.columns(2)
    with col3:
        st.subheader("Prediction Distribution")
        pred_chart = st.empty()
    with col4:
        st.subheader("Key Statistics")
        stats_placeholder = st.empty()

    # Continuous update
    while True:
        df = get_mongo_data()
        
        # Update table
        with placeholder.container():
            st.dataframe(df[['LoanID', 'LoanAmount', 'CreditScore', 'prediction', 'LoanPurpose']],
                        use_container_width=True)
        
        # Update prediction distribution
        if not df.empty:
            pred_dist = df['prediction'].value_counts().rename({0: "No Default", 1: "Default"})
            with pred_chart:
                st.bar_chart(pred_dist)
            
            # Update statistics
            with stats_placeholder:
                st.metric("Total Predictions", len(df))
                st.metric("Default Rate", f"{(df['prediction'].mean()*100):.2f}%")
                st.metric("Average Loan Amount", f"${df['LoanAmount'].mean():,.2f}")

        time.sleep(5)  # Refresh every 5 seconds

if __name__ == "__main__":
    main()