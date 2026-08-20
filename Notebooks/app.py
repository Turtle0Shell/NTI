import json
import time
import pandas as pd
import streamlit as st
from kafka import KafkaConsumer

st.set_page_config(page_title="Flight Delay Dashboard", layout="wide")
st.title("🛫 Real-Time Flight Delay Predictions")

# Initialize session state for persistent live buffer
if 'live_data' not in st.session_state:
    st.session_state['live_data'] = []


# Initialize consumer (supports both 'kafka:9092' in Docker or 'localhost:9092')
@st.cache_resource
def get_kafka_consumer():
    servers = ['kafka:9092', 'localhost:9092']
    return KafkaConsumer(
        'flight-predictions',
        bootstrap_servers=servers,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
    )


try:
    consumer = get_kafka_consumer()

    # Poll for available messages in batches
    raw_messages = consumer.poll(timeout_ms=1000, max_records=20)
    for topic_partition, messages in raw_messages.items():
        for message in messages:
            st.session_state['live_data'].append(message.value)

    # Maintain a rolling window of 100 flights
    if len(st.session_state['live_data']) > 100:
        st.session_state['live_data'] = st.session_state['live_data'][-100:]

except Exception as e:
    st.error(f"Kafka Connection Error: {e}")

# Render UI
if st.session_state['live_data']:
    df = pd.DataFrame(st.session_state['live_data'])

    # 1. Summary Metrics
    st.subheader("Live System Metrics")
    col1, col2, col3 = st.columns(3)
    col1.metric("Flights Processed", len(df))
    col2.metric(
        "Avg Predicted Delay", f"{df['Predicted_Arr_Delay'].mean():.1f} min"
    )
    col3.metric("Avg Actual Delay", f"{df['Actual_Arr_Delay'].mean():.1f} min")

    st.markdown("---")

    # 2. Comparison Chart
    st.subheader("Predicted vs. Actual Arrival Delay")
    st.line_chart(df[['Predicted_Arr_Delay', 'Actual_Arr_Delay']])

    # 3. Stream Table
    st.subheader("Live Predictions Feed (Latest Records)")
    st.dataframe(df.tail(10), use_container_width=True)

else:
    st.info(
        "⏳ Waiting for streaming data from Kafka topic 'flight-predictions'..."
    )

# Refresh interval
time.sleep(1)
st.rerun()