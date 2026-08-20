# 🛫 Real-Time Flight Delay Prediction Pipeline (Lambda Architecture)

An end-to-end data engineering and machine learning pipeline that ingests, processes, and predicts flight delays in real-time. Built with a Lambda Architecture using Docker, Apache Kafka, PySpark Structured Streaming, Hadoop HDFS, and Streamlit.

## 🏗️ Architecture & Tech Stack
* **Containerization:** Docker & Docker Compose
* **Storage:** Hadoop HDFS (Data Lake)
* **Message Broker:** Apache Kafka
* **Processing engine:** PySpark (Batch & Structured Streaming)
* **Machine Learning:** Spark MLlib (XGBoost/RandomForest)
* **Dashboard:** Streamlit & Pandas

## 📂 Project Structure
* `docker-compose.yml`: Infrastructure setup (Kafka, Hadoop, Spark, Jupyter).
* `app.py`: Real-time Streamlit dashboard.
* `Notebooks/`
  * `batch_transformation.ipynb`: Loads raw CSVs into HDFS.
  * `model_training.ipynb`: Trains the ML model on historical HDFS data.
  * `stream_transformation.ipynb`: Cleans real-time Kafka data.
  * `kafka_consumer.ipynb`: Loads the ML model, makes live predictions, and pushes to a new Kafka topic.
  * `kafka_producer.ipynb`: Simulates a real-time data stream into Kafka.

---

## 🚀 How to Run the Project (Instructor Guide)
### Phase 0: See data sources folder and follow it's guide
### Phase 1: Infrastructure & Setup (Run Once)
1. **Start the cluster:** Run `docker compose up -d` in the root directory.
2. **Access Jupyter:** Open the Jupyter lab interface provided by the Spark container.
3. **Load Data to HDFS:** Run all cells in `Notebooks/batch_transformation.ipynb` to seed the HDFS data lake.
4. **Train the Model:** Run all cells in `Notebooks/model_training.ipynb` to generate the `.model` file needed for live inference.

### Phase 2: Real-Time Streaming Demo
Run these in the exact order below to ensure Kafka topics initialize correctly:

1. **Start the Dashboard:** 
   Run Streamlit via Docker connected to the Kafka network:
   ```bash
   docker run -it --rm --network nti_default -p 8501:8501 -v "$(pwd)/Notebooks":/app -w /app python:3.9-slim bash -c "pip install streamlit kafka-python pandas && streamlit run app.py --server.address 0.0.0.0"
