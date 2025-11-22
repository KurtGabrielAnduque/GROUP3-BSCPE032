#  Streaming Data Dashboard:  TIP Air Watch

Tip Air Watch is a big data–driven streaming platform designed to monitor daily air quality conditions at the Technological Institute of the Philippines – Quezon City. It serves as a web-based application that applies big data engineering methods and real-time data aggregation to present continuous air quality information. The system collects data from physical sensors and external APIs to display key environmental indicators and other related metrics. It is developed to provide reliable, real-time insights and ensure uninterrupted access to air quality data.

The system gathers data from **physical sensors** and **external APIs** to present key environmental indicators such as:

- PM2.5  
- PM10  
- CO₂ Levels  
- Temperature  
- Other air quality metrics  

It is designed to deliver **reliable, real-time insights** and ensure uninterrupted access to air quality data.

---

## 🏗️ Architecture & Components

This application is built using the following **Pipeline Stack**:

### 🗄️ MongoDB
A NoSQL, document-based database used to store large volumes of semi-structured and time-series air quality data. It supports fast queries and aggregation, making it ideal for handling high-frequency streamed data.

### 📡 Apache Kafka
An event streaming platform that ingests real-time air quality data and organizes it into **topics and partitions**, enabling:
- Parallel processing  
- Horizontal scalability  
- Reliable message delivery through replication and fault tolerance  

### ⚡ Apache Spark
A distributed analytics engine that performs real-time stream processing using **Spark Structured Streaming**. It:
- Consumes data from Kafka  
- Performs window-based aggregations  
- Ensures consistent data delivery using checkpointing and fault-tolerant mechanisms  

---

## 🌐 Data Source – API Used

### ☁️ Open-Meteo API
Open-Meteo is a free, open-source weather API that provides accurate **hourly weather data** using global and mesoscale weather models.  
It delivers data in **JSON format over HTTP**, enabling simple and efficient integration for real-time weather monitoring.

---

### Application Features
* Real-time air quality data streaming
* Interactive data visualizations
* Automatic dataset storage and management

---

## 🏃‍♂️ Setup & Execution

### Prerequisites
Python 3.8+, Apache Kafka, MongoDB.

### Setup
1. **Setup environment**
    - Download miniconda
    - Create your python environment
    ```bash
    conda create -n bigdata python=3.10.13
    ```
2.  **Clone Repo & Install:**
    ```bash
    git clone [REPO_URL]
    conda activate bigdata
    pip install -r requirements.txt
    ```
3.  **Configure:** Set up Kafka and your chosen Storage System.
4.  **Optional Environment File (`.env`):** Use for connection details.

### Execution
1.  **Start Kafka Broker** (and Controller).
2.  **Start Producer:**
    ```bash
    python producer.py
    ```
3.  **Launch Dashboard:**
    ```bash
    streamlit run app.py
    ```

---
