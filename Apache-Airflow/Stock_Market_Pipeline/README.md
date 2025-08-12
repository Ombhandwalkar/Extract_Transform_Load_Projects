# 📈 Stock Market Data Pipeline – Apache Airflow + Astro CLI
A containerized, automated, and scalable data pipeline that fetches stock market data from Yahoo Finance API, processes it with PySpark, stores it in PostgreSQL for analytics, and visualizes trends in Metabase. Built with Apache Airflow for orchestration and integrated with Slack notifications for real-time monitoring.

---

## 📌 Key Highlights

### ⚙️ Orchestration with **Apache Airflow** (DAG-based workflow automation)

###  🌐 API Integration – Fetch real-time stock data from **Yahoo Finance API**

### 🗄️ Data Lake with **MinIO** for raw & processed storage

### 🔄 Data Transformation with **PySpark** (distributed processing)

### 🛢 Data Warehouse – **PostgreSQL** for analytics-ready storage

### 📊 Visualization – **Metabase** for interactive dashboards

### 🔔 Monitoring – **Slack** alerts on DAG success/failure

### 🐳 Fully Dockerized for easy deployment & networking between services
---

## 🎥 Demo Video  
[![Demo Video](https://img.youtube.com/vi/4JSl5onrIQI/0.jpg)](https://youtu.be/4JSl5onrIQI)
---

## ✨ Pipeline Workflow

### DAG Structure – 6 Tasks

* **API Availability Check** -
Verifies if the Yahoo Finance API is up and responding.

* **Fetch Stock Prices** -
Retrieves the latest NVIDIA (NVDA) stock price data from Yahoo Finance API.

* **Store Raw Data in MinIO** -Saves raw JSON data to MinIO object storage.
(What is MinIO? → A high-performance, open-source, S3-compatible object storage system.)

* **Format Prices with PySpark** -Cleans & transforms stock data into structured format (CSV).(What is PySpark? → Python API for Apache Spark, a distributed computing framework.)

* **Stores transformed CSV back into MinIO** -

* **Load into PostgreSQL** -Moves processed data into PostgreSQL (data warehouse) for analytics.

* **Visualize in Metabase** -Displays trends & insights in an interactive dashboard.

**Metabase directly queries PostgreSQL for real-time analytics.**

---

## 🏗️ System Architecture

![alt text](<arch.png>)







