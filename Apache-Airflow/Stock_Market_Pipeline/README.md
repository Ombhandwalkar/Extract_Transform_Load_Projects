# 📈 Stock Market Data Pipeline

> An end-to-end automated data engineering pipeline that orchestrates real-time stock market data ingestion, transformation, and visualization using Apache Airflow, PySpark, and modern data stack technologies.

[![Demo Video](https://img.youtube.com/vi/4JSl5onrIQI/0.jpg)](https://youtu.be/4JSl5onrIQI)

---

## 🎯 Project Overview

This project demonstrates a production-grade data pipeline that:
- Fetches real-time stock market data from Yahoo Finance API
- Stores raw data in a scalable object storage layer (MinIO)
- Transforms data using distributed computing (PySpark)
- Loads processed data into a PostgreSQL data warehouse
- Visualizes insights through interactive Metabase dashboards
- Monitors pipeline health with Slack notifications

**Current Focus**: NVIDIA (NVDA) stock price analysis with daily updates

---

## 🏗️ Architecture

![alt text](<arch.png>)

```
Yahoo Finance API → Airflow DAG → MinIO (Data Lake) → Spark Transformation 
                                                              ↓
Slack Notifications ← PostgreSQL (Data Warehouse) ← Formatted CSV
                                  ↓
                            Metabase Dashboard
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow | Workflow automation & scheduling |
| **Data Lake** | MinIO | S3-compatible object storage |
| **Processing** | Apache Spark (PySpark) | Distributed data transformation |
| **Data Warehouse** | PostgreSQL | Analytics-ready storage |
| **Visualization** | Metabase | Interactive dashboards |
| **Monitoring** | Slack | Real-time alerts |
| **Containerization** | Docker & Docker Compose | Service orchestration |
| **Runtime** | Astro CLI | Airflow development environment |

---

## 🔄 Pipeline Workflow

The DAG executes 6 sequential tasks daily:

### 1️⃣ **API Availability Check** (`is_api_available`)
- **Type**: Sensor Task
- **Function**: Validates Yahoo Finance API connectivity
- **Configuration**: 30s poke interval, 5min timeout
- **Output**: Returns API endpoint URL via XCom

### 2️⃣ **Fetch Stock Prices** (`get_stock_prices`)
- **Type**: Python Operator
- **Function**: Retrieves NVDA stock data (1-year historical, 1-day interval)
- **Data Retrieved**: Open, High, Low, Close, Volume, Timestamp
- **Output**: Raw JSON response stored in XCom

### 3️⃣ **Store Raw Data** (`store_prices`)
- **Type**: Python Operator
- **Function**: Persists raw JSON to MinIO bucket
- **Storage Pattern**: `stock-market/{SYMBOL}/prices.json`
- **Output**: Bucket path for downstream tasks

### 4️⃣ **Format Prices** (`format_prices`)
- **Type**: Docker Operator
- **Function**: Executes PySpark job in isolated container
- **Transformations**:
  - Explodes nested JSON arrays
  - Flattens quote indicators (price/volume)
  - Converts Unix timestamps to dates
  - Outputs structured CSV
- **Storage**: `stock-market/{SYMBOL}/formatted_prices/*.csv`

### 5️⃣ **Retrieve Formatted CSV** (`get_formatted_csv`)
- **Type**: Python Operator
- **Function**: Locates processed CSV file in MinIO
- **Output**: CSV object path for loading

### 6️⃣ **Load to Data Warehouse** (`load_to_dw`)
- **Type**: Python Operator
- **Function**: Bulk inserts data into PostgreSQL
- **Table**: `public.stock_market`
- **Method**: Pandas → SQLAlchemy bulk insert

---

## 🚀 Getting Started

### Prerequisites

- **Docker Desktop**: 8GB+ RAM allocated
- **Astro CLI**: [Installation Guide](https://docs.astronomer.io/astro/cli/install-cli)
- **Git**: For cloning the repository

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd Stock_Market_Pipeline
```

2. **Build Docker images**
```bash
# Build Spark application image
cd spark/notebooks/stock_transform
docker build -t airflow/stock-app .
cd ../../..

# Build Spark master/worker
docker build -t airflow/spark-master ./spark/master
docker build -t airflow/spark-worker ./spark/worker
```

3. **Start Airflow environment**
```bash
astro dev start
```

This command will spin up:
- Airflow Webserver (http://localhost:8080)
- Airflow Scheduler
- PostgreSQL Database
- Triggerer
- MinIO (http://localhost:9001)
- Spark Master (http://localhost:8082)
- Spark Worker
- Metabase (http://localhost:3000)

### Configuration

#### Airflow Connections

Navigate to **Admin → Connections** in Airflow UI and configure:

**1. MinIO Connection (`minio`)**
```json
Connection Type: Amazon Web Services
Extra: {
  "aws_access_key_id": "minio",
  "aws_secret_access_key": "minio123",
  "endpoint_url": "http://minio:9000"
}
```

**2. Stock API Connection (`stock_api`)**
```json
Connection Type: HTTP
Host: https://query1.finance.yahoo.com
Extra: {
  "endpoint": "/v8/finance/chart/",
  "headers": {
    "User-Agent": "Mozilla/5.0"
  }
}
```

**3. PostgreSQL Connection (`postgres`)**
```
Connection Type: Postgres
Host: postgres
Schema: postgres
Login: postgres
Password: postgres
Port: 5432
```

**4. Slack Connection (`slack`)** *(Optional)*
```json
Connection Type: Slack Webhook
Password: <your-slack-webhook-url>
```

---

## 📊 Accessing Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:8080 | admin / admin |
| **MinIO Console** | http://localhost:9001 | minio / minio123 |
| **Spark Master UI** | http://localhost:8082 | N/A |
| **Metabase** | http://localhost:3000 | Setup on first visit |
| **PostgreSQL** | localhost:5432 | postgres / postgres |

---

## 🔧 Project Structure

```
Stock_Market_Pipeline/
├── dags/
│   └── stock_market.py              # Main DAG definition
├── include/
│   ├── stock_market/
│   │   └── tasks.py                 # Task implementations
│   ├── helpers/
│   │   └── minio.py                 # MinIO helper functions
│   └── data/
│       ├── minio/                   # MinIO persistent storage
│       └── metabase/                # Metabase data
├── spark/
│   ├── master/                      # Spark master configuration
│   ├── worker/                      # Spark worker configuration
│   └── notebooks/stock_transform/
│       └── stock_transform.py       # PySpark transformation logic
├── tests/
│   └── dags/
│       └── test_dag_example.py      # DAG validation tests
├── docker-compose.override.yml      # Additional services
├── Dockerfile                       # Airflow runtime image
└── requirements.txt                 # Python dependencies
```

---

## 🛠️ Key Features

### Data Transformation with PySpark

The transformation process (`stock_transform.py`):
```python
# 1. Explode nested JSON structure
df_exploded = df.select("timestamp", explode("indicators.quote").alias("quote"))

# 2. Zip arrays for structured data
df_zipped = df_exploded.select(
    arrays_zip("timestamp", "close", "high", "low", "open", "volume")
)

# 3. Convert Unix timestamps to dates
df_final = df_zipped.withColumn('date', from_unixtime('timestamp').cast(DateType()))
```

### S3-Compatible Object Storage

MinIO configuration enables:
- Versioned data lake storage
- S3 API compatibility for Spark integration
- Local development without cloud costs
- Separation of raw and processed data

### Monitoring & Alerting

Slack notifications trigger on:
- ✅ DAG success
- ❌ DAG failure
- Customizable per-task alerts

---

## 📈 Sample Queries

Once data is loaded into PostgreSQL, analyze with SQL:

```sql
-- Get latest stock prices
SELECT date, close, volume
FROM public.stock_market
ORDER BY date DESC
LIMIT 30;

-- Calculate 7-day moving average
SELECT 
    date,
    close,
    AVG(close) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as ma_7
FROM public.stock_market
ORDER BY date DESC;

-- Identify high-volume trading days
SELECT date, close, volume
FROM public.stock_market
WHERE volume > (SELECT AVG(volume) * 1.5 FROM public.stock_market)
ORDER BY volume DESC;
```

---

## 🧪 Testing

Run DAG validation tests:
```bash
pytest tests/dags/test_dag_example.py -v
```

Tests verify:
- No import errors in DAGs
- All DAGs have required tags
- Retry configuration is set (≥2 retries)

---

## 🐛 Troubleshooting

### Common Issues

**Spark job hangs after "Passing arguments..."**
```bash
# Restart Airflow environment
astro dev kill
astro dev start
```

**MinIO connection refused**
- Verify MinIO container is running: `docker ps | grep minio`
- Check endpoint URL uses container name: `http://minio:9000`

**Insufficient memory for Spark**
- Allocate 8GB+ RAM to Docker Desktop
- Path: Docker Desktop → Settings → Resources → Memory

**CSV file not found**
- Ensure Spark transformation completed successfully
- Check MinIO bucket at `stock-market/NVDA/formatted_prices/`

---

## 📝 Future Enhancements

- [ ] Multi-stock support (parameterized DAG runs)
- [ ] Real-time streaming with Apache Kafka
- [ ] Machine learning price prediction models
- [ ] Data quality validation with Great Expectations
- [ ] Advanced Metabase dashboards (technical indicators, RSI, MACD)
- [ ] Cost optimization with data partitioning
- [ ] CI/CD pipeline with GitHub Actions

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request


