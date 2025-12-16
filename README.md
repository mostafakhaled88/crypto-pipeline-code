# 🚀 Medallion Architecture Cryptocurrency ETL Pipeline

A robust, idempotent data pipeline orchestrated by **Apache Airflow** that extracts raw cryptocurrency price data, enforces data quality via a Silver layer, and delivers curated business metrics to a Gold layer using **PostgreSQL**.

---

## 🎯 Project Goal

This project demonstrates proficiency in building end-to-end data pipelines following the **Medallion Architecture** (Bronze, Silver, Gold).

**Key objectives:**

1. **Orchestration & Scheduling** – Use Apache Airflow to manage dependencies and schedule daily runs.
2. **Data Quality & Immutability** – Preserve raw data in the Bronze layer and enforce structure and quality in the Silver layer.
3. **Idempotency** – Ensure safe reruns using PostgreSQL `ON CONFLICT` logic to prevent duplicates.
4. **Curated Metrics** – Produce business-ready aggregates such as daily averages and percentage price change.

---

## 💡 Technical Stack & Architecture

| Component        | Technology                    | Purpose                                       |
| ---------------- | ----------------------------- | --------------------------------------------- |
| Orchestration    | Apache Airflow (2.x)          | Scheduling, dependency management, monitoring |
| Database         | PostgreSQL                    | Metadata store + unified data warehouse       |
| Data Source      | External REST API (CoinGecko) | Cryptocurrency price ingestion                |
| Containerization | Docker & Docker Compose       | Reproducible local environment                |
| Languages        | Python, SQL, YAML             | ETL logic, modeling, orchestration            |

---

## 🧱 Medallion Architecture Flow

The pipeline follows the Medallion Architecture pattern:

| Layer         | Input        | Output                             | Description                         |
| ------------- | ------------ | ---------------------------------- | ----------------------------------- |
| 🥉 **Bronze** | External API | `bronze_raw_prices` (JSONB)        | Immutable raw data ingestion        |
| 🥈 **Silver** | Bronze       | `silver_clean_prices` (Relational) | Cleaned, flattened, normalized data |
| 🥇 **Gold**   | Silver       | `gold_daily_metrics` (Aggregated)  | Analytics-ready KPIs                |

---

## 🛠️ Data Engineering Skills Demonstrated

* **Idempotent Pipelines** using `ON CONFLICT`
* **Data Modeling** from nested JSON to normalized tables
* **Advanced SQL** (CTEs, aggregations, joins, time-series calculations)
* **Airflow DAG Design** using modern `@task` API
* **Dockerized Infrastructure** for local deployment
* **Separation of Concerns** via layered ETL design

---

## ⚙️ Quick Start Guide

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/mostafakhaled88/crypto-pipeline-code.git
cd crypto-pipeline-code
```

---

### 2️⃣ Configure Environment

* Ensure `docker-compose.yml` is present
* Update `config/api_config.json` with desired coins and API settings

---

### 3️⃣ Build & Start Services

```bash
docker compose up --build -d
```

This will start:

* PostgreSQL
* Airflow Webserver
* Airflow Scheduler

---

### 4️⃣ Access Airflow UI

* **URL:** [http://localhost:8081](http://localhost:8081)
* **Username:** admin
* **Password:** admin

---

### 5️⃣ Run the Pipeline

1. Locate `medallion_crypto_pipeline` DAG
2. Toggle DAG **ON**
3. Trigger a manual run

---

## 📁 Repository Structure

```text
crypto-pipeline-code/
├── .gitignore
├── docker-compose.yml
├── config/
│   └── api_config.json
├── dags/
│   ├── medallion_crypto_dag.py
│   └── medallion/
│       ├── bronze_extract.py
│       ├── silver_transform.py
│       └── gold_curation.py
└── README.md
```

---

## ✅ Project Status

✔ Fully Dockerized
✔ Idempotent ETL Pipeline
✔ Production-style Medallion Architecture
✔ Portfolio-ready Data Engineering project

---

## 👤 Author

**Mostafa Khaled Farag**
Junior Data Analyst / Data Engineer
📍 Cairo, Egypt
🔗 GitHub: [https://github.com/mostafakhaled88](https://github.com/mostafakhaled88)

---

⭐ If you find this project useful, feel free to star the repository!
