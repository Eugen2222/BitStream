# Bitcoin Data Platform

End-to-end real-time Bitcoin analytics project:

- CoinGecko API -> Python Producer -> Kafka
- Kafka -> Spark Structured Streaming -> PostgreSQL
- Streamlit dashboard for price, latency, and 1-minute OHLC
- Airflow DAG placeholder for future orchestration

## Project Structure

```text
bitcoin-data-platform/
├── docker-compose.yml
├── README.md
├── architecture/
│   └── pipeline_architecture.md
├── producer/
│   └── btc_producer.py
├── spark/
│   ├── streaming/
│   │   └── spark_stream_v2.py
│   └── batch/
│       └── analytics_job.py
├── database/
│   ├── init.sql
│   └── schema.sql
├── dashboard/
│   └── dashboard_v2.py
├── airflow/
│   └── dags/
│       └── btc_pipeline_dag.py
├── sql/
│   └── queries.sql
└── lakehouse/
    └── README.md
```

## Run

1. Start containers

```bash
docker-compose up -d
```

2. Install Python packages

```bash
pip install requests kafka-python streamlit pandas sqlalchemy psycopg2-binary pyspark==3.5.1
```

3. Start producer

```bash
python producer/btc_producer.py
```

4. Start Spark streaming job

Windows:

```bash
spark-submit ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 ^
  spark/streaming/spark_stream_v2.py
```

macOS / Linux:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
  spark/streaming/spark_stream_v2.py
```

5. Start dashboard

```bash
streamlit run dashboard/dashboard_v2.py
```

## Notes

- This version intentionally does **not** use Delta Lake.
- It focuses on a stable streaming-to-database pipeline for portfolio/demo purposes.
- If your Spark version is not 3.5.1, adjust the Kafka connector package version to match.
