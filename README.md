
# 🔥 Kafka-Based ETL Pipeline for Material Demand & Capacity

A scalable, schema-driven data pipeline built with **FastAPI**, **Kafka**, and **Apache Spark** to validate, store, and forward supply chain data to external APIs.

---

## 📦 Project Structure

```
project/
├── api/
│   └── schema/
│       ├── IdBasedComment-schema.json
│       ├── WeekBasedMaterialDemand-schema.json
│       └── WeekBasedCapacityGroup-schema.json
├── transforms/
│   ├── transform_comment.py
│   ├── transform_demand.py
│   └── transform_capacity.py
├── logs/
├── raw_data/
│   ├── comments-topic/
│   ├── demand-topic/
│   └── capacity-topic/
├── main.py                # FastAPI server
├── consumer.py            # Kafka raw message archiver
├── spark_processor.py     # Spark Kafka stream processor
├── config.py              # Settings and paths
└── requirements.txt
```

---

## ⚙️ Tech Stack

- **FastAPI** – JSON schema validation & REST API
- **Apache Kafka** – Message streaming backbone
- **Apache Spark (Structured Streaming)** – Real-time ETL & API forwarding
- **JSONSchema (Draft-07)** – Schema validation
- **Tenacity** – Retry logic for failed API posts
- **Logging** – Traceable logs across all components

---

## 🛠️ Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

Include dependencies like:
```txt
fastapi
uvicorn
jsonschema
kafka-python
pyspark
tenacity
requests
```

### 2. Start Kafka

Ensure Kafka is running at `localhost:9092`.

### 3. Launch FastAPI Server

```bash
uvicorn main:app --reload
```

### 4. Start Kafka Consumer

This stores raw validated messages to disk.

```bash
python consumer.py
```

### 5. Start Spark Processor

```bash
python spark_processor.py
```

---

## 🌐 API Endpoints

| Endpoint     | Description                         | Kafka Topic      |
|--------------|-------------------------------------|------------------|
| `POST /comments` | Accepts user comment payloads       | `comments-topic` |
| `POST /demand`   | Accepts week-based material demand | `demand-topic`   |
| `POST /capacity` | Accepts weekly capacity info       | `capacity-topic` |

Each endpoint validates payloads using its schema in `/api/schema`.

---

## 📤 Output Data

### 🧾 Raw JSON Archive
- Saved in `raw_data/<topic-name>/`
- Filename pattern: `YYYYMMDDTHHMMSS_UUID.json`

### 🔁 External API Posting

After transformation, payloads are posted to:

| Kafka Topic      | Target API URL                                  |
|------------------|--------------------------------------------------|
| `comments-topic` | `https://myo9poc.o9solutions.com/api/pulse`      |
| `demand-topic`   | `https://myo9poc.o9solutions.com/api/demand`     |
| `capacity-topic` | `https://myo9poc.o9solutions.com/api/capacity`   |

Includes:
- Chunked posting (`100` records per batch)
- Retry logic (5 attempts, exponential backoff)

---

## 🧪 Testing & Debugging

- Use Postman or `curl` to hit API endpoints with valid/invalid payloads.
- Check:
  - FastAPI logs for validation errors
  - Consumer logs for file save issues
  - Spark logs for transformation/posting problems

---

## 🧰 Dev Tips

- ✅ Use `.trigger(once=True)` in Spark for batch testing
- 🚧 Add more schema files under `api/schema/` for future endpoints
- 🔒 Secure API keys using `.env` or `AWS Secrets Manager`

---

## 🔐 Security Note

Hardcoded API keys (`API_KEY = "..."`) should be **moved to secure storage** before production deployment.

---

## 👨‍🔧 Contributors

- Backend + ETL Orchestration: You
- API Design & Schema Validation: Also You
- Sanity Preserved by: Coffee

---

## 📜 License

MIT – do what you want, but don't blame us if it breaks.

---
# DCMKit
