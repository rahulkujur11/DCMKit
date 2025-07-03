import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import traceback
import logging
import requests
import json
from datetime import date, datetime
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from config import settings
from transforms.transform_comment import transform_comment
from transforms.transform_demand import transform_demand
from transforms.transform_capacity import transform_capacity


# === CONFIG ===
API_URLS = {
    "comments-topic": "https://webhook.site/d49a10bb-46f9-41a9-808b-83f957d27e37",
    "demand-topic": "https://webhook.site/d49a10bb-46f9-41a9-808b-83f957d27e37",
    "capacity-topic": "https://webhook.site/d49a10bb-46f9-41a9-808b-83f957d27e37",
}
API_KEY = "lxt174f2.mu24jr9zgmj1adoduw3eow2"
CHUNK_SIZE = 100


# === SPARK SESSION ===
spark = SparkSession.builder \
    .appName("KafkaToAPIWithCustomTransform") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()


# === LOGGING ===
os.makedirs(settings.LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(settings.LOG_DIR, "spark_stream.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)
logger.info("=== Spark Session started ===")


# === KAFKA STREAM READ ===
logger.info("Initializing Kafka stream read")
raw_kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "comments-topic,demand-topic,capacity-topic") \
    .load()
logger.info("Kafka stream initialized and subscribed to 'comments-topic'")


# === SANITIZER ===
def sanitize(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize(v) for v in obj]
    else:
        return obj


# === PARSE FUNCTION ===
def parse_batch(kafka_df):
    logger.info("Starting to parse Kafka batch")
    kafka_rdd = kafka_df.selectExpr("CAST(value AS STRING)", "topic").rdd.map(lambda row: (row["topic"], row["value"]))
    parsed = kafka_rdd.map(lambda tup: (tup[0], json.loads(tup[1])))
    return parsed.collect()


# === TRANSFORM FUNCTION ===
def transform_data(input_data, topic):
    transformers = {
        "comments-topic": transform_comment,
        "demand-topic": transform_demand,
        "capacity-topic": transform_capacity,
    }
    transformer = transformers.get(topic)
    if not transformer:
        raise ValueError(f"No transformer defined for topic: {topic}")
    return transformer(input_data)


# === CHUNK UTILITY ===
def chunked(iterable, chunk_size=CHUNK_SIZE):
    logger.info(f"Chunking payloads into size {chunk_size}")
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i:i + chunk_size]


# === RETRYING API CALL ===
@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5)
)
def post_data_to_api(data_batch, topic):

    # headers = {
    #     "Content-Type": "application/json",
    #     "Authorization": f"APIKey {API_KEY}"
    # }

    url = API_URLS.get(topic)
    if not url:
        raise ValueError(f"No API URL configured for topic: {topic}")

    try:
        if isinstance(data_batch, list) and len(data_batch) == 1:
            data_batch = data_batch[0]

        safe_payload = sanitize(data_batch)
        logger.debug(f"[{topic}] Sending payload to API:\n%s", json.dumps(safe_payload, indent=2))

        # response = requests.post(url, json=safe_payload, headers=headers)
        response = requests.post(url, json=safe_payload)

        logger.debug(f"[{topic}] API response: {response.status_code} - {response.text}")

        if response.status_code != 200:
            raise Exception(f"[{topic}] Bad response: {response.status_code} - {response.text}")

        logger.info(f"[{topic}] Batch successfully sent to API.")
    except Exception as e:
        logger.error(f"[{topic}] Error during API post: {e}")
        logger.error(traceback.format_exc())
        raise


# === PAYLOAD SENDER ===
def send_payload_chunks(payloads, topic):
    logger.info(f"[{topic}] Preparing to send {len(payloads)} payloads in chunks")
    try:
        for idx, chunk in enumerate(chunked(payloads)):
            logger.info(f"[{topic}] Sending chunk {idx + 1}")
            logger.info(chunk)
            post_data_to_api(chunk, topic)
    except Exception as e:
        logger.error(f"[{topic}] Failed to send payloads: {e}")


# === BATCH PROCESSING FUNCTION ===
def process_multiline_batch(df, batch_id):
    logger.info(f"Processing batch {batch_id}")
    try:
        topic_payloads = parse_batch(df)
        by_topic = {"comments-topic": [], "demand-topic": [], "capacity-topic": []}

        for topic, record in topic_payloads:
            by_topic[topic].append(record)

        # Process each topic separately
        for topic, records in by_topic.items():
            if not records:
                continue

            logger.info(f"Transforming and sending {len(records)} from {topic}")
            transformed = []
            for row in records:
                try:
                    transformed.append(transform_data(row, topic))
                except Exception as e:
                    logger.error(f"[{topic}] Transform error: {e} - {row}")

            send_payload_chunks(transformed, topic)

    except Exception as e:
        logger.error(f"Batch processing error: {e}")
        logger.error(traceback.format_exc())


# === START STREAMING ===
logger.info("Starting the streaming query...")
query = raw_kafka_df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_multiline_batch) \
    .option("checkpointLocation", "checkpoint_dir") \
    .start()

logger.info("Streaming query started and running")
query.awaitTermination()