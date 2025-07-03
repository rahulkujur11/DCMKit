import os
import traceback
import logging
import json
from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

from config import settings
from transforms.bronze import transform_demand_bronze, transform_capacity_bronze, transform_comment_bronze
from transforms.silver import transform_demand_silver, transform_capacity_silver, transform_comment_silver
from transforms.gold import transform_demand_gold, transform_capacity_gold, transform_comment_gold

# === TOPIC CONFIG ===
TOPIC_TO_FOLDER = {
    "comments-topic": "comments",
    "demand-topic": "demand",
    "capacity-topic": "capacity"
}

BRONZE_TRANSFORMERS = {
    "capacity-topic": transform_capacity_bronze,
    "demand-topic": transform_demand_bronze,
    "comments-topic": transform_comment_bronze,
}

SILVER_TRANSFORMERS = {
    "capacity-topic": transform_capacity_silver,
    "demand-topic": transform_demand_silver,
    "comments-topic": transform_comment_silver,
}

GOLD_TRANSFORMERS = {
    "capacity-topic": transform_capacity_gold,
    "demand-topic": transform_demand_gold,
    "comments-topic": transform_comment_gold,
}

BRONZE_PATH = os.path.join(os.getcwd(), "datalake", "bronze")
SILVER_PATH = os.path.join(os.getcwd(), "datalake", "silver")
GOLD_PATH = os.path.join(os.getcwd(), "datalake", "gold")

# === LOGGING ===
os.makedirs(settings.LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(settings.LOG_DIR, "spark_stream.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# === SPARK SESSION ===
spark = SparkSession.builder \
    .appName("KafkaToDeltaPipeline") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# === UTILITIES ===
def parse_batch(kafka_df):
    kafka_rdd = kafka_df.selectExpr("CAST(value AS STRING)", "topic").rdd
    return kafka_rdd.map(lambda row: (row["topic"], json.loads(row["value"]))).collect()

def write_layer(data, path, config=None):
    """
    Universal Delta writer for both demand and capacity data.
    
    Assumes data is already flattened and includes:
    - 'materialDemandId' + 'pointInTime' for demand
    - 'capacityGroupId' + 'pointInTime' for capacity
    
    Args:
        data (list[dict]): Flattened data records
        path (str): Destination path in Delta Lake
        config (dict): Optional config with 'partitions' key
    """

    if not data:
        raise ValueError("No data provided to write_layer.")

    logger.info("write_layer: Starting write operation...")

    # Infer data type from keys
    first = data[0]
    if "materialDemandId" in first and "pointInTime" in first:
        logger.info("Detected demand data.")
        partitions = config.get("partitions", ["materialDemandId", "pointInTime"]) if config else ["materialDemandId", "pointInTime"]
    elif "capacityGroupId" in first and "pointInTime" in first:
        logger.info("Detected capacity data.")
        partitions = config.get("partitions", ["capacityGroupId", "pointInTime"]) if config else ["capacityGroupId", "pointInTime"]
    else:
        logger.error("Unrecognized data structure.")
        raise ValueError("Data must contain either 'materialDemandId' or 'capacityGroupId' and 'pointInTime'.")

    # Create DataFrame and format date
    df = spark.createDataFrame(data)
    df = df.withColumn("pointInTime", to_date("pointInTime", "yyyy-MM-dd"))

    _validate_columns(df, partitions)

    logger.info(f"Writing data with partitions: {partitions}")
    _write_delta(df, path, partitions)
    logger.info("✅ Data write completed.")


def _validate_columns(df, columns):
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required partition columns in DataFrame: {missing}")


def _write_delta(df, path, partitions=None):
    writer = df.repartition(*partitions).write if partitions else df.write
    writer = writer.format("delta").option("mergeSchema", "true")
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.mode("overwrite").save(path)


def process_layer(input_path, transform_fn, output_path):
    try:
        df = spark.read.format("delta").load(input_path)
        rows = [transform_fn(r.asDict()) for r in df.toLocalIterator() if r]
        write_layer(rows, output_path)
        return True
    except Exception as e:
        logger.error(f"[ERROR] Failed to process layer {output_path}: {e}")
        logger.debug(traceback.format_exc())
        return False


def _auto_detect_partitions(df):
    # Common fields to partition on
    possible_partitions = {"PointInTime", "ChangedAt", "CustomerBPNL"}

    # Only include if DataFrame has >1 column
    df_cols = set(df.columns)
    candidate_partitions = list(df_cols.intersection(possible_partitions))

    # If all columns would be used as partitions, skip
    if len(candidate_partitions) == len(df_cols):
        logger.warning(f"⚠️ Skipping partitioning for table with only partition columns: {df_cols}")
        return []

    return candidate_partitions




def process_gold(topic):
    try:
        silver_path = os.path.join(SILVER_PATH, TOPIC_TO_FOLDER[topic])
        gold_base_path = os.path.join(GOLD_PATH, TOPIC_TO_FOLDER[topic])

        logger.info(f"✨ Reading Silver layer for {topic}")
        df = spark.read.format("delta").load(silver_path)
        records = [r.asDict() for r in df.toLocalIterator()]

        # Gold transform returns dict of table_name -> DataFrame
        gold_outputs = GOLD_TRANSFORMERS[topic](records)
        if not isinstance(gold_outputs, dict):
            raise TypeError(f"GOLD_TRANSFORMER for {topic} must return dict(table_name -> DataFrame)")

        for table_name, gold_df in gold_outputs.items():
            output_path = os.path.join(gold_base_path, table_name)
            partitions = _auto_detect_partitions(gold_df)

            logger.info(f"💾 Writing Gold table: {table_name} | Partitions: {partitions}")
            _write_delta(gold_df, output_path, partitions)
            logger.info(f"✅ Gold table {table_name} written to {output_path}")

            logger.info("\n" + gold_df._jdf.showString(20, 20, False))


    except Exception as e:
        logger.error(f"[ERROR] Failed in gold layer for topic {topic}: {e}")
        logger.debug(traceback.format_exc())



def process_multiline_batch(df, batch_id):
    logger.info(f"Processing batch {batch_id}")
    try:
        topic_payloads = parse_batch(df)
        by_topic = {topic: [] for topic in TOPIC_TO_FOLDER}
        for topic, record in topic_payloads:
            by_topic[topic].append(record)

        for topic, records in by_topic.items():
            if not records:
                continue
            try:
                bronze_data = []
                transform_fn = BRONZE_TRANSFORMERS.get(topic)
                for r in records:
                    try:
                        result = transform_fn(r)
                        if isinstance(result, list):
                            bronze_data.extend(result)
                        elif isinstance(result, dict):
                            bronze_data.append(result)
                    except Exception as e:
                        logger.error(f"[ERROR] Bronze transform error for topic {topic}: {e}")
                        logger.debug(traceback.format_exc())

                bronze_path = os.path.join(BRONZE_PATH, TOPIC_TO_FOLDER[topic])
                write_layer(bronze_data, bronze_path)
                logger.info(f"Bronze written for {topic}")

                silver_path = os.path.join(SILVER_PATH, TOPIC_TO_FOLDER[topic])
                if not process_layer(bronze_path, SILVER_TRANSFORMERS[topic], silver_path):
                    continue
                logger.info(f"Silver written for {topic}")

                # 🆕 Gold step now supports multiple outputs
                process_gold(topic)

            except Exception as e:
                logger.error(f"[ERROR] Processing failed for topic {topic}: {e}")
                logger.debug(traceback.format_exc())

    except Exception as e:
        logger.error(f"[ERROR] Batch-level error: {e}")
        logger.debug(traceback.format_exc())

# === DIRECTORY BOOTSTRAP ===
for layer_path in [BRONZE_PATH, SILVER_PATH, GOLD_PATH]:
    for folder in TOPIC_TO_FOLDER.values():
        os.makedirs(os.path.join(layer_path, folder), exist_ok=True)


# === KAFKA STREAM ===
raw_kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", ",".join(TOPIC_TO_FOLDER.keys())) \
    .load()

query = raw_kafka_df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_multiline_batch) \
    .option("checkpointLocation", "checkpoint_dir") \
    .start()

query.awaitTermination()





