import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "comments-topic"
RAW_DATA_DIR = os.path.join(BASE_DIR, "raw_kafka_data")
LOG_DIR = os.path.join(BASE_DIR, "logs")
GROUP_ID = "raw-file-dumper"
