import os, json, uuid, logging
from datetime import datetime
from kafka import KafkaConsumer
from config import settings

# Setup directories
os.makedirs(settings.LOG_DIR, exist_ok=True)
os.makedirs(settings.RAW_DATA_DIR, exist_ok=True)

# Logging config
logging.basicConfig(
    filename=os.path.join(settings.LOG_DIR, "consumer.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Topics to subscribe
TOPICS = ["comments-topic", "demand-topic", "capacity-topic"]

# Start Kafka Consumer
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    group_id=settings.GROUP_ID,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

logger.info(f"Consumer started. Subscribed to topics: {', '.join(TOPICS)}")

# Consume messages by topic
for message in consumer:
    topic = message.topic
    data = message.value

    # Base folder per topic
    topic_dir = os.path.join(settings.RAW_DATA_DIR, topic)
    os.makedirs(topic_dir, exist_ok=True)

    # Generate unique filename with timestamp
    filename = f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex}.json"
    filepath = os.path.join(topic_dir, filename)

    # Save the message
    try:
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"[{topic}] Message saved to {filepath}")
    except Exception as e:
        logger.error(f"[{topic}] Failed to save message: {e}", exc_info=True)
