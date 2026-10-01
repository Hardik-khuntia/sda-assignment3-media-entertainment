import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION")

if not MONGODB_URI:
    raise ValueError("MONGODB_URI is missing from .env")

if not MONGODB_DATABASE:
    raise ValueError("MONGODB_DATABASE is missing from .env")

if not MONGODB_COLLECTION:
    raise ValueError("MONGODB_COLLECTION is missing from .env")


# ============================================================
# KAFKA CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

TOPICS = [
    "playback-events",
    "content-metadata",
    "subscription-events",
    "cdn-performance",
    "social-mentions",
]


# ============================================================
# MONGODB ATLAS CONNECTION
# ============================================================

mongo_client = MongoClient(
    MONGODB_URI,
    server_api=ServerApi("1"),
)

database = mongo_client[MONGODB_DATABASE]
collection = database[MONGODB_COLLECTION]

# Verify MongoDB connection
mongo_client.admin.command("ping")


# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id="assignment3-atlas-consumer-v2",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def parse_timestamp(value):
    """
    Convert Kafka timestamp strings into Python datetime objects.
    """
    if not value:
        return datetime.now(timezone.utc)

    try:
        return datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)


def normalize_event(topic, event):
    """
    Convert different Kafka event schemas into one
    MongoDB document structure.
    """

    source_map = {
        "playback-events": "playback",
        "content-metadata": "metadata",
        "subscription-events": "subscription",
        "cdn-performance": "cdn",
        "social-mentions": "social",
    }

    source = source_map.get(topic, topic)

    document = {
        # Common fields
        "event_id": event.get("event_id"),
        "source": source,
        "kafka_topic": topic,
        "timestamp": parse_timestamp(event.get("timestamp")),

        # User / session
        "user_id": event.get("user_id"),
        "session_id": event.get("session_id"),

        # Content
        "title_id": event.get("title_id"),
        "title": event.get("title"),
        "genre": event.get("genre"),
        "language": event.get("language"),
        "content_rating": event.get("content_rating"),
        "licensing_window_end": event.get("licensing_window_end"),
        "update_type": event.get("update_type"),

        # Playback
        "event_type": event.get("event_type"),
        "device": event.get("device"),
        "watch_position_sec": event.get("watch_position_sec"),
        "content_duration_sec": event.get("content_duration_sec"),
        "watch_completion_pct": event.get("watch_completion_pct"),
        "session_quality": event.get("session_quality"),

        # Location
        "region": event.get("region"),

        # CDN / QoS
        "cdn_node": event.get("cdn_node"),
        "latency_ms": event.get("latency_ms"),
        "bitrate_mbps": event.get("bitrate_mbps"),
        "buffering_rate_pct": event.get("buffering_rate_pct"),
        "error_code": event.get("error_code"),

        # Subscription
        "subscription_action": event.get("subscription_action"),
        "plan": event.get("plan"),

        # Social
        "platform": event.get("platform"),
        "text": event.get("text"),
    }

    return document


# ============================================================
# START MESSAGE
# ============================================================

print()
print("=" * 70)
print("SDA ASSIGNMENT 3 - KAFKA TO MONGODB ATLAS CONSUMER")
print("=" * 70)
print()
print("Kafka:       localhost:9092")
print("MongoDB:     MongoDB Atlas")
print(f"Database:    {MONGODB_DATABASE}")
print(f"Collection:  {MONGODB_COLLECTION}")
print()
print("Kafka topics:")
for topic in TOPICS:
    print(f"  - {topic}")

print()
print("MongoDB Atlas connection: SUCCESS")
print("Waiting for Kafka events...")
print("=" * 70)
print()


# ============================================================
# CONSUME EVENTS
# ============================================================

try:
    for message in consumer:

        topic = message.topic
        event = message.value

        document = normalize_event(
            topic,
            event,
        )

        try:
            collection.insert_one(document)

            print(
                f"[ATLAS] Stored | "
                f"source={document['source']} | "
                f"event_id={document['event_id']} | "
                f"topic={topic}"
            )

        except Exception as error:

            print(
                f"[ATLAS ERROR] "
                f"Could not store event "
                f"{document.get('event_id')}: {error}"
            )


except KeyboardInterrupt:

    print()
    print("Consumer stopped by user.")


finally:

    consumer.close()
    mongo_client.close()

    print("Kafka consumer closed.")
    print("MongoDB Atlas connection closed.")