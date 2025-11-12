from kafka import KafkaConsumer
import json

# Create a consumer
consumer = KafkaConsumer(
    "video_frames",                # topic name used by producer
    bootstrap_servers="kafka:9092",  # matches your service name and port
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",  # start from beginning if no offset
    enable_auto_commit=True,       # commit offsets automatically
    group_id="video-consumer-group" # group id so Kafka tracks position
)

print("Connected to Kafka. Listening for messages...")

for message in consumer:
    print(f"Received frame data: {message.topic}, {message.partition}, {message.offset}")
