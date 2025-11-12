from kafka import KafkaConsumer
import json
import json
import cv2
import numpy as np
import base64

# Create a consumer
consumer = KafkaConsumer(
    "video_frames",                # topic name used by producer
    bootstrap_servers="kafka:9092",  # matches your service name and port
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",  # start from beginning if no offset
    enable_auto_commit=True,       # commit offsets automatically
    group_id="video-consumer-group" # group id so Kafka tracks position
)

print("Connected to Kafka. Listening for messages")
print("Connected: " + str(consumer.bootstrap_connected()))
print("Assignment: " + str(consumer.assignment()))
print("Subscription: " + str(consumer.subscription()))

for message in consumer:
    try:
        encoded_frame = message.value["frame_data"] #encoded frame data

        frame_bytes = base64.b64decode(encoded_frame)  # decoding using base64
        nparr = np.frombuffer(frame_bytes, np.uint8)   # converting to numpyArray for cv2
        frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)  # final Frame formilization

        print(nparr.size)

    except Exception as e:
        print(f"Error decoding frame: {e}")

    print(f"Received frame data: {message.topic}, {message.partition}, {message.offset}")
