import random
import base64
import requests
import os
from dotenv import load_dotenv
import numpy as np
from flask import Flask, request, jsonify
import cv2
import matplotlib.pyplot as plt
from flask_cors import CORS, cross_origin
from kafka import KafkaConsumer
from datetime import datetime
import json

# from kafkaConfig import kafkaConfig, kafkaTopic, kafkaPollFrequency

# app = Flask(__name__)
# CORS(app)

def printConnectionStatus(consumer, partitions): 
  print("Connected to Kafka Assignment ", partitions)

ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}

# Helper function to check allowed file extensions
def allowed_file(filename):
  return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def display_image(img, windowName=""): 
  if (len(windowName) > 0): 
    plt.figure(num=windowName)
  plt.imshow(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))
  plt.axis('off')
  plt.show()

# @app.route('/upload', methods=['POST'])
# @cross_origin(origin="*")
def process_image(img_data):
# def upload_image():
  # Check if the request contains a file
  # if 'image' not in request.files:
    # return jsonify({'error': 'No image file provided'}), 400

  # file = request.files['image']

  # Check if the file is valid
  # if file.filename == '':
    # return jsonify({'error': 'No selected file'}), 400

  # if file and allowed_file(file.filename):
    # read file data
    # file_bytes = np.frombuffer(file.read(), np.uint8)
    # img_data = cv2.imdecode(img_bytes, cv2.IMREAD_COLOR)

    # perform image processing
    # print("In processing image")
    height, width, _ = img_data.shape
    starting_size =  (height, width)
    # print("In processing image 2")
    max_starting_dim = max(starting_size[0], starting_size[1])
    # print("In processing image 3")

    img_resized_256 = cv2.resize(img_data, (256,256), interpolation=cv2.INTER_AREA if (256 < max_starting_dim) else cv2.INTER_CUBIC)
    img_resized_720 = cv2.resize(img_data, (1280,720), interpolation=cv2.INTER_AREA if (1280 < max_starting_dim) else cv2.INTER_CUBIC)
    img_resized_1080 = cv2.resize(img_data, (1920,1080), interpolation=cv2.INTER_AREA if (1920 < max_starting_dim) else cv2.INTER_CUBIC)
    img_resized_1440 = cv2.resize(img_data, (2560,1440), interpolation=cv2.INTER_AREA if (2560 < max_starting_dim) else cv2.INTER_CUBIC)
    
    # print("In processing image 4")

    # decode image data back to image format
    # _, buffer_256 = cv2.imencode('.png', img_resized_256)
    # _, buffer_720 = cv2.imencode('.png', img_resized_720)
    # _, buffer_1080 = cv2.imencode('.png', img_resized_1080)
    # _, buffer_1440 = cv2.imencode('.png', img_resized_1440)
    # processed_image_bytes_256 = buffer_256.tobytes()
    # processed_image_bytes_720 = buffer_720.tobytes()
    # processed_image_bytes_1080 = buffer_1080.tobytes()
    # processed_image_bytes_1440 = buffer_1440.tobytes()

    # display images - use img_resized, not processed_image_bytes
    # display_image(img_resized_256, "256")
    # display_image(img_resized_720, "720")
    # display_image(img_resized_1080, "1080")
    # display_image(img_resized_1440, "1440")

    # display_image(processed_image_bytes_256)
    # display_image(processed_image_bytes_720)
    # display_image(processed_image_bytes_1080)
    # display_image(processed_image_bytes_1440)

    # send resized images to the next processing stage

    url = os.getenv("NEXT_PROCESSING_STAGE_URL")
    # param_dict = {
    #   256: img_resized_256, 
    #   720: img_resized_720, 
    #   1080: img_resized_1080, 
    #   1440: img_resized_1440, 
    # }

    encoding = '.jpg'

    encoded256 = cv2.imencode(encoding, img_resized_256)
    encoded720 = cv2.imencode(encoding, img_resized_720)
    encoded1080 = cv2.imencode(encoding, img_resized_1080)
    #use entry [1] in the tuple returned from imencode: tuple is of format [successIndicator, buffer]

    # print("Encoding status: " + str(encoded256[0]) + ", " + str(encoded720[0]) + ", " + str(encoded1080[0]))

    # ready_to_send_256 = base64.b64encode(encoded256[1]).decode("utf-8")
    # ready_to_send_720 = base64.b64encode(encoded720[1]).decode("utf-8")
    # ready_to_send_1080 = base64.b64encode(encoded1080[1]).decode("utf-8")
    ready_to_send_256 = encoded256[1]
    ready_to_send_720 = encoded720[1]
    ready_to_send_1080 = encoded1080[1]

    saveMin = 0
    saveChance = int(os.getenv("CHANCE_OF_TEN_TO_SAVE_IMG")) if os.getenv("CHANCE_OF_TEN_TO_SAVE_IMG") is not None else 0
    saveMax = 10
    randVal = random.randint(saveMin, saveMax)
    if (randVal <= saveChance): 
        file_256 = cv2.imwrite("/data/output/" + getDatetimeString() + "256.png", img_resized_256)
        file_720 = cv2.imwrite("/data/output/" + getDatetimeString() + "720.png", img_resized_720)
        file_1080 = cv2.imwrite("/data/output/" + getDatetimeString() + "1080.png", img_resized_1080)
    # print("in processing image 4.5")

    delimiter = '-'
    streamTopic = os.getenv("KAFKA_TOPIC") if os.getenv("KAFKA_TOPIC") is not None else "my_topic"

    files = [
        (streamTopic + delimiter + '256.png', ready_to_send_256),
        (streamTopic + delimiter + '720.png', ready_to_send_720),
        (streamTopic + delimiter + '1080.png', ready_to_send_1080) 
    ]
    if (url is not None):
    #   print("In processing image 5")
      response = requests.post(url, files=files)
    #   print("In processing image 6")
      print("Response from next processing stage: ", response)
    #   print("Response from next processing stage: ", response.json())

    # Return success response
    # return jsonify({'message': 'Image uploaded successfully'}), 200

  # return jsonify({'error': 'Invalid file type'}), 400

def getDatetimeString(): 
  return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def oldPollKafkaIndefinitely(consumer): 
  while True: 
    print("[" + getDatetimeString() + "] Polling kafka")
    # msg = consumer.poll(int(os.getenv("KAFKA_POLL_FREQUENCY") if os.getenv("KAFKA_POLL_FREQUENCY") is not None else "10"))
    # if msg is None:
    #   continue
    # if msg.error() is None:
    #   # parse the message here
    checkForFetchedImages(consumer)
    # print("[" + getDatetimeString() + "] Processing Image(s)")
    # print(str(msg))
    # if (msg != {} and msg.values is not None):
      # process_image(msg.values)

def checkForFetchedImages(consumer): 
  for message in consumer:
    try:
        encoded_frame = message.value["frame_data"] #encoded frame data
        print("[" + getDatetimeString() + "] Received frame data")
        frame_bytes = base64.b64decode(encoded_frame)  # decoding using base64
        # print("Got frame bytes")
        nparr = np.frombuffer(frame_bytes, np.uint8)   # converting to numpyArray for cv2
        # print("Got nparr")
        frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)  # final Frame formilization
        # print("Frame: ", frame)
        # print("Got cv2 frame")
        process_image(frame)

        # print(nparr.size)

    except Exception as e:
        print(f"Error decoding frame: {e}")

    # print(f"Received frame data: {message.topic}, {message.partition}, {message.offset}")
    
def getEnvs(): 
  return {
    "CHANCE_OF_TEN_TO_SAVE_IMG": os.getenv("CHANCE_OF_TEN_TO_SAVE_IMG"), 
    "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC"), 
    "KAFKA_SERVER_URL": os.getenv("KAFKA_SERVER_URL"), 
    "KAFKA_POLL_FREQUENCY": os.getenv("KAFKA_POLL_FREQUENCY"), 
    "NEXT_PROCESSING_STAGE_URL": os.getenv("NEXT_PROCESSING_STAGE_URL"), 
    "GROUP_ID": os.getenv("GROUP_ID"), 
  }

def getConfig(): 
  return {
  'bootstrap.servers': (os.getenv("KAFKA_SERVER_URL") if os.getenv("KAFKA_SERVER_URL") is not None else "abc"),
  'group.id': 'mygroup',
  'auto.offset.reset': 'earliest'
}

def initConsumer(config): 
  # c = Consumer(config)
  # c.subscribe([os.getenv("KAFKA_TOPIC") if os.getenv("KAFKA_TOPIC") is not None else "my_topic"], on_assign=printConnectionStatus)
  # return c

  # Create a consumer
  consumer = KafkaConsumer(
    os.getenv("KAFKA_TOPIC") if os.getenv("KAFKA_TOPIC") is not None else "my_topic",  # topic name used by producer = "video_frames"
    # "video_frames",  # topic name used by producer = "video_frames"
    # bootstrap_servers="kafka:9092",  # matches your service name and port = "kafka:9092"
    bootstrap_servers=config["bootstrap.servers"],  # matches your service name and port = "kafka:9092"
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",  # start from beginning if no offset
    enable_auto_commit=True,       # commit offsets automatically
    group_id=os.getenv("GROUP_ID") if os.getenv("GROUP_ID") is not None else "component-2" # group id so Kafka tracks position
  )
  print("Connected to Kafka. Listening for messages...")
  return consumer

if __name__ == '__main__':
  load_dotenv()
  print(getEnvs())
  config = getConfig()
  consumer = initConsumer(config)
  # app.run(port=8000, debug=True)
#   oldPollKafkaIndefinitely(consumer)
  checkForFetchedImages(consumer)