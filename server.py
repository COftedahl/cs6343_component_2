import requests
import os
from dotenv import load_dotenv
import numpy as np
from flask import Flask, request, jsonify
import cv2
import matplotlib.pyplot as plt
from flask_cors import CORS, cross_origin
from confluent_kafka import Consumer, KafkaError
from datetime import datetime

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
def process_image():
# def upload_image():
  # Check if the request contains a file
  if 'image' not in request.files:
    return jsonify({'error': 'No image file provided'}), 400

  file = request.files['image']

  # Check if the file is valid
  if file.filename == '':
    return jsonify({'error': 'No selected file'}), 400

  if file and allowed_file(file.filename):
    # read file data
    file_bytes = np.frombuffer(file.read(), np.uint8)
    img_data = cv2.imdecode(file_bytes, cv2.IMREAD_COLOR)

    # perform image processing
    height, width, _ = img_data.shape
    starting_size =  (height, width)
    max_starting_dim = max(starting_size[0], starting_size[1])
    img_resized_256 = cv2.resize(img_data, (256,256), interpolation=cv2.INTER_AREA if (256 < max_starting_dim) else cv2.INTER_CUBIC)
    img_resized_720 = cv2.resize(img_data, (1280,720), interpolation=cv2.INTER_AREA if (1280 < max_starting_dim) else cv2.INTER_CUBIC)
    img_resized_1080 = cv2.resize(img_data, (1920,1080), interpolation=cv2.INTER_AREA if (1920 < max_starting_dim) else cv2.INTER_CUBIC)
    img_resized_1440 = cv2.resize(img_data, (2560,1440), interpolation=cv2.INTER_AREA if (2560 < max_starting_dim) else cv2.INTER_CUBIC)
    
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
    param_dict = {
      256: img_resized_256, 
      720: img_resized_720, 
      1080: img_resized_1080, 
      1440: img_resized_1440, 
    }
    if (url is not None):
      response = requests.post(url, data=param_dict)

    # Return success response
    return jsonify({'message': 'Image uploaded successfully'}), 200

  return jsonify({'error': 'Invalid file type'}), 400

def getDatetimeString(): 
  return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def pollKafkaIndefinitely(consumer): 
  while True: 
    print("[" + getDatetimeString() + "] Polling kafka")
    msg = consumer.poll(int(os.getenv("KAFKA_POLL_FREQUENCY") if os.getenv("KAFKA_POLL_FREQUENCY") is not None else "10"))
    if msg is None:
      continue
    if msg.error() is None:
      # parse the message here
      print("[" + getDatetimeString() + "] Processing Image(s)")
      process_image(msg.value())

def getEnvs(): 
  return {
    "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC"), 
    "KAFKA_SERVER_URL": os.getenv("KAFKA_SERVER_URL"), 
    "KAFKA_POLL_FREQUENCY": os.getenv("KAFKA_POLL_FREQUENCY"), 
    "NEXT_PROCESSING_STAGE_URL": os.getenv("NEXT_PROCESSING_STAGE_URL")
  }

def getConfig(): 
  return {
  'bootstrap.servers': (os.getenv("KAFKA_SERVER_URL") if os.getenv("KAFKA_SERVER_URL") is not None else "abc"),
  'group.id': 'mygroup',
  'auto.offset.reset': 'earliest'
}

def initConsumer(config): 
  c = Consumer(config)
  c.subscribe([os.getenv("KAFKA_TOPIC") if os.getenv("KAFKA_TOPIC") is not None else "my_topic"], on_assign=printConnectionStatus)
  return c

if __name__ == '__main__':
  load_dotenv()
  print(getEnvs())
  config = getConfig()
  consumer = initConsumer(config)
  # app.run(port=8000, debug=True)
  pollKafkaIndefinitely(consumer)