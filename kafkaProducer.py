from kafkaConfig import kafkaTopic
import cv2
from confluent_kafka import Producer
import socket

conf = {'bootstrap.servers': 'host1:9092,host2:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

def getImage(path): 
  img = cv2.imread(path)
  if img is None:
    raise FileNotFoundError(
      "sample_image.jpg not found. Please upload an image with this name.")
  else: 
    return img

image = getImage("resources/Squirrel_Square_PNG.png")

producer.produce(kafkaTopic, key="key1", value=image[0])
producer.produce(kafkaTopic, key="key2", value=image[1])
producer.produce(kafkaTopic, key="key3", value=image[2])

# print(image)