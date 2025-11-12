# https://docs.confluent.io/kafka-clients/python/current/overview.html

kafkaConfig = {
    'bootstrap.servers': 'localhost:8000',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
}

kafkaTopic = "video_streamer"

kafkaPollFrequency = 10.0