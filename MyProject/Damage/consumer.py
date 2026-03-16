import json
from confluent_kafka import Consumer

consumer_config = {
    "bootstrap.servers": "kafka:9092",
    "group.id": "client_damage",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)
consumer.subscribe(["damage"])
print("Consumer running and subscribed to Intel topic")


def data_reader():
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            return -1
        if msg.error():
            print(msg.error())
            return -1

        value = msg.value()
        data = value.decode("utf-8")
        return data
    except KeyboardInterrupt:
        print("\n Stopping consumer")
        return -2

def close_consumer():
    consumer.close()
