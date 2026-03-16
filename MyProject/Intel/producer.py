import json
from confluent_kafka import Producer



producer = Producer({"bootstrap.servers": "kafka:9092"})


def delivery_report(err, msg):
    if err:
        print(err)
    else:
        print(msg)


def sending_data(data):
    data = json.dumps(data).encode("utf-8")
    producer.produce(
        topic="intel_signals_dlq",
        value=data,
        callback=delivery_report
    )
    producer.flush()





