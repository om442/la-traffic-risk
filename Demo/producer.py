from time import sleep
import requests
import json
from kafka import KafkaProducer
import pandas as pd
from sodapy import Socrata
import threading


def get_data(limit, offset):
    # Data is pulled from public dataset using API calls with a specified frequency and only a certain amount of records are pulled at a time.
    client = Socrata("data.lacity.org", app_token=None, timeout=20)

    results = client.get("d5tf-ez2w", limit=limit, offset=offset)

    lengt = len(results)
    results = json.dumps(results)

    return results, lengt


def publish_message(producer_instance, topic_name, value):
    try:

        producer_instance.send(topic_name, value=value)
        producer_instance.flush()

        print("Message published successfully.")
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    # Connect to kafka producer
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=[
                                  'localhost:9092'], value_serializer=lambda x: x.encode('utf-8'), api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def fun_set(limit, kafka_producer, topic, offset):
    # Data is being pushed onto kafka via threading for efficiency every 10s. The frequency of pushing data can be changed based on convienience.
    print('Data Retrieval Begins')
    print('Offset value:', offset)

    val, lengt = get_data(limit, offset)
    print('Data Retrieval ends')
    offset = offset+min(limit, lengt)
    publish_message(kafka_producer, topic, val)
    print('Data pushed into kafka')
    threading.Timer(
        10.0, fun_set, [limit, kafka_producer, topic, offset]).start()


def main():
    # Push streaming data to producer
    kafka_producer = connect_kafka_producer()

    # Limit to 1k records per push and offset is set here so that only new data is updated in the public dataset is taken in. The raw data is already pushed onto our database.
    topic = 'traffic_data'
    limit = 1000
    offset = 567120
    print('Intializing pushing of data to Kafka')
    fun_set(limit, kafka_producer, topic, offset)


if __name__ == '__main__':
    main()
