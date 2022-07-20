import json
import logging as log
import sys
from typing import Optional, Final

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logger = log.getLogger()
handler = log.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logger.setLevel(log.INFO)

SOURCE_CURRENCY: Final[str] = 'USD'
TARGET_CURRENCY: Final[str] = 'RUB'
START_DATE: Final[str] = '2022-05-01'
END_DATE: Final[str] = '2022-05-10'

BROKER: Final[str] = '0.0.0.0:9092'
REPLICATION_FACTOR: Final[int] = 1
NUM_OF_PARTITIONS: Final[int] = 3
TOPIC_NAME: Final[str] = 'exchange_currency_usd_rub'


def get_currency_rate_info(source_currency: str, target_currency: str, start_date: Optional[str] = None,
                           end_date: Optional[str] = None) -> dict:
    """
    Get currency rate information about pair

    :param source_currency: source currency
    :param target_currency: target currency
    :param start_date: start date for uploading data. Default: None
    :param end_date: end date for uploading data. Default: None

    :return data: information about rate of pair
    """
    params: dict = {'base': source_currency, 'symbols': target_currency}
    url: str = f'https://api.exchangerate.host/timeseries?start_date={start_date}&end_date={end_date}'

    try:
        response = requests.get(url, params=params)
        data = response.json()
    except Exception as ex:
        log.error(f'Unable get currency rate info. Error: {ex}')
        raise ex

    return data


def create_kafka_topic(broker: str, topic_name: str, partitions: int, replicas: int) -> None:
    """
    Create topic in Kafka

    :param broker: Broker name
    :param topic_name: Topic name
    :param partitions: Number of partitions
    :param replicas: Number of replicas

    :return
    """
    kafka_client = AdminClient({'bootstrap.servers': broker})
    new_topic = [NewTopic(topic_name, num_partitions=partitions, replication_factor=replicas)]

    try:
        kafka_client.create_topics(new_topic)
        log.info('Topic created')
    except Exception as ex:
        log.error(f'Failed to create topic. Error {ex}')


def delivery_report(err, msg):
    """
    Called once for each message produced to indicate delivery result. Triggered by poll() or flush()
    :param err: Error info
    :param msg: Message info
    """
    if err is None:
        log.info(f'Message delivered to topic {msg.topic()} in partition [{msg.partition()}]')
    else:
        log.error(f'Message delivery failed: {err}')
        raise Exception('Error occurred while sending messages')


def write_exchange_currency_to_kafka(broker: str, topic_name: str, data: dict) -> None:
    """
    Write message about exchange currency to Kafka

    :param broker: Broker url
    :param topic_name: Topic name
    :param data: Data about exchange currency

    :return
    """
    producer = Producer({'bootstrap.servers': broker})

    for item in data['rates'].items():
        message = json.dumps(item).encode('utf-8')
        while True:
            try:
                producer.poll(0)
                producer.produce(topic_name, value=message, callback=delivery_report)
                break
            except BufferError as err:
                log.error(f'Kafka error: {err}. Waiting one second.')
                producer.poll(1)
    producer.flush()
    log.info('Data sent to Kafka')


if __name__ == '__main__':
    exchange_data = get_currency_rate_info(SOURCE_CURRENCY, TARGET_CURRENCY, START_DATE, END_DATE)
    create_kafka_topic(BROKER, TOPIC_NAME, NUM_OF_PARTITIONS, REPLICATION_FACTOR)
    write_exchange_currency_to_kafka(BROKER, TOPIC_NAME, exchange_data)
