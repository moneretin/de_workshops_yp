from typing import Final, Callable

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

from python_producer import delivery_report, get_currency_rate_info

SOURCE_CURRENCY: Final[str] = 'USD'
TARGET_CURRENCY: Final[str] = 'RUB'
START_DATE: Final[str] = '2022-05-01'
END_DATE: Final[str] = '2022-05-10'

BROKER: Final[str] = '0.0.0.0:9092'
SCHEMA_REGISTRY: Final[str] = 'http://0.0.0.0:8081'
TOPIC_NAME: Final[str] = 'avro_exchange_currency_usd_rub'

value_schema_str: str = """
{
  "name": "exchangeRate",
  "type": "record",
  "namespace": "exchangerate.host",
  "fields": [
    {
      "name": "RUB",
      "type": "float"
    }
  ]
}
"""

key_schema_str: str = """
{
  "name": "exchangeRate",
  "type": "record",
  "namespace": "exchangerate.host",
  "fields": [
    {
      "name": "date_key",
      "type": "string"
    }
  ]
}
"""


def create_avro_producer(broker: str, schema_registry: str, key_schema: str,
                         value_schema: str, callback_func: Callable) -> AvroProducer:
    """
    Create avro producer for Kafka

    :param broker: Broker url
    :param schema_registry: Schema registry url
    :param key_schema: Key schema for topic
    :param value_schema: Value schema for topic
    :param callback_func: Func for async report

    :return Avro producer
    """
    avro_producer = AvroProducer({
        'bootstrap.servers': broker,
        'on_delivery': callback_func,
        'schema.registry.url': schema_registry
    }, default_key_schema=key_schema, default_value_schema=value_schema)

    return avro_producer


def write_avro_exchange_currency_to_kafka(avro_producer: AvroProducer, topic_name: str, data: dict) -> None:
    """
    Write exchange currency into Kafka in Avro format

    :param avro_producer: Avro producer
    :param topic_name: Topic name
    :param data: Data about exchange currency

    :return:
    """
    for key, value in data['rates'].items():
        key_json = {'date_key': key}
        while True:
            try:
                avro_producer.poll(0)
                avro_producer.produce(topic=topic_name, value=value, key=key_json)
                break
            except BufferError as err:
                print(f'Kafka error: {err}. Waiting one second.')
                avro_producer.poll(1)
    avro_producer.flush()


if __name__ == '__main__':
    topic_value_schema = avro.loads(value_schema_str)
    topic_key_schema = avro.loads(key_schema_str)
    exchange_data = get_currency_rate_info(SOURCE_CURRENCY, TARGET_CURRENCY, START_DATE, END_DATE)
    producer = create_avro_producer(BROKER, SCHEMA_REGISTRY, key_schema_str, value_schema_str, delivery_report)
    write_avro_exchange_currency_to_kafka(producer, TOPIC_NAME, exchange_data)
