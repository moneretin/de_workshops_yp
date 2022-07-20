import time

from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

from python_avro_producer import BROKER, SCHEMA_REGISTRY, TOPIC_NAME


def read_from_avro_kafka_topic(broker: str, schema_registry: str, topic_name: str) -> None:
    """
    Read avro message from Kafka

    :param broker: Broker url
    :param schema_registry: Schema registry url
    :param topic_name: Topic name

    :return
    """
    avro_consumer = AvroConsumer({'bootstrap.servers': broker, 'group.id': 'practicumgroup',
                                  'auto.offset.reset': 'earliest', 'enable.auto.commit': False,
                                  'schema.registry.url': schema_registry})

    avro_consumer.subscribe([topic_name])

    start_time = time.time()
    seconds = 30

    while True:
        current_time = time.time()
        elapsed_time = current_time - start_time
        if elapsed_time > seconds:
            break

        try:
            msg = avro_consumer.poll(1.0)
        except SerializerError as ex:
            print(f'Message deserialization failed: {ex}')
            break
        if msg is None:
            continue
        if msg.error():
            print(f'AvroConsumer error: {msg.error()}')
            continue

        print(f'Received key: {msg.key()}. Received message: {msg.value()}')

    avro_consumer.close()


if __name__ == '__main__':
    read_from_avro_kafka_topic(BROKER, SCHEMA_REGISTRY, TOPIC_NAME)
