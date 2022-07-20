import time

from confluent_kafka import Consumer

from python_producer import BROKER, TOPIC_NAME


def read_from_kafka_topic(broker: str, topic_name: str) -> None:
    """
    Read message from Kafka

    :param broker: Broker url
    :param topic_name: Topic name

    :return
    """
    consumer = Consumer({'bootstrap.servers': broker, 'group.id': 'practicumgroup',
                         'auto.offset.reset': 'earliest', 'enable.auto.commit': False})
    consumer.subscribe([topic_name])

    start_time = time.time()
    seconds = 30

    while True:
        current_time = time.time()
        elapsed_time = current_time - start_time
        if elapsed_time > seconds:
            break
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f'Consumer error: {msg.error()}')
            continue

        print(f'Received message: {msg.value().decode("utf-8")}')

    consumer.close()


if __name__ == '__main__':
    read_from_kafka_topic(BROKER, TOPIC_NAME)
