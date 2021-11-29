from kafka import KafkaProducer
from time import sleep
from random import randint


vm_ip = '34.141.229.194'

def kafka_python_producer_sync(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8'))
    print("Sending " + msg)
    producer.flush(timeout=60)


def success(metadata):
    print(metadata.topic)


def error(exception):
    print(exception)


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=f'{vm_ip}:9092')  # use your VM's external IP Here!
    with open('/Users/twan/PycharmProjects/DE2/stream_data/data.csv') as f:
        lines = f.readlines()

    for line in lines:
        kafka_python_producer_sync(producer, line, 'test')
        sleep(randint(1,3))

    f.close()


