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
    with open('/Users/twan/PycharmProjects/DE2/stream_data/movies.csv') as f:
        lines_movies = f.readlines()
    with open('/Users/twan/PycharmProjects/DE2/stream_data/roles.csv') as f2:
        lines_roles = f2.readlines()


    i = 0

    for i in range(0, len(lines_roles)):
        try:
            kafka_python_producer_sync(producer, lines_movies[i], 'movies')
            pass
        except:
            print('Movies out of data, nothing to stream.')
        try:
            kafka_python_producer_sync(producer, lines_roles[i], 'roles')
        except:
            print('Roles out of data, nothing to stream.')
        i += 1
        sleep(randint(1, 2))
    f.close()
    f2.close()


