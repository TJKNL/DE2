"""
This application is designed to immitate a streaming data source.
The code splits a .csv file into seperate lines which form the stream.
"""
from kafka import KafkaProducer
from time import sleep
from random import randint

# Enter the IP of the machine running Kafka.
vm_ip = '34.91.159.65'

# Messeges need to be encoded in bytes before sending to Kafka.
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
    # Load three seperate files.
    with open('/Users/twan/PycharmProjects/DE2/stream_data/initial_movies.csv') as f:
        lines_movies = f.readlines()
    with open('/Users/twan/PycharmProjects/DE2/stream_data/initial_roles.csv') as f2:
        lines_roles = f2.readlines()
    with open('/Users/twan/PycharmProjects/DE2/stream_data/initial_movies_directors.csv') as f3:
        lines_directors = f3.readlines()

    # Send each line of the file to Kafka.
    # Try, except statements are meant to combat the inequal length of the .csv files.
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
        try:
            kafka_python_producer_sync(producer, lines_directors[i], 'directors')
        except:
            print('Directors out of data, nothing to stream.')
        i += 1
        # After sending 3 lines. Wait for a random time interval. (seconds)
        sleep(randint(1, 2))
    f.close()
    f2.close()
    f3.close()


