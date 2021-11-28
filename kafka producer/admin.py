from kafka.admin import KafkaAdminClient, NewTopic


def delete_topics(admin, topic):
    admin.delete_topics(topics=[topic])


def create_topics(admin, topic_list):
    admin.create_topics(new_topics=topic_list, validate_only=False)


if __name__ == '__main__':
    admin_client = KafkaAdminClient(bootstrap_servers="34.90.245.247:9092",
                                    client_id='test')  # use your VM's external IP Here!
    topic_list = [NewTopic(name="test", num_partitions=1, replication_factor=1)]
                  #NewTopic(name="wordcount", num_partitions=1, replication_factor=1)]
    create_topics(admin_client, topic_list)


#delete_topics(KafkaAdminClient(bootstrap_servers="34.90.245.247:9092", client_id='test'), 'test')