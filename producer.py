import json

import os
import yaml
from birdy.twitter import StreamClient
from kafka import KafkaProducer

TWITTER_AUTH_FILE = 'conf/twitter.yml'
KAFKA_CONF_FILE = 'conf/kafka.yml'


def main():
    #Twitter Setup
    twitter_tokens = yaml.safe_load(open(os.path.expanduser('~') + TWITTER_AUTH_FILE))
    client = StreamClient(twitter_tokens['consumer_key'], twitter_tokens['consumer_secret'],twitter_tokens['access_token'], twitter_tokens['access_secret'])
    resource = client.stream.statuses.filter.post(track='java')

    # Kafka Setup
    kafka_conf = yaml.safe_load(open(KAFKA_CONF_FILE))
    producer = KafkaProducer(bootstrap_servers=kafka_conf['bootstrap_servers'], client_id=kafka_conf['client_id'],value_serializer=json.dumps)

    # Start Pumping to twitter
    if producer is not None:
        for data in resource.stream():
            producer.send(kafka_conf['topic'], data)
            print json.dumps(data, sort_keys=True)

if __name__ == "__main__":
    main()
