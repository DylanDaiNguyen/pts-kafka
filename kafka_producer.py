from json import dumps
from random import random
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime

producer = KafkaProducer(bootstrap_servers=['192.168.121.121:9094'], value_serializer=lambda x: dumps(x).encode('utf-8'))
flag=0.5
# produce asynchronously
for x in range(100):
    if random()<flag:
        data = {'id': '1001', 'timestamp': str(datetime.now())}
        producer.send('TEST_1', value=data)
    else:
        data = {'id': '1002', 'timestamp': str(datetime.now())}
        producer.send('TEST_1', value=data)

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)
    # handle exception

# block until all async messages are sent
producer.flush()

# configure multiple retries
producer = KafkaProducer(retries=5)
