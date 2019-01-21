from kafka import KafkaProducer
from kafka.errors import KafkaError

# $KAFKAZKHOSTS=zk0-kafkam.gbjxrqvgunuetntxekcww4yhsh.hx.internal.cloudapp.net:2181,zk1-kafkam.gbjxrqvgunuetntxekcww4yhsh.hx.internal.cloudapp.net:2181
# $KAFKABROKERS=wn0-kafkam.gbjxrqvgunuetntxekcww4yhsh.hx.internal.cloudapp.net:9092,wn1-kafkam.gbjxrqvgunuetntxekcww4yhsh.hx.internal.cloudapp.net:9092



producer = KafkaProducer(bootstrap_servers=['wn1-kafkam.gbjxrqvgunuetntxekcww4yhsh.hx.internal.cloudapp.net:9092'])
print 'producer initiated'
# Asynchronous by default
future = producer.send('test', b'raw_bytes')

# produce keyed messages to enable hashed partitioning
producer.send('test', key=b'foo', value=b'bar')

# encode objects via msgpack
# producer = KafkaProducer(value_serializer=msgpack.dumps)
# producer.send('test', {'key': 'value'})

# produce json messages
# producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
# producer.send('test', {'key': 'value'})

# produce asynchronously
for _ in range(100):
    producer.send('test', b'msg')

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)
    # handle exception

# produce asynchronously with callbacks
producer.send('test', b'raw_bytes').add_callback(on_send_success).add_errback(on_send_error)

# block until all async messages are sent
producer.flush()

