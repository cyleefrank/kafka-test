import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

# $KAFKAZKHOSTS=zk0-kafkam.gbjxrqvgunuetntxekcww4yhsh.hx.internal.cloudapp.net:2181,zk1-kafkam.gbjxrqvgunuetntxekcww4yhsh.hx.internal.cloudapp.net:2181
# $KAFKABROKERS=wn0-kafkam.gbjxrqvgunuetntxekcww4yhsh.hx.internal.cloudapp.net:9092,wn1-kafkam.gbjxrqvgunuetntxekcww4yhsh.hx.internal.cloudapp.net:9092

producer = KafkaProducer(bootstrap_servers=['wn1-kafkam.gbjxrqvgunuetntxekcww4yhsh.hx.internal.cloudapp.net:9092'])
print 'producer initiated'
# Asynchronous by default

with open('ratings.csv') as f:
	for line in f:
		future = producer.send('ratings', line)
		time.sleep(1)

producer.flush()

