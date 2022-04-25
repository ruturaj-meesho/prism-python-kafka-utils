from kafka import KafkaProducer
from kafka.errors import KafkaError

topic = "messaging-api-test"
sasl_mechanism = "PLAIN"
username = "633QDYTLFCQYZJMO"
password = "HlB0jsDvogvyww0oP9q94qZvbP8B74C0Eq3IQem5vuU4Va1hMAe1mn73hjyDnp8q"
security_protocol = "SASL_SSL"
producer = KafkaProducer(bootstrap_servers='pkc-w12qj.ap-southeast-1.aws.confluent.cloud:9092',
                         security_protocol=security_protocol,
                         ssl_cafile='/etc/ssl/cert.pem',
                         sasl_mechanism=sasl_mechanism,
                         sasl_plain_username=username,
                         sasl_plain_password=password)

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)
    # handle exception

# produce asynchronously with callbacks
producer.send(topic, b'Hello World SSL').add_callback(on_send_success).add_errback(on_send_error)

# block until all async messages are sent
producer.flush()
