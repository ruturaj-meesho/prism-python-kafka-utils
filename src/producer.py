from kafka import KafkaConsumer, KafkaProducer
import kafka
import ssl
import logging
logging.basicConfig(level=logging.INFO)

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

try:
    print('At the start')
    topic = "new_test_topic"
    sasl_mechanism = "PLAIN"
    username = "633QDYTLFCQYZJMO"
    password = "HlB0jsDvogvyww0oP9q94qZvbP8B74C0Eq3IQem5vuU4Va1hMAe1mn73hjyDnp8q"
    security_protocol = "SASL_SSL"

    #context = ssl.create_default_context()
    #context.options &= ssl.OP_NO_TLSv1
    #context.options &= ssl.OP_NO_TLSv1_1

    consumer = KafkaConsumer(topic, bootstrap_servers='pkc-w12qj.ap-southeast-1.aws.confluent.cloud:9092',
                             #api_version=(0, 10),
                             security_protocol=security_protocol,
                             #ssl_context=context,
                             ssl_check_hostname=True,
                             ssl_cafile='/etc/ssl/cert.pem',
                             sasl_mechanism = sasl_mechanism,
                             sasl_plain_username = username,
                             sasl_plain_password = password)
    #ssl_certfile='../keys/certificate.pem',
    #ssl_keyfile='../keys/key.pem')#,api_version = (0, 10))

    producer = KafkaProducer(bootstrap_servers='pkc-w12qj.ap-southeast-1.aws.confluent.cloud:9092',
                             #api_version=(0, 10),
                             security_protocol=security_protocol,
                             #ssl_context=context,
                             ssl_check_hostname=True,
                             ssl_cafile='/etc/ssl/cert.pem',
                             sasl_mechanism=sasl_mechanism,
                             sasl_plain_username=username,
                             sasl_plain_password=password)
    #ssl_certfile='../keys/certificate.pem',
    #ssl_keyfile='../keys/key.pem')#, api_version = (0,10))
    # Write hello world to test topic
    producer.send(topic, bytes("Hello World SSL")).add_callback(on_send_success)
    producer.flush()

    # while(True):
    #     for msg in consumer:
    #         print('You have a new message :')
    #         print(msg)





except Exception as e:
    print(e)