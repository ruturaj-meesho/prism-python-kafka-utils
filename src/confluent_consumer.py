from kafka import KafkaConsumer

topic = "messaging-api-test"
sasl_mechanism = "PLAIN"
username = "633QDYTLFCQYZJMO"
password = "HlB0jsDvogvyww0oP9q94qZvbP8B74C0Eq3IQem5vuU4Va1hMAe1mn73hjyDnp8q"
security_protocol = "SASL_SSL"
# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(topic, bootstrap_servers='pkc-w12qj.ap-southeast-1.aws.confluent.cloud:9092',
                         group_id='mygroupId',
                         #api_version=(0, 10),
                         security_protocol=security_protocol,
                         #ssl_context=context,
                         ssl_check_hostname=True,
                         ssl_cafile='/etc/ssl/cert.pem',
                         sasl_mechanism = sasl_mechanism,
                         sasl_plain_username = username,
                         sasl_plain_password = password)
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))