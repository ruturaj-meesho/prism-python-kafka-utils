import json

from kafka import KafkaProducer
from json import dumps
from csv import reader
from concurrent.futures.thread import ThreadPoolExecutor
import sys
import time

producer = KafkaProducer(bootstrap_servers=['dp-ingestion-kb1.meeshoint.in:9092','dp-ingestion-kb2.meeshoint.in:9092','dp-ingestion-kb3.meeshoint.in:9092','dp-ingestion-kb4.meeshoint.in:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

def push_to_kafka(json_msg):
    json_msg['pipeline_id'] = 32028380
    json_msg['topicName'] = 'bulk_mixpanel_backfill'
    producer.send('bulk_mixpanel_backfill', json_msg).add_errback(on_send_error)
    print('Event sent for : '+str(json_msg['event']))

def on_send_success(record_metadata):
    print('Sent successfully, topic: '+str(record_metadata.topic)+' partition: '+str(record_metadata.partition)+' offset: '+str(record_metadata.offset))

def on_send_error(excp):
    print('I am an errback : '+ str(excp))

def process_events():
    events = set(['Duplicate Product Viewed'])
    count = 0
    with open('output/error_payloads.txt', 'r') as read_obj, ThreadPoolExecutor() as executor:
        for line in read_obj:
            try:
                json_msg = json.loads(line)
            except Exception as e:
                continue
            if (json_msg['event'] not in events):
                continue
            count+=1
            if (count<167613):
                print('Skipped event count '+str(count))
                continue
            executor.submit(push_to_kafka, json_msg)
            if (count%1000==0):
                print('submitted event number '+str(count))
    time.sleep(300)

process_events()