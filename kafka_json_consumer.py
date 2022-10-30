import argparse


from ctypes import *
CDLL(r"C:\Users\HP\anaconda3\Lib\site-packages\confluent_kafka.libs\librdkafka-09f4f3ec.dll")
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
import csv
# the record will get store as individual dic in list
API_KEY = 'Y5I5EAIXZS4CCXV4'
ENDPOINT_SCHEMA_URL  = 'https://psrc-8kz20.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'CuGxIMe/Vsv30pxw0wDnQBAIQPiGEYYsxQyGhSkh/doBHxrWXLtQ9gN0Su+ZpUNe'
BOOTSTRAP_SERVER = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'YLONCR6YGJNMMBVA'
SCHEMA_REGISTRY_API_SECRET = 'h02TQZD/Ym4Mga/X9S4q1AhhZat9xK8CgpN+csMfI2DKENJeKhO9Pw7+OcADaZ3H'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,

    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Restaurent:
    def __init__(self, record: dict):
        for k, v in record.items():
            setattr(self, k, v)

        self.record = record

    @staticmethod
    def dict_to_restaurent(data: dict, ctx):
        return Restaurent(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    schema_str = schema_registry_client.get_latest_version("restaurent-take-away-data-value").schema.schema_str
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Restaurent.dict_to_restaurent)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])


    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            restaurent = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            myDict = []
            if restaurent is not None:
                print("User record {}: restaurent: {}\n"
                      .format(msg.key(), restaurent))
                myDict.append(restaurent.__dict__)
                print('list',myDict)

            FieldName=[]
            for m in myDict[0]:
                FieldName.append(m)

            myFile = open('Streaming_Data.csv','w+')
            writer = csv.DictWriter(myFile, fieldnames=list(FieldName))
            writer.writeheader()
            for i in myDict:
                writer.writerow(i)
            myFile.close()
        except KeyboardInterrupt:
            break

    consumer.close()

main("restaurent-take-away-data")