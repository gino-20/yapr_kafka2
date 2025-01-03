from argparse import ArgumentParser
from confluent_kafka import Producer, Consumer, serialization
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.serialization import Serializer, Deserializer
from faker import Faker
from time import sleep
from multiprocessing import Pool
from random import randint, choice
import json
import faust
import keyword
from ksqldb import KSQLdbClient

from modules import init_logger

SENDER_TOPIC = 'messages'
RECEIVER_TOPIC = 'filtered_messages'
SENDER_IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
RECEIVER_IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
TOPIC_PARTITIONS = 3


class Message:
    def __init__(self, sender_id, reciever_id, message):
        self.sender_id = sender_id
        self.reciever_id = reciever_id
        self.message = message

class UserSerializer(Serializer):
   def __call__(self, obj: Message, ctx=None):
       message_bytes = obj.message.encode("utf-8")
       message_size = len(message_bytes)

       result = message_size.to_bytes(4, byteorder="big")
       result += message_bytes
       result += obj.sender_id.to_bytes(4, byteorder="big")
       result += obj.reciever_id.to_bytes(4, byteorder="big")
       return result

class UserDeserializer(Deserializer):
   def __call__(self, value: bytes, ctx=None):
       if not value:
           return None
       message_size = int.from_bytes(value[:4], byteorder="big")
       message = value[4:4 + message_size].decode("utf-8")
       sender_id = int.from_bytes(value[4 + message_size:], byteorder="big")
       reciever_id = int.from_bytes(value[4 + message_size:], byteorder="big")
       return Message(sender_id, reciever_id, message)

def produce():
    
    conf = {
        "bootstrap.servers": "localhost:9094",
        "acks": "all"
    }
    # Создание продюсера
    producer = Producer(conf)
    serializer = UserSerializer()
    try:
        while True:
            # ТУДУ: не кодировать отправитедя в сообщение, а использовать его как ключ!
            # msg = Message(choice(SENDER_IDS), choice(RECEIVER_IDS), Faker().sentence(nb_words=10))
            msg = str(choice(SENDER_IDS)) + ', ' + Faker().sentence(nb_words=15)
            # Отправка сообщения
            producer.produce(
                topic=SENDER_TOPIC,
                key=bytes(str(choice(RECEIVER_IDS)), encoding="utf-8"),
                value=msg
            )
            sleep(5)
            producer.flush()
    except KeyboardInterrupt:
        logger.debug('Caught ctlr+C')
    finally:
        producer.flush()


def message_consumer():
    pass
    # conf = {
    #     "bootstrap.servers": "localhost:9094",
    #     "group.id": "mygroup",
    #     "auto.offset.reset": "earliest"
    #     }
    
    # consumer = Consumer(conf)
    # deserializer = UserDeserializer()
    # consumer.subscribe([RECEIVER_TOPIC])
    # try:
    #     print('Polling mesages. Press f to filter out sender')
    #     while True:
    #         msg = consumer.poll(1.0)
    #         if msg is None:
    #             continue
    #         if msg.error():
    #             print(f"Consumer error: {msg.error()}")
    #             continue
    #         key = msg.key().decode('utf-8')
    #         value = deserializer(msg.value())
    #         if keyboard.is_pressed('f'):

if __name__ == '__main__':
    parser = ArgumentParser(usage='Simple Kafka producer/consumer')
    parser.add_argument('-p', action='store_true', help='Produce')
    parser.add_argument('-c', action='store_true', help='Consume')
    args = parser.parse_args()
    logger = init_logger()
    if args.p:
        produce()
    elif args.c:  # Consumer
        message_consumer()
    else:
        logger.debug('Unknown action')
