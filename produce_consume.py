from argparse import ArgumentParser
from confluent_kafka import Producer, Consumer, serialization
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.serialization import Serializer, Deserializer
from faker import Faker
from time import sleep
from multiprocessing import Pool
from random import randint, choice
import json

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
       sender_id = int.from_bytes(value[4 + name_size:], byteorder="big")
       reciever_id = int.from_bytes(value[4 + name_size:], byteorder="big")
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
            msg = Message(choice(SENDER_IDS), choice(RECEIVER_IDS), Faker().sentence(nb_words=10))
            value = serializer(msg)
            # Отправка сообщения
            producer.produce(
                topic=SENDER_TOPIC,
                key='id'+str(randint(1, TOPIC_PARTITIONS)),
                value=value,
            )
#            sleep(1)
            producer.flush()
    except KeyboardInterrupt:
        print('Caught ctlr+C')
    finally:
        producer.flush()

def consume(poll=1):
    conf = {
        "bootstrap.servers": "localhost:9094",
        "group.id": "my-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    }
    if poll:
        conf["enable.auto.commit"] = True
    deserializer = UserDeserializer()
    # Создание консьюмера
    consumer = Consumer(conf)

    # Подписка на топик
    consumer.subscribe([RECEIVER_TOPIC])

    # Чтение сообщений в бесконечном цикле
    try:
        while True:
            # Получение сообщений
            msg = consumer.poll(poll)

            if msg is None:
                continue
            if msg.error():
                print(f"Ошибка: {msg.error()}")
                continue

            key = msg.key().decode("utf-8")
            value = deserializer(msg.value())
            if not poll:
                consumer.commit(asynchronous=False)
            print(f"Получено сообщение: key={key}, value={value.name}, offset={msg.offset()}, partition={msg.partition()}")
    except KeyboardInterrupt:
        print('caught ctrl+C')
    finally:
        # Закрытие консьюмера
        consumer.close()


if __name__ == '__main__':
    parser = ArgumentParser(usage='Simple Kafka producer/consumer')
    parser.add_argument('-p', action='store_true', help='Produce')
    parser.add_argument('-c', action='store_true', help='Consume')
    parser.add_argument('-t', action='store_true', help='Create topic')
    args = parser.parse_args()
    if args.p:
        produce()
    # Идея запускать одновременно два консьюмера с разными параметрами
    elif args.c:
        with Pool(2) as p:
            p.map(consume(), [0.1, None])
    elif args.t:
        create_topic()
    else:
        print('Unknown action')
