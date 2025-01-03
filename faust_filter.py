import faust
from confluent_kafka.serialization import Serializer, Deserializer


SENDER_TOPIC = 'messages'
RECEIVER_TOPIC = 'filtered_messages'

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

filtered_words = {'test', 'blue', 'grey'}
app = faust.App(
    "L2-message-filter",
    broker="localhost:9094",
    store="//rocksdb",
    value_serializer='raw'  #  UserSerializer()
  #  value_deserializer=UserDeserializer()
)
# table = app.Table(
#     "filter",
#     partitions=1,
#     default=lambda: Message(0, 0, "")
# )
incoming_messages = app.topic(SENDER_TOPIC, value_type=bytes)
filtered_messages = app.topic(RECEIVER_TOPIC, value_type=bytes)

@app.agent(incoming_messages)
async def process(stream):  # Filtering out messages, containing filtered words
    async for value in stream:
        if filtered_words.intersection(value.decode("utf-8").split(' ')):
            continue
        await filtered_messages.send(value=value)
