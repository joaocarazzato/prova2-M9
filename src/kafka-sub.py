from confluent_kafka import Consumer, KafkaError
import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os


# Configurações do consumidor
consumer_config = {
    'bootstrap.servers': 'localhost:29092,localhost:39092',
    'group.id': 'python-consumer-group',
    'auto.offset.reset': 'earliest'
}

# Criar consumidor
consumer = Consumer(**consumer_config)

# Assinar tópico
topic = 'qualidadeAr'
consumer.subscribe([topic])

load_dotenv()

server = os.getenv('SERVER')
port = int(os.getenv('PORT'))
keepalive = int(os.getenv('KEEPALIVE'))
username = os.getenv('USERNAME_MQTT')
pw = os.getenv('PW_MQTT')
mongo_user= os.getenv('MONGO_USER')
mongo_pass= os.getenv('MONGO_PASS')

uri = f"mongodb+srv://{mongo_user}:{mongo_pass}@cluster0.4b4rfa4.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
    database = client["sensor_data"]["qualidadeAr"]
except Exception as e:
    print(e)

# Consumir mensagens
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        sensor_params = msg.value().decode("utf-8")
        sensor_info = json.loads(sensor_params)
        print(f'Received message! | {sensor_info["timestamp"]} | idSensor: {sensor_info["idSensor"]}, tipoPoluente: {sensor_info["tipoPoluente"]}, nivel: {sensor_info["nivel"]}')
        database.insert_one(sensor_info)
except KeyboardInterrupt:
    pass
finally:
    # Fechar consumidor
    consumer.close()
