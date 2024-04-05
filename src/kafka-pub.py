from confluent_kafka import Producer
import time
import csv
from datetime import datetime
import json

file_path = "./simulated_data.csv"
columns_data = {"idSensor": [], "tipoPoluente": [], "nivel": []}
x = 0
update_time = 1

# Configurações do produtor
producer_config = {
    'bootstrap.servers': 'localhost:29092,localhost:39092',
    'client.id': 'python-producer'
}

# Criar produtor
producer = Producer(**producer_config)


def get_sensor_data(file_path):
  # Initialize an empty list for each column name
    with open(file_path, newline='') as csvfile:
    # Cria um leitor CSV
        reader = csv.reader(csvfile)
        # Itera sobre as linhas do arquivo
        next(reader)
        for row in reader:
            # Extrai o nome do sensor e seu dado
            columns_data["idSensor"].append(row[0])  # Supondo que a primeira coluna seja o nome do sensor
            columns_data["tipoPoluente"].append(row[1])         # Supondo que a segunda coluna seja o dado do sensor
            columns_data["nivel"].append(row[2])

    print(columns_data)
    return columns_data

def show_sensor_data():
    global x
    if x < len(columns_data["idSensor"]):
        id_sensor = columns_data["idSensor"][x]
        tipo_poluente = columns_data["tipoPoluente"][x]
        nivel = columns_data["nivel"][x]
        x += 1
        return id_sensor, tipo_poluente, nivel
        
    else:
        print("Publicação de dados finalizada.")
        return "nan", "nan", "nan"

def delivery_callback(err, msg):
    if err:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def run_pub():
    try:
            while True:
                topic = "qualidadeAr"
                id_sensor, tipo_poluente, nivel = show_sensor_data()
                if id_sensor == "nan":
                    print("Publicação encerrada.")
                    quit()
                sensor_info = {
                    "idSensor": id_sensor,
                    "tipoPoluente": tipo_poluente,
                    "nivel": nivel,
                    "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                }
                message = "" + json.dumps(sensor_info)
                producer.produce(topic, message.encode('utf-8'), callback=delivery_callback)
                print(f"Publicado: {message}")
                producer.flush()
                time.sleep(float(update_time))
    except KeyboardInterrupt:
        pass
    finally:
        print("Publicação encerrada.")

def run():
    get_sensor_data(file_path)
    run_pub()

def main():
    run()

if __name__ == "__main__":
    main()
# try:
#     while True:
#         # Enviar mensagem
#         topic = 'test_topic'
#         message = input("Digite sua mensagem: ")
#         producer.produce(topic, message.encode('utf-8'), callback=delivery_callback)
# except KeyboardInterrupt:
#     pass
# finally:
#     producer.flush()