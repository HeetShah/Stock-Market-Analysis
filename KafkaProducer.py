import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from time import sleep
from json import dumps
import json

def main():
    
    producer = KafkaProducer(bootstrap_servers=['18.222.191.141:9092'], 
                            value_serializer=lambda x: 
                            dumps(x).encode('utf-8'))


    # while True:
    #     user_input = input("Enter a message: ")
    #     producer.send('demo-test', value=user_input)
    #     producer.flush()
    #     print("Message sent!")
    #     sleep(1)
        
    df = pd.read_csv('indexProcessed.csv')
    
    while True:
        random_stock_data = df.sample(1).to_dict(orient='records')[0]
        producer.send('demo-test', value=random_stock_data)
        sleep(1)
        print("Message sent!")
        
        

if __name__ == "__main__":
    main()
        