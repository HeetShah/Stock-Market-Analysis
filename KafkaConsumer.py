from kafka import KafkaConsumer
from time import sleep
from json import dumps,loads
import json
from s3fs import S3FileSystem

def main():
    consumer = KafkaConsumer(
        'demo-test',
        bootstrap_servers=['INSERT_AWS_PUBLIC_IP:9092'], # Insert AWS Public IP
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    s3 = S3FileSystem()
    
    for count, i in enumerate(consumer):
        with s3.open("s3://kafka-stock-market-heet/stock_market_{}.json".format(count), 'w') as file:
            json.dump(i.value, file)    
    
        

if __name__ == "__main__":
    main()
