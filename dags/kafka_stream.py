from datetime import datetime
import json
import requests
from kafka import KafkaProducer
import time
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1)
}

def get_data():
    result = requests.get("http://randomuser.me/api/")
    result = result.json()
    result = result['results'][0]
    
    return result

def format_data(result):
    data = {}

    data['id'] = uuid.uuid4()	
    data['first_name'] = result['name']['first']
    data['last_name'] = result['name']['last']
    data['gender'] = result['gender']
    data['postcode'] = result['postcode']
    data['email'] = result['email']
    data['usernam'] = result['login']['username']
    data['dob'] = result['dob']['date']
    data['registered_date'] = result['registered']['date']
    data['phone'] = result['phone']
    data['picture'] = result['picture']['medium']
    data['address'] = f"{str(result['location']['street']['number'])} {result['location']['street']['name']}, " \
                    f"{result['location']['city']}, {result['location']['state']}, {result['location']['postcode']}, {result['location']['country']}"


    return data

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            result = get_data()
            result = format_data(result)

            producer.send('users_created', json.dumps(result).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue


with DAG(#change name eventually
    'automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='streama_data_from_api',
        python_callable=stream_data
    )
