"""
    This file will contain the helper functions and airflow dag used for streaming data from the API
"""
import uuid
from datetime import datetime
from airflow import DAG 
from airflow.operators.python import PythonOperator

# Creating the default argument that I will be using to attach to my dag to know-
# who owns the project
default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 12, 28, 10, 00)
}

# Function to get the API response and use it in the python operator
def get_data():
    import requests
    
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    
    # Geting the 'results' portion of the json response and the first index
    res = res['results'][0]
    
    return res

# Function that formats the data so that it fits into the kafka quoe
def format_data(res):
    # Storing the cleaned json data into a dictionary
    data = {}
    
    location = res['location']
    data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                    f"{location['city']}, {location['state']}, {location['country']}"
    
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    
    return data

# This function is responsible for streaming the API data into Kafka
def stream_data():
    import json
    import time
    import logging
    from kafka import KafkaProducer
    
    # A Kafka client that publishes records to the Kafka cluster.
    # To test this locally use bootstrap_servers=['localhost:9092']
    # To test this inside the container use bootstrap_servers=['broker:29092']
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()
    
    # This logic makes sure that while the requests to the API are within 1 minute,
    # the data is being cleaned and sent to the Kafka cluster. 
    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            res = format_data(res)
            
            # Push the data that is gotten from the json result into the queue
            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f"An error ocurred: {e}")
            continue

# Dag creation
with DAG('user_automation',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable=stream_data
    )

# stream_data();