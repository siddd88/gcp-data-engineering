import time
import datetime
import pytz
import numpy
import random
import gzip
import zipfile
import sys
import argparse
from faker import Faker
from random import randrange
from tzlocal import get_localzone
local = get_localzone()
import csv
import sys 
import time 

from google.cloud import storage


faker = Faker()

def upload_to_bucket(blob_name, path_to_file, bucket_name):
    
    storage_client = storage.Client.from_service_account_json('creds.json')

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)
    return blob.public_url


flag= True 
while flag : 
    timestr = time.strftime("%Y%m%d-%H%M%S")
    data = list() 
    for i in range(100) : 
        otime = datetime.datetime.now()
        increment = datetime.timedelta(seconds=random.randint(30, 300))
        otime += increment

        
        date_time = otime.strftime("%Y-%m-%d %H:%M:%S")

        # event_time = otime + datetime.timedelta(days = -130)

        # event_time.replace(hour=12)
        
        event_time = date_time
        
        house_uid = random.randint(1,2000)

        
        dishwasher_consumption = random.uniform(0.0,4)

        furnace_1_consumption = random.uniform(0.3,1.2)

        furnace_2_consumption = random.uniform(0.2,0.7)

        fridge_consumption = random.uniform(0.5,1.7)

        wine_cellar_consumption = random.uniform(0.2,0.5)

        microwave_consumption = random.uniform(0.02,0.1)

        living_room_consumption = random.uniform(0.01,0.08)        

        overall_house_energy_consumption = dishwasher_consumption + furnace_1_consumption + furnace_2_consumption + fridge_consumption + wine_cellar_consumption + microwave_consumption + living_room_consumption

        tup =(event_time,house_uid,overall_house_energy_consumption,dishwasher_consumption,furnace_1_consumption,furnace_2_consumption,fridge_consumption,wine_cellar_consumption,microwave_consumption,living_room_consumption)
        data.append(tup)
        if i == 99:
            break


    kwargs = {'newline': ''}
    mode = 'w'
    if sys.version_info < (3, 0):
        kwargs.pop('newline', None)
        mode = 'wb'
    
    with open('csv-files/server-logs-'+timestr+'.csv', mode, **kwargs) as fp:
        writer = csv.writer(fp, delimiter=',')
        writer.writerows(data)


    filename = 'server-logs-'+timestr+'.csv'
    blob_name = "streaming_files/"+filename
    bucket = "your_bucket_name"

    upload_to_bucket(blob_name,"csv-files/"+filename,bucket)

    time.sleep(60)
