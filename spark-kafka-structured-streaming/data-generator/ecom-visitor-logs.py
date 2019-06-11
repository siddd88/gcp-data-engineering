#!/usr/bin/python
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
import json 

from kafka import KafkaProducer

cluster_name = "gs://cluster_name" # cluster in which kafka & zookeeper are installed  

class switch(object):
    def __init__(self, value):
        self.value = value
        self.fall = False

    def __iter__(self):
        yield self.match
        raise StopIteration

    def match(self, *args):
        if self.fall or not args:
            return True
        elif self.value in args: 
            self.fall = True
            return True
        else:
            return False

parser = argparse.ArgumentParser(__file__, description="User Visit Generator")
parser.add_argument("--output", "-o", dest='output_type', help="Write to a Log file, a gzip file or to STDOUT", choices=['LOG','GZ','CONSOLE'] )
parser.add_argument("--log-format", "-l", dest='log_format', help="Log format, Common or Extended Log Format ", choices=['CLF','ELF',"common"])
parser.add_argument("--num", "-n", dest='num_lines', help="Number of lines to generate (0 for infinite)", type=int, default=1)
parser.add_argument("--prefix", "-p", dest='file_prefix', help="Prefix the output file name", type=str)
parser.add_argument("--sleep", "-s", help="Sleep this long between lines (in seconds)", default=0.0, type=float)

args = parser.parse_args()

log_lines = args.num_lines
file_prefix = args.file_prefix
output_type = args.output_type
log_format = args.log_format

faker = Faker()


timestr = time.strftime("%Y%m%d-%H%M%S")
otime = datetime.datetime.now()

response=["200","404","500","301"]

verb=["GET","POST",'DELETE',"PUT"]

mens_wear_cart =["/products/mens-wear/shoes/cart.jsp?pid=","/products/mens-wear/formal-tshirts/cart.jsp?pid="]
mens_wear_cart +=["/products/mens-wear/sports/cart.jsp?pid=","/products/men/home-lifestyle/cart.jsp?pid="]
mens_wear_cart +=["/products/men/home-gifting/cart.jsp?pid=","/products/men/bags/cart.jsp?pid="]

womens_wear_cart =["/products/womens-wear/shoes/cart.jsp?pid=","/products/womens-wear/accessories/cart.jsp?pid="]
womens_wear_cart +=["/products/womens-wear/grooming/cart.jsp?pid=","/products/womens-wear/bags/cart.jsp?pid="]
womens_wear_cart +=["/products/women/perfumes/cart.jsp?pid=","/products/women/home-gifting/cart.jsp?pid="]

women_product_hits = ["/women-clothing/list/dresses/","/women-clothing/list/leggings/"]
women_product_hits += ["/women-clothing/list/arabian-clothing/","/women-clothing/list/sports-tees/"]
women_product_hits += ["/women/list/perfumes/","/women-clothing/list/pants/"]
women_product_hits += ["/women-clothing/list/accessories/","/women-clothing/list/denims/"]

mens_product_hits = ["/men-clothing/list/polo-tshirts/","/men-clothing/list/sports-tshirts/"]
mens_product_hits += ["/men-clothing/list/polo-tshirts/","/men-clothing/list/sports-tshirts/"]
mens_product_hits += ["/men-clothing/list/perfumes/","/men-clothing/list/trousers/"]
mens_product_hits += ["/men-clothing/list/accessories/","/men-clothing/list/denims/"]

resources = []
resources += mens_wear_cart
resources += mens_wear_cart+womens_wear_cart +mens_product_hits+women_product_hits

ualist = [faker.firefox, faker.chrome, faker.safari, faker.internet_explorer, faker.opera]

flag = True
while (flag):
    if args.sleep:
        increment = datetime.timedelta(seconds=args.sleep)
    else:
        increment = datetime.timedelta(seconds=random.randint(10,20))
    otime += increment

    ip = faker.ipv4()
    fake_state = faker.state() #US States
    dt = otime.strftime("%Y-%m-%d %H:%M:%S")
    tz = datetime.datetime.now(local).strftime('%z')
    vrb = numpy.random.choice(verb,p=[0.6,0.1,0.1,0.2])

    uri = random.choice(resources)

    if "products" in uri :
        uri += str(random.randint(1,2000))

    resp = numpy.random.choice(response,p=[0.9,0.04,0.02,0.04])
    byt = int(random.gauss(5000,50))
    referer = faker.uri()
    useragent = numpy.random.choice(ualist,p=[0.5,0.3,0.1,0.05,0.05] )()
    

    producer = KafkaProducer(bootstrap_servers=[cluster_name+'-w-0:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))


    uri_segments = [i for i in uri.split("/") if i !='']

    if len(uri_segments) == 4 : 
        pid = int(uri_segments[3].split("pid=",1)[1])
    else :
        pid = None

    json_str = {"date_time": dt, "state":fake_state, "ip_address":ip, "category":uri_segments[0],'sub_cat':uri_segments[1],'type':uri_segments[2],"pid":pid}
   
    producer.send(b'user_browsing_logs', value=json_str)

    if args.sleep:
        time.sleep(args.sleep)
    else : 
        time.sleep(2)
        
