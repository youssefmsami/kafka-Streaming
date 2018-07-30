# -*- coding: utf-8 -*-
"""
Created on Sat Jun 23 19:49:08 2018

@author: ysami
"""


from kafka import KafkaProducer
from random import random, randint
import json
import itertools  
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row,SQLContext
import sys
import requests
import json
import re
from kafka import SimpleClient, SimpleConsumer
import json
from datetime import datetime
import pandas as pd
from bokeh.io import curdoc
from bokeh.plotting import figure, show
from bokeh.client import push_session
from bokeh.models import ColumnDataSource
import os
import threading, logging, time
import multiprocessing
from kafka import SimpleProducer, KafkaClient
import pykafka
sys.path.append(os.getcwd())




    

def process_rdd(time, rdd):
    
    consumer=[]
    consumer.append(str(time))
    consumer.append(rdd.first())
    c_df=pd.DataFrame({'time':[str(time)], 'value':[rdd.first()]})
    
    global time_val
    global value
    value=rdd.first()
    time_val=str(time)
    global df
    df = df.append(pd.DataFrame(c_df))
   
    message = {"time" : str(time), "value" : value}
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    future=producer.send('results',message)
    producer.flush()
    
    
    print(df.to_string())
    print(consumer)


#if __name__ == "__main__":

	#Create Spark Context to Connect Spark Cluster
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
kstream = KafkaUtils.createDirectStream(ssc, topics = ['logs'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
events = kstream.map(lambda x: x[1].encode("ascii","ignore"))
#ip = re.compile("^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
    
regex = '([(\d\.)]+) - - \[(.*?)\]'

new_ips = events.map(lambda k: re.match(regex, k).groups())
total_ips=new_ips.countByWindow(500,10)
    
total_ips.pprint()
total_ips.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination()
    
    
