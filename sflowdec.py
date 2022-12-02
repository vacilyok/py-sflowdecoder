#!/bin/python
import time
from pb import flow_pb2 as flowproto
import pika
import os
import datetime
from datetime import date, datetime
import configparser
import sys
from time import gmtime, strftime
import pytz
import logging
import base64
import ipaddress

logging.basicConfig(filename='hhc.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
tz = pytz.timezone('Europe/Moscow')
moscow_now = datetime.now(tz)

LevelType = {}
LevelType["PKTLEN"] = 11
LevelType["L3_SRC"] = 31
LevelType["L3_DST"] = 32
LevelType["L4_SRC"] = 41
LevelType["L4_DST"] = 42


# *******************************************************************************************************
# Read config
# *******************************************************************************************************
dir_path = os.path.dirname(os.path.realpath(__file__))
config = configparser.ConfigParser()
params = config.read(f"{dir_path}/settings.conf")
if len(params) == 0:
    logging.error('Error: Not found config file histogramm.conf. Stop programm.')
    sys.exit(0)
rmq_host = config.get('rabbitMq','host')
rmq_port = config.get('rabbitMq','port')
rmq_queue = config.get('rabbitMq','queue')
rmq_exchange = config.get('rabbitMq','exchange')

log_parh = config.get('logs','log_parh')
log_file_name = config.get('logs','log_file_name')


#***************************************************************************************************************            
def histogramReciver(ch, method, properties, body):
    pb_data = flowproto.FlowMessage.FromString(body)
    pktraw = base64.encodebytes(pb_data.DstAddr)
    dstip = int.from_bytes(pb_data.DstAddr,byteorder='big')
    dstip_str = str(ipaddress.IPv4Address(dstip))
    print(f"DstAddr= {dstip_str} tcp flag={pb_data.TCPFlags} , DstPort= {pb_data.DstPort}  SrcPort={pb_data.SrcPort} L3={pb_data.Etype}  L4={pb_data.Proto}")
    
    
    


#***************************************************************************************************************
if __name__ == "__main__":
    try:
        credentials = pika.PlainCredentials('guest', 'guest')
        parameters = pika.ConnectionParameters(rmq_host,rmq_port,'/',credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        # try:
        #     channel.exchange_declare(exchange=rmq_queue, exchange_type="fanout", passive=False)
        # except:
        channel.queue_declare(queue=rmq_queue, durable=True, auto_delete=True)
        try:
            channel.queue_bind(exchange=rmq_exchange,queue=rmq_queue,routing_key="")
        except:
            logging.error(f"Error: Exchange with name '{rmq_queue}' not found. Stop programm")

        channel.basic_consume(rmq_queue,histogramReciver, auto_ack=True)
        cur_time = strftime("%Y-%m-%d %H:%M:%S", gmtime() )
        print("Start consuming")
        channel.start_consuming()
    except BaseException as e:
            print("Errors .....", e)
            
            # print(e.args[0])
            # logging.error(e.args[0])
            # sys.exit(0)
