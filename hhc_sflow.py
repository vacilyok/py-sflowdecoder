#!/bin/python
import time
from pyspark.sql.types import *
from pyspark.sql import SparkSession
# from pb import format_pb2 as histogram
from pb import histogramm_pb2 as histogram
import pika
import os
import datetime
import threading
from datetime import date, datetime
import configparser
import sys
from multiprocessing import Pool,Manager,Process
import pytz
import logging
# hadoop fs -rm -R /histogramm/level_1645995600 -- command delete file from hadoop

logging.basicConfig(filename='./hhc2.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
os.environ["HADOOP_USER_NAME"] = "hadoop"
os.environ["ARROW_LIBHDFS_DIR"] = "/usr/local/hadoop/lib/native"
tz = pytz.timezone('Europe/Moscow')
moscow_now = datetime.now(tz)




LevelType = {}
LevelType["PKTLEN"]  =      11
LevelType["SRCPORT"] =      31
LevelType["DSTPORT"] =      32
LevelType["L3"]      =      41
LevelType["L4"]      =      42
LevelType["TCP_STATES"] =   51

GlobProcc = 0
# *******************************************************************************************************
# Read config
# *******************************************************************************************************
dir_path = os.path.dirname(os.path.realpath(__file__))
config = configparser.ConfigParser()
params = config.read(f"{dir_path}/histogramm.conf")
if len(params) == 0:
    logging.error('Error: Not found config file histogramm.conf. Stop programm.')
    sys.exit(0)
hdfs_host = config.get('hdfs','host')
hdfs_port = config.get('hdfs','port')
hdfs_file_dir = config.get('hdfs','file_dir')
rmq_host = config.get('rabbitMq','host')
rmq_port = config.get('rabbitMq','port')
rmq_queue = config.get('rabbitMq','queue')
rmq_exchange = config.get('rabbitMq','exchange')

log_parh = config.get('logs','log_parh')
log_file_name = config.get('logs','log_file_name')

# *******************************************************************************************************
# spark = SparkSession.builder.appName("Python Spark histogramm collector").getOrCreate()
spark = SparkSession.builder\
   .master('spark://10.100.6.152:7077')\
   .appName('Python Spark histogramm collector')\
   .config('spark.executor.memory', '8gb')\
   .config("spark.cores.max", "8")\
   .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')   

Globalschema = StructType([
        StructField("timestamp", IntegerType(), True),
        StructField("subagent_id", IntegerType(), True),
        StructField("type_proto", IntegerType(), True),
        StructField("protocol_key", IntegerType(), True),
        StructField("protocol_val", IntegerType(), True),
        StructField("dst_ip", LongType(), True)
    ])
file_dir = f"/{hdfs_file_dir}/"


#***************************************************************************************************************
sc = spark.sparkContext
URI           = sc._gateway.jvm.java.net.URI
Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
fs = FileSystem.get(URI("hdfs://"+str(hdfs_host)+":"+str(hdfs_port)), Configuration())


#***************************************************************************************************************
def dataToParquet (dataArr):
    ts = int(time.time())
    start_time = time.time()
    start = time.perf_counter()
    round_hour = int(ts/3600) * 3600
    file_name = str(round_hour)
    print ("Start to file =======================================================")
    # rdd = spark.sparkContext.parallelize(LevelArrayToParquet)    
    # Data = spark.createDataFrame(rdd, Globalschema)
    columns=["timestamp","subagent_id","type_proto","protocol_key","protocol_val","dst_ip"]
    Data=spark.createDataFrame(dataArr,columns)    
    end = time.perf_counter()
    # LevelArrayToParquet[:] = []
    try:
        # Data.write.mode('append').partitionBy("type_proto", "num_protocol").parquet(f"hdfs://{hdfs_host}:{hdfs_port}{file_dir}{file_name}")            
        Data.write.mode('append').parquet(f"hdfs://{hdfs_host}:{hdfs_port}{file_dir}{file_name}")            
        # print(f"INFO: Append to  file  {file_name} ")
    except BaseException as e:
        print("Create File", e)
        Data.write.parquet(f"hdfs://{hdfs_host}:{hdfs_port}{file_dir}{file_name}")    
        # Data.write.partitionBy("type_proto", "num_protocol").parquet(f"hdfs://{hdfs_host}:{hdfs_port}{file_dir}{file_name}")    
        # print(f"INFO: CRATE FILE {file_name}")
        logging.info(f"INFO: CRATE FILE {file_name}")
    end = time.perf_counter()
    print ("Save to file = ", round(end - start,1), "=======================================================")



def disassemblyProto(proto):

    for proto_item in proto:
        proto_name, proto_data, probe_time, mashine_id,dst_ip_addr = proto_item
        dst_ip_addr = proto_item[4]
        for elem in proto_data:
            # print ("Insert to parquet", (probe_time,mashine_id,LevelType[proto_name],elem.key,elem.val,dst_ip_addr))
            LevelArrayToParquet.append((probe_time,mashine_id,LevelType[proto_name],elem.key,elem.val,dst_ip_addr))
        

#***************************************************************************************************************            
def histogramReciver(ch, method, properties, body):
    global GlobProcc
    pb_data = histogram.HistogramFamily.FromString(body)
    data_arr =[
        ("PKTLEN",list(pb_data.pck_len), pb_data.probe_time.seconds, pb_data.mashine_id,pb_data.dst_ip_addr),
        ("SRCPORT",list(pb_data.SRC_PORT), pb_data.probe_time.seconds, pb_data.mashine_id,pb_data.dst_ip_addr),
        ("DSTPORT",list(pb_data.DST_PORT), pb_data.probe_time.seconds, pb_data.mashine_id,pb_data.dst_ip_addr),
        ("L3",list(pb_data.L3), pb_data.probe_time.seconds, pb_data.mashine_id,pb_data.dst_ip_addr),
        ("L4",list(pb_data.L4), pb_data.probe_time.seconds, pb_data.mashine_id,pb_data.dst_ip_addr),
        ("TCP_STATES",list(pb_data.TCP_STATES), pb_data.probe_time.seconds, pb_data.mashine_id,pb_data.dst_ip_addr)
    ]
    st = time.perf_counter()
    disassemblyProto(data_arr)
    end = time.perf_counter()
    # print("Parse func = ", round(end-st,2))
    # try:
    #     with Pool(4) as p:
    #         p.map(disassemblyProto, data_arr )    
    # except BaseException as e:            
    #     print("Error =>", e)
    #     sys.exit(0)
    len_arr = len(LevelArrayToParquet)
    if len_arr%1000 == 1:
        print (len_arr)
    if len_arr > 150000:
        if GlobProcc == 0:
            GlobProcc = Process(target=dataToParquet, args=(LevelArrayToParquet,))
            GlobProcc.start()
        else:
            GlobProcc.terminate()
            GlobProcc = Process(target=dataToParquet, args=(LevelArrayToParquet,))
            GlobProcc.start()
        # p.join()
        LevelArrayToParquet[:] = []        
        # dataToParquet()
        
        


#***************************************************************************************************************
if __name__ == "__main__":
    pktLenData_to_save = []
    manager = Manager()
    LevelArrayToParquet = manager.list()
    try:
        credentials = pika.PlainCredentials('guest', 'guest')
        parameters = pika.ConnectionParameters(rmq_host,rmq_port,'/',credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        # try:
        #     channel.exchange_declare(exchange=rmq_queue, exchange_type="fanout", passive=False)
        # except:
        channel.queue_declare(queue="queue_sflow", durable=True)
        try:
            channel.queue_bind(exchange="from_sflow",queue="queue_sflow",routing_key="")
        except:
            logging.error(f"Error: Exchange with name from_sflow not found. Stop programm")
        try:
            channel.basic_consume(rmq_queue,histogramReciver, auto_ack=True)
        except:
            print("WTF &&")
        logging.info("Start consuming")
        channel.start_consuming()
    except BaseException as e:
            print(e.args[0])
            # logging.error(e.args[0])
            # sys.exit(0)
