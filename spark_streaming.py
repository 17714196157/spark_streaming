#-*- coding:utf-8 -*-
from __future__ import print_function
import sys
import os
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from config.setting import zk_Quorum
from config.setting import kafka_in_topic

from code_src.spark_RDD_opt import run_line, run_line_qq
from code_src.spark_RDD_opt import modify_line
from code_src.spark_RDD_opt import industry_line
from code_src.spark_RDD_opt import out_to_kafka

# os.environ['JAVA_HOME'] = r'C:\Program Files (x86)\Java\jdk1.8.0_144'
# os.environ['SPARK_HOME'] = r'D:\home\spark'


# spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.3.1.jar spark_streaming.py
if __name__ == "__main__":
    conf = SparkConf().set("spark.python.profile", "true")
    sc = SparkContext(appName="PythonStreamingKafkaWordCount", conf=conf)
    ssc = StreamingContext(sc, 2)  # 创建DStream, 每2切出一个RDD

    # 广播变量
    # globalvar._init()
    # df_broadcast = sc.broadcast(value=df)  # 广播一个可以读的变量 每个Executor 保存一份 而不是每个task保存
    # column_name_list_broadcast = sc.broadcast(value=column_name_list)  # 广播一个可以读的变量 每个Executor 保存一份 而不是每个task保存
    # REGISTER_TIME_ENUM_broadcast = sc.broadcast(value=REGISTER_TIME_ENUM)  # 广播一个可以读的变量 每个Executor 保存一份 而不是每个task保存
    # REGISTER_CAPITAL_ENUM_broadcast = sc.broadcast(value=REGISTER_CAPITAL_ENUM)  # 广播一个可以读的变量 每个Executor 保存一份 而不是每个task保存
    # globalvar.set_value('df_broadcast', df_broadcast)
    # globalvar.set_value('column_name_list_broadcast', column_name_list_broadcast)
    # globalvar.set_value('REGISTER_TIME_ENUM_broadcast', REGISTER_TIME_ENUM_broadcast)
    # globalvar.set_value('REGISTER_CAPITAL_ENUM_broadcast', REGISTER_CAPITAL_ENUM_broadcast)

    zkQuorum, topic = (zk_Quorum, kafka_in_topic.decode())
    # "test-consumer-group" 是消费群组 ，需要在kafka 消费者配置文件
    # vi kafka/config /consumer.properties
    # #consumer group id
    # group.id = test - consumer - group
    # {topic: 1,topic: 0,topic: 2}) 是主题在那些kafka节点上有partition， partition是并发消费的基本单元了
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "test-consumer-group", {topic: 1, topic: 0, topic: 2})
    kvs = kvs.window(windowDuration=10, slideDuration=10)  # 没10s 是一个窗口 里面有5个RDD ，没10s计算一次窗口里的数据

    lines = kvs.map(lambda x: x[1])
    counts = lines.map(run_line)
    # counts = lines.map(run_line)
    # counts = counts.map(modify_line)
    # counts = counts.map(industry_line)

    counts.foreachRDD(lambda rdd: rdd.foreachPartition(out_to_kafka))  # 立刻执行算子

    # counts.saveAsTextFiles("file:///tmp/kafka")
    # counts.pprint()
    ssc.start()
    ssc.awaitTermination()

