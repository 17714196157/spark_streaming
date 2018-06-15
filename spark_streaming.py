#-*- coding:utf-8 -*-
from __future__ import print_function
import sys
import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from config.setting import zk_Quorum

from code.spark_RDD_opt import run_line
from code.spark_RDD_opt import modify_line
from code.spark_RDD_opt import industry_line
from code.spark_RDD_opt import out_to_kafka

# os.environ['JAVA_HOME'] = r'C:\Program Files (x86)\Java\jdk1.8.0_144'
# os.environ['SPARK_HOME'] = r'D:\home\spark'


# spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.3.1.jar spark_streaming.py
if __name__ == "__main__":
    # if len(sys.argv) != 3:
    #     print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
    #     exit(-1)

    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    ssc = StreamingContext(sc, 10)  # 创建DStream, 每60s 执行一次计算流
    # zkQuorum, topic = sys.argv[1:]  # zk的访问ip或者主机名
 
    zkQuorum, topic = (zk_Quorum, 'test')
    # "test-consumer-group" 是消费群组 ，需要在kafka 消费者配置文件
    # vi kafka/config /consumer.properties
    # #consumer group id
    # group.id = test - consumer - group
    # {topic: 1,topic: 0,topic: 2}) 是主题在那些kafka节点上有partition， partition是并发消费的基本单元了
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "test-consumer-group", {topic: 1, topic: 0, topic: 2})

    #
    lines = kvs.map(lambda x: x[1])
    # counts = lines.flatMap(run_line)  # 结果被按字符拆开
    counts = lines.map(run_line)
    counts = counts.map(modify_line)
    counts = counts.map(industry_line)

    counts.foreachRDD(lambda rdd: rdd.foreachPartition(out_to_kafka))  # 立刻执行算子

    # counts.saveAsTextFiles("file:///tmp/kafka")
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()


