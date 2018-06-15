"""
从爬虫页面的数据库获取数据，并写入到kafka中
"""
import numpy as np
import pandas as pd
import re
import time
import json
from pykafka import KafkaClient
from pymongo import MongoClient
from config.setting import websource
from config.setting import dbconfig
from config.setting import kafka_Quorum
from config.setting import kafka_in_topic

db_info = dbconfig[websource]
mongodb_ip = db_info['mongodb_ip']
dbname = db_info['dbname']
tablename = db_info['tablename']


def product_mongdb_data_to_kafka(mongodb_ip=mongodb_ip, db_name=dbname, table_name=tablename, start_rowkey_id=0):
    """
    主函数  用于测试流式的数据清理脚本，模拟输入添加到kafka消息队列中
    从mongdb 获取爬虫数据，并写入到kafka队列中
    :param mongodb_ip:
    :param db_name:
    :param table_name:
    :param start_rowkey_id:
    :return:
    """

    #
    client = KafkaClient(hosts=kafka_Quorum)
    topic = client.topics[kafka_in_topic]
    # 链接mongodb数据库
    mongo_cli = MongoClient(mongodb_ip, 27017)
    # 选择数据库
    db = mongo_cli[db_name]
    # 选择集合
    col = db[table_name]
    mongodb_find_result = col.find()
    with topic.get_sync_producer() as producer:
        for index, item in enumerate(mongodb_find_result):
            item.pop('_id')
            # 生产者
            data = json.dumps(item).encode(encoding='utf-8')
            producer.produce(data)
            if divmod(index, 10)[1] == 0:
                time.sleep(10)


if __name__ == "__main__":
    product_mongdb_data_to_kafka()


