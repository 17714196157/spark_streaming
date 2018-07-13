import os
import sys
ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_PATH)
print(sys.path)
from code_src.mongodb_Data_cleaning_to_csv import run as mongodb_out_run
from code_src.mongodb_Data_cleaning_to_csv import modify as mongodb_out_merge
from code_src.Industry_keyword_interactive import run as Industry_out_run
import shutil
from config.setting import WEB_SOURCE_LIST, dbconfig, savedb, column_name_list
from config.setting import OUTPUT_PATH
import time
import datetime
from pymongo import MongoClient
import pandas as pd
from bson.objectid import ObjectId


def timing_mongdb_export_run():
    """
    定时任务 ，导出爬虫库的数据 ，导入的指定mongdb
    :return:
    """
    # 链接mongodb数据库
    mongodb_ip = savedb['mongodb_ip']
    dbname = savedb['dbname']
    tablename = savedb['tablename']

    mongo_cli = MongoClient(mongodb_ip, 27017)
    db = mongo_cli[dbname]  # 选择数据库
    col = db[tablename]  # 选择集合
    all_n = col.count()

    endindex = int(all_n) + 1

    filepath_list = []
    file_root_path = os.path.join(OUTPUT_PATH, 'Timing_mongdbdata_import')
    try:
        shutil.rmtree(file_root_path)
    except Exception as e:
        print("文件夹不操作，无法删除", e)
    time.sleep(3)
    os.makedirs(file_root_path, exist_ok=True)

    outfilepath = os.path.join(file_root_path, "all_new.csv")

    now_time = datetime.datetime.now()  # 获取当前时间
    yes_time = now_time + datetime.timedelta(days=-1)
    yes_time = yes_time.strftime("%Y-%m-%dT00:00:00Z")  # 格式化输出
    now_time = now_time.strftime("%Y-%m-%dT00:00:01Z")  # 格式化输出

    print("yes_time", yes_time)
    print("now_time", now_time)

    now_time = int(time.mktime(time.strptime(now_time, "%Y-%m-%dT%H:%M:%SZ")))
    yes_time = int(time.mktime(time.strptime(yes_time, "%Y-%m-%dT%H:%M:%SZ")))

    gt_insert_time_mongdb_objectID = hex(yes_time)[2:] + '0000000000000000'  # 转换成16进制的字符串，再加补齐16个0
    lt_insert_time_mongdb_objectID = hex(now_time)[2:] + '0000000000000000'  # 转换成16进制的字符串，再加补齐16个0

    # print("gt_insert_time_mongdb_objectID", gt_insert_time_mongdb_objectID)

    WEB_SOURCE_LIST = ['meituan', 'dzdp']

    # WEB_SOURCE_LIST = ['dzdp']
    for webname in WEB_SOURCE_LIST:
        print(webname, " begin get data")
        mongodb_ip = dbconfig[webname]['mongodb_ip']
        dbname = dbconfig[webname]['dbname']
        tablename = dbconfig[webname]['tablename']
        filepath_list.append(os.path.join(file_root_path, "{}.csv".format(webname)))
        print("mongodb_ip：", mongodb_ip)

        t1all = time.time()
        endindex = mongodb_out_run(mongodb_ip=mongodb_ip, db_name=dbname, table_name=tablename, start_rowkey_id=endindex,
                                   gt_insert_time_mongdb_objectID=gt_insert_time_mongdb_objectID,
                                   lt_insert_time_mongdb_objectID=lt_insert_time_mongdb_objectID, filerootpath=file_root_path)
        t2all = time.time()
        print(endindex, "{} total cost time:".format(webname), t2all - t1all)

    mongodb_out_merge(filepath_list, outfilepath)
    Industry_out_run(filerootpath=file_root_path)

    with open(os.path.join(file_root_path, 'fgood_all_new.csv'), mode='r', encoding="utf-8") as f:
        data_pd = pd.read_csv(f, sep=',', header=None)  # skiprows 是需要忽略的行数从第1行开始读，header=None 不设置列索引
        data_pd.columns = column_name_list  # 自定义列索引
        data_pd['TEL'] = data_pd['TEL'].astype("str")
        print("数据量：", data_pd.shape)
        data_pd['WEB_SOURCE'] = data_pd['WEB_SOURCE'].apply(lambda x:x.strip())
        data_json = data_pd.to_dict('records')
        col.insert(data_json)

def list_alln_by_day():
    """
    列出每天的数据总量
    :return:
    """
    # 链接mongodb数据库
    mongodb_ip = savedb['mongodb_ip']
    dbname = savedb['dbname']
    tablename = savedb['tablename']

    mongo_cli = MongoClient(mongodb_ip, 27017)
    db = mongo_cli[dbname]  # 选择数据库
    col = db[tablename]  # 选择集合

    now_time = datetime.datetime.now()  # 获取当前时间
    # now_time = now_time.strftime("%Y-%m-%dT00:00:01Z")  # 格式化输出
    # now_time = int(time.mktime(time.strptime(now_time, "%Y-%m-%dT%H:%M:%SZ")))
    for i in range(0, -10, -1):
        # yes_time = now_time + datetime.timedelta(days=-1)
        yes_time = now_time + datetime.timedelta(days=i)
        yes_time_str = yes_time.strftime("%Y-%m-%dT23:59:59Z")  # 格式化输出
        yes_time = int(time.mktime(time.strptime(yes_time_str, "%Y-%m-%dT%H:%M:%SZ")))
        lt_insert_time_mongdb_objectID_day = hex(yes_time)[2:] + '0000000000000000'  # 转换成16进制的字符串，再加补齐16个0
        all_n = col.find({'_id': {
            '$lt': ObjectId(lt_insert_time_mongdb_objectID_day)}}).count()
        print(str(i) + " " + yes_time_str + " :" + str(all_n))


if __name__ == "__main__":
    timing_mongdb_export_run()
    # list_alln_by_day()
    pass
