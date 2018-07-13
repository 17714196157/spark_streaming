import os
import pandas as pd
import yaml
import time
BASE_PATH = os.path.dirname(os.path.dirname(__file__))
# print(BASE_PATH)
config_path = os.path.join(BASE_PATH, 'config')

INPUT_PATH = os.path.join(BASE_PATH, 'input')
OUTPUT_PATH = os.path.join(BASE_PATH, 'output')


with open(os.path.join(config_path, "setting.yaml"), mode='r', encoding='utf-8') as f:
    setting_yml = yaml.load(f)
    print(setting_yml)
    websource = setting_yml['websource']  # 流式数据清理，配置是那个爬虫页面
    dbconfig = setting_yml['dbconfig']   # 已知爬虫来源数据库信息列表
    kafka_in_topic = str(setting_yml['kafka_in_topic']).encode(encoding='utf-8')  # 字符串转成 bytes
    kafka_out_topic = str(setting_yml['kafka_out_topic']).encode(encoding='utf-8')  # 字符串转成 bytes
    WEB_SOURCE_LIST = setting_yml['WEB_SOURCE_LIST']  # 单独运行 mongodb_Data_cleaning_to_csv生成csv文件,需要配置需要做那些爬虫数据库
    savedb = dbconfig['savedb']
    gt_insert_time = setting_yml['gt_insert_time']  # 读取字符串表示的时间戳
    gt_insert_time = int(time.mktime(time.strptime(gt_insert_time, "%Y-%m-%dT%H:%M:%SZ")))   # 转换成多少秒
    lt_insert_time = setting_yml['lt_insert_time']
    lt_insert_time = int(time.mktime(time.strptime(lt_insert_time, "%Y-%m-%dT%H:%M:%SZ")))


with open(os.path.join(config_path, "province.yaml"), mode='r', encoding='utf-8') as f:
    province_map_city = yaml.load(f)   # 省份 下属那些城市多的 字典


# 读取每个网页字段与标准字段映射关系 file_map.xlsx
if "df" not in vars():
    # 将yaml文件数据合成为一个DF表格，方便对接后面的程序
    with open(os.path.join(config_path, "file_map.yaml"), mode='r', encoding='utf-8') as f:
        res_file_map = yaml.load(f)
        df_file_map = pd.DataFrame(res_file_map)
        # print(df_file_map)
        # print(df_file_map.index, df_file_map.columns)
    df = df_file_map
    for web_name in WEB_SOURCE_LIST:
        with open(os.path.join(config_path, "{}_map.yaml".format(web_name)), mode='r', encoding='utf-8') as f:
            res_map = yaml.load(f)
            df_map = pd.DataFrame(res_map)
            df_map.columns = [web_name + "_" + x for x in df_map.columns]
            df = df.join(df_map, how='outer')
    df['hbase字段名'] = df.index.tolist()
    df.index = range(0, df.shape[0])
    df['default'] = df['default'].astype("str")
    # input(df)
    # df = pd.read_excel(os.path.join(config_path, 'file_map.xlsx'), encoding='utf-8', eindex_col=False)
    # df['default'] = df['default'].astype("str")
    column_name_list = df['hbase字段名'].values.tolist()
    column_name_list.insert(0, "ID")
    column_name_list.append("WEB_SOURCE")
    print(column_name_list)

    REGISTER_TIME_ENUM = {'1': [-1, 2018],
                          '2': [2017, 2013],
                          '3': [2012, 2008],
                          '4': [2007, 2003],
                          '5': [2002, -1],
                          }

    REGISTER_CAPITAL_ENUM = {'1': [100, 0],
                          '2': [200, 100],
                          '3': [500, 200],
                          '4': [1000, 500],
                          '5': [-1, 1000],
                          }

    zk_Quorum = "192.168.1.70:2181"
    # kafka_Quorum = "192.168.1.45:9092,192.168.1.70:9092,192.168.1.171:9092"
    kafka_Quorum = "192.168.1.45:9092"
    df.to_excel("file_map.xlsx",index=False)
