from pykafka import KafkaClient
import json
import re
import numpy as np
import pandas as pd
from code_src import mongodb_Data_cleaning_to_csv
from code_src import Industry_keyword_interactive
from config.setting import kafka_Quorum
from config.setting import websource
from config.setting import kafka_out_topic
from config.setting import savedb
from pymongo import MongoClient
from code_src import globalvar
from config.setting import REGISTER_TIME_ENUM
from config.setting import REGISTER_CAPITAL_ENUM
from config.setting import df
from config.setting import column_name_list
import mongoengine
from code_src.models import Company

if "industry_object" not in vars():
    industry_object = Industry_keyword_interactive.industry_classify()

# REGISTER_TIME_ENUM = globalvar.get_value("REGISTER_TIME_ENUM_broadcast").value
# REGISTER_CAPITAL_ENUM = globalvar.get_value("REGISTER_CAPITAL_ENUM_broadcast").value
# df = globalvar.get_value("df_broadcast").value
# column_name_list = globalvar.get_value("column_name_list_broadcast").value
index_NAME = column_name_list.index('NAME')
index_INDUSTRY = column_name_list.index('INDUSTRY')


def run_line(iline, table_name=websource):
    """
    获取mongdb 数据，按照字段映射关系，做RDD之间的转换
    :param iline:
    :param table_name:
    :return:
    """
    if iline.strip() == "":
        return ""

    # print(str(iline))
    try:
        iline = json.loads(iline)
        # iline.pop('_id')
    except Exception as e:
        # print("ERROR:", str(e))
        return ""

    default_value_list = df['default'].values.tolist()
    Data_cleaning_regular_list = df['{}_Data_cleaning_regular'.format(table_name)].values.tolist()  # 数据清理的正则表达式
    hbase_name_index = df.loc[(df['hbase字段名'] == 'NAME')].index[0]  # NAME不能为空
    hbase_tel_index = df.loc[(df['hbase字段名'] == 'TEL')].index[0]  # tel不能为空

    node_list = default_value_list[:]  # 字段值，按顺序保存在一个list中,此处应该是复制一个list对象，而不是指定引用

    for order_n in df.index:
        # hbase_field_name = df.at[order_n, 'hbase字段名']
        key = df.at[order_n, '{}_fieldname'.format(table_name)]
        if key is np.nan:
            continue
        value = iline.get(key, None)

        if value == None:
            continue

        # node_list_old[order_n] = str(value)  # 保存字段的原始值
        regular_value = df.at[order_n, '{}_regular'.format(table_name)]
        default_value = default_value_list[order_n]
        value = mongodb_Data_cleaning_to_csv.standard_value(value, regular_value, Data_cleaning_regular_list[order_n], default_value)
        node_list[order_n] = value

    if node_list[hbase_name_index] != "未知" and node_list[hbase_name_index] != "" and \
            node_list[hbase_tel_index] != "未知" and node_list[hbase_tel_index] != "":
        node_list.insert(0, str(1))  # 第一个字段是ID
        node_list.append(table_name)  # 最后一个字段是web_source
        str_csv = ','.join(node_list) + '\n'
        return str_csv
    else:
        return ""


def modify_line(iline):
    """
    输入RDD
    每行数据 清理过的数据， 做数据枚举值、 数据转换等操作数
    输出RDD
    :param filepath_list: 输入文件路径列表
    :param outfilepath: 输出文件路径
    :return:
    """
    index_REGISTER_TIME = column_name_list.index('REGISTER_TIME')
    index_REGISTER_CAPITAL = column_name_list.index('REGISTER_CAPITAL')
    index_REG_TIME = column_name_list.index('REG_TIME')
    index_REG_CAPITAL = column_name_list.index('REG_CAPITAL')
    index_NAME = column_name_list.index('NAME')
    index_TEL = column_name_list.index('TEL')

    if iline.strip() == "":
        return ""

    field_value_list = iline.split(',')
    field_value_list = [x.strip() for x in field_value_list]

    if field_value_list[index_NAME] == "未披露":
        return ""

    if field_value_list[index_TEL] == "未披露":
        return ""

    try:
        field_value_list[index_REGISTER_TIME] = re.sub("[\s]+", "", field_value_list[index_REGISTER_TIME])
        value = field_value_list[index_REGISTER_TIME]
        if value != "未知" and value != "":
            field_value_list[index_REG_TIME] = mongodb_Data_cleaning_to_csv.enumerate_valueof_enum(value, REGISTER_TIME_ENUM)
            field_value_list[index_REGISTER_TIME] = field_value_list[index_REGISTER_TIME] + "年"

        field_value_list[index_REGISTER_CAPITAL] = re.sub("[\s]+", "", field_value_list[index_REGISTER_CAPITAL])
        value = field_value_list[index_REGISTER_CAPITAL]
        if value != "未知" and value != "":
            field_value_list[index_REG_CAPITAL] = mongodb_Data_cleaning_to_csv.enumerate_valueof_enum(value, REGISTER_CAPITAL_ENUM)
            field_value_list[index_REGISTER_CAPITAL] = field_value_list[index_REGISTER_CAPITAL] + "万人民币"
        return ','.join(field_value_list)

    except Exception as e:
        return ""


def out_to_kafka(iter):
    """
    RDD 输出到kafka队列中
    每个Partition 执行一次 ，建立一次链接
    :param iter:
    :return:
    """
    database_url = r'http://localhost:8765/'
    client = KafkaClient(hosts=kafka_Quorum)
    # 链接mongodb数据库
    mongodb_ip = savedb['mongodb_ip']
    dbname = savedb['dbname']
    tablename = savedb['tablename']
    conn_mongodb = mongoengine.connect(dbname, host=mongodb_ip, port=27017)
    all_n = Company.objects().count()


    mongo_cli = MongoClient(mongodb_ip, 27017)
    db = mongo_cli[dbname]  # 选择数据库
    col = db[tablename]  # 选择集合

    # conn = phoenixdb.connect(database_url, max_retries=1, autocommit=True)
    # cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
    # res_db = cursor.execute("select ID from company  order by id desc limit 1").fetchone()
    # for key, value in res_db.items():
    #     all_n = value
    # 生产者
    topic = client.topics[kafka_out_topic]
    with topic.get_sync_producer() as producer:
        indexn = all_n + 1
        data_json_list = []
        for record in iter:
            if record.strip() == "":
                continue
            record_list = record.split(',')
            record_list[0] = str(indexn)
            producer.produce(str(','.join(record_list)).encode(encoding='utf-8'))

            data_json_list.append(record_list)

            # 输出到mongdb那个表已经写死，应该用的是ORM，必须事先定义好表的字段
            # company_node = Company(
            #     **data_json
            # )
            #
            # company_node.save()
            indexn += 1

        else:
            if len(data_json_list) != 0:
                # json 化数据 插入mongdb
                data_pd = pd.DataFrame(data_json_list, columns=column_name_list)
                data_json = data_pd.to_dict('records')
                col.insert(data_json)
            pass
            # data_json_list = []


def industry_line(iline):
    """
    每行数据 行业分类 设置
    :param iline:
    :return:
    """
    if iline.strip() == "":
        return ""

    index_feild = index_INDUSTRY
    index_industry = index_INDUSTRY
    index_name = index_NAME
    iline_list = iline.split(',')
    industry_tmp = iline_list[index_feild]

    industry_key = Industry_keyword_interactive.industry_to_stand(iline_list[index_name], industry_object)
    if industry_key == "生产/加工/制造":
        industry_key = Industry_keyword_interactive.industry_to_stand(industry_tmp, industry_object)

    # print("industry_key:", industry_key)
    if industry_key is not None:
        iline_list[index_industry] = industry_key
        data = ','.join(iline_list)
        return data
    else:
        return ""

###########################################


def run_line_qq(iline_list, table_name=websource):
    """
    获取mongdb 数据，按照字段映射关系，做RDD之间的转换
    :param iline:
    :param table_name:
    :return:
    """
    result_list = []
    for iline in iline_list:
        if iline.strip() == "":
            continue
        try:
            iline = json.loads(iline)
        except Exception as e:
            continue

        default_value_list = df['default'].values.tolist()
        Data_cleaning_regular_list = df['{}_Data_cleaning_regular'.format(table_name)].values.tolist()  # 数据清理的正则表达式
        hbase_name_index = df.loc[(df['hbase字段名'] == 'NAME')].index[0]  # NAME不能为空
        hbase_tel_index = df.loc[(df['hbase字段名'] == 'TEL')].index[0]  # tel不能为空

        node_list = default_value_list[:]  # 字段值，按顺序保存在一个list中,此处应该是复制一个list对象，而不是指定引用

        for order_n in df.index:
            # hbase_field_name = df.at[order_n, 'hbase字段名']
            key = df.at[order_n, '{}_fieldname'.format(table_name)]
            if key is np.nan:
                continue
            value = iline.get(key, None)

            if value == None:
                continue

            # node_list_old[order_n] = str(value)  # 保存字段的原始值
            regular_value = df.at[order_n, '{}_regular'.format(table_name)]
            default_value = default_value_list[order_n]
            value = mongodb_Data_cleaning_to_csv.standard_value(value, regular_value, Data_cleaning_regular_list[order_n], default_value)
            node_list[order_n] = value

        if node_list[hbase_name_index] != "未知" and node_list[hbase_name_index] != "" and \
                node_list[hbase_tel_index] != "未知" and node_list[hbase_tel_index] != "":
            node_list.insert(0, str(1))  # 第一个字段是ID
            node_list.append(table_name)  # 最后一个字段是web_source
            str_csv = ','.join(node_list) + '\n'
            result_list.append(str_csv)
        else:
            return ""
    return iter(result_list)