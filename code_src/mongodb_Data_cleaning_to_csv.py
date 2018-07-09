import numpy as np
import pandas as pd
import re
import time
from pymongo import MongoClient
from config.setting import gt_insert_time
from config.setting import lt_insert_time

from datetime import datetime
from bson.objectid import ObjectId
import os
import sys
gt_insert_time_mongdb_objectID = hex(gt_insert_time)[2:] +'0000000000000000' # 转换成16进制的字符串，再加补齐16个0
lt_insert_time_mongdb_objectID = hex(lt_insert_time)[2:] +'0000000000000000' # 转换成16进制的字符串，再加补齐16个0


sys.path.append("/home/spark_streaming")
################################################################################
# 爬虫数据清理整合脚本
# 根据配置文件 file_map.xlsx 中配置的字段映射 ，将mongdb中保存的网页爬虫信息 清洗与整合
################################################################################
"""
爬虫数据清理整合脚本
mongodb数据导出并清理
"""
from config.setting import df
from config.setting import REGISTER_TIME_ENUM
from config.setting import REGISTER_CAPITAL_ENUM
from config.setting import column_name_list
from config.setting import BASE_PATH, INPUT_PATH, OUTPUT_PATH
from config.setting import WEB_SOURCE_LIST, dbconfig
from config.setting import province_map_city


def strQ2B(ustring):
    """全角转半角"""
    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 12288:                              #全角空格直接转换
            inside_code = 32
        elif (inside_code >= 65281 and inside_code <= 65374): #全角字符（除空格）根据关系转化
            inside_code -= 65248

        rstring += chr(inside_code)
    return rstring

def enumerate_valueof_enum(node_value, REGISTER_ENUM_MAP):
    """
    对应int字段值变成枚举值，
    :param node_value: 可以是 表示整数的 str 或者 int
    :param REGISTER_ENUM_MAP:  字段枚举化规则
    :return:
    """
    if isinstance(node_value, str) and node_value.isdigit:
        node_value = int(node_value)
    elif isinstance(node_value, int):
        pass
    else:
        node_ENUM = '0'
        return node_ENUM

    for enum in REGISTER_ENUM_MAP:
        end = REGISTER_ENUM_MAP[enum][0]
        start = REGISTER_ENUM_MAP[enum][1]
        # print(start, end)
        if start != -1 and end != -1:
            if node_value in range(start, end+1):
                node_ENUM = enum
                return node_ENUM
        if start == -1 and node_value <= end:
            node_ENUM = enum
            return node_ENUM
        if end == -1 and node_value >= start:
            node_ENUM = enum
            return node_ENUM


class OutSql(object):
    """
    根据字段映射配置文件file_map.xlsx， 输出创建表的sql语句
    """
    @staticmethod
    def out_create_table_SQL(df):
        list_hbase_filename = []
        for index in df.index:
            hbase_filename = "{f}.{name}".format(f=df.at[index, 'hbase列族'], name=df.at[index, 'hbase字段名'])\
                if df.at[index, 'hbase列族'] != "rowkey" else df.at[index, 'hbase字段名']
            list_hbase_filename.append(hbase_filename)

        feild_str = "ID BIGINT not null ," + " VARCHAR(128), ".join(list_hbase_filename) + " VARCHAR(64),A.web_source VARCHAR(64),"
        PK_str = "CONSTRAINT PK PRIMARY KEY (ID,{}) ".format(",".join(df.loc[(df['hbase列族']=='rowkey')]['hbase字段名'].values.tolist()))
        str_create_SQL = '''CREATE TABLE IF NOT EXISTS COMPANY ({feild_str} {PK_str}) SALT_BUCKETS = 8,COMPRESSION='GZ';'''.format(feild_str=feild_str,PK_str=PK_str)
        print(str_create_SQL)
        return str_create_SQL

    @staticmethod
    def out_upset_SQL(df,feild_value_list):
        list_hbase_filename = []
        for index in df.index:
            hbase_filename = "{f}.{name}".format(f=df.at[index, 'hbase列族'], name=df.at[index, 'hbase字段名'])\
                if df.at[index, 'hbase列族'] != "rowkey" else df.at[index, 'hbase字段名']
            list_hbase_filename.append(hbase_filename)

        feild_str = "ID ," + ",".join(list_hbase_filename) + ",A.web_source"
        feild_value_str ='\''+ "','".join(feild_value_list) + '\''
        str_upset_SQL = '''UPSERT INTO COMPANY ({feild_str}) VALUES ({feild_value_str});'''.format(feild_str=feild_str,feild_value_str=feild_value_str)

        # 屏蔽输入的ID 使用 mySEQ 自增序列
        # feild_value_str = ",".join(feild_value_list[1:])
        # str_upset_SQL = '''UPSERT INTO COMPANY ({feild_str}) VALUES (NEXT VALUE FOR mySEQ,{feild_value_str});'''.format(feild_str=feild_str,feild_value_str=feild_value_str)
        return str_upset_SQL

    @staticmethod
    def out_create_index_SQL(df):
        for index_table_name in df.loc[(df['index_table']==True)]['hbase字段名']:
            str_create_index_SQL = '''CREATE INDEX IF NOT EXISTS  {index_table_name}_INDEX ON COMPANY ({index_table_name}) INCLUDE(ID)  SALT_BUCKETS = 4;'''.format(index_table_name=index_table_name)
            print(str_create_index_SQL)


    @staticmethod
    def out_create_SQL(df):
        print("#######create hbase 表结构sql语句如下:########")
        OutSql.out_create_table_SQL(df)
        OutSql.out_create_index_SQL(df)
        print("#######################################")


def standard_value(value, regular_value, Data_cleaning_regular, default_value):
    """
    对字段值 做数据清理
    :param value: 原数据值
    :param regular_value: 正则表达式获取需要的字符串
    :param Data_cleaning_regular:  对获取到的字段值做数据清理
    :param default_value: 如何数据为空，赋值默认值
    :return:
    """
    result_value_list = []
    try:                    #如果value不是字符串，说明还没有找到对应字段值
        value = value.strip()
        value = value.replace(",", "")
        value = value.replace("，", "")
        value = value.replace("\'", "")
        value = value.replace("\"", "")
        # value = value.replace(" ", "")
        value = re.sub("[\s]+", "", value)

        if len(value) > 127:
            value = value[:126]
        result_value_list.append(value)
    except Exception as e:
        # print(str(value))
        # regular = regular_value + '''[，。！、……《》（）【】：；“‘”’？￥,\.\?:;' "\(\)]+(\w+)'''
        regular = regular_value + '''[，。！、……《》（）【】：；“‘”’？￥,\.\?:;' "\(\)]+([^\'^\"^,^;]+)'''
        result_value_list = re.findall(regular, str(value))

    if pd.notnull(Data_cleaning_regular):
        result_value_re_list = []
        for result_value in result_value_list:
            result_value = result_value.replace("O", "0").replace("S", "5").replace(" ", "").replace("Y", "7")    # 电话号码中存在和数字很像的字母，作矫正
            result_value = result_value.replace("-", "")
            result_value = strQ2B(result_value)
            res = re.findall(Data_cleaning_regular, result_value)
            if len(res) != 0:
                # print(res)
                # 正则匹配有可能得到的元素还是一个元组类型
                if isinstance(res[-1], str):
                    result_value_re_list.append(res[-1])
                else:
                    list_res = [x for x in res[-1] if x.strip() != ""]
                    result_value_re_list.append(list_res[-1])
            else:
                # 观察正则匹配识别的字段,原始数据什么样子，正则匹配表达式是什么样子
                # if '|' in Data_cleaning_regular and result_value != "暂无信息" and result_value !="未提供" and result_value !="未披露" and result_value !="未知":
                #     print(result_value, Data_cleaning_regular, res)   # 检查电话号码 异常都是什么样式
                #     input("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
                #     pass
                result_value_re_list = [str(default_value)]
    else:
        if len(result_value_list) != 0:
            result_value_re_list = result_value_list
        else:
            result_value_re_list = [str(default_value)]

    result_value_re_list_str = '|'.join(result_value_re_list)
    if len(result_value_re_list_str) > 126:
        result_value_re_list_str = result_value_re_list_str[:126]
    return result_value_re_list_str


def run(mongodb_ip='192.168.1.45', db_name="shunqi", table_name="shunqi", start_rowkey_id=0):
    """
    主函数
    从mongdb 获取爬虫数据，并进行数据处理，输出到文件中，文件名就是mongdb的表名
    :param mongodb_ip:
    :param db_name:
    :param table_name:
    :param start_rowkey_id:
    :return:
    """
    outsql_object = OutSql()
    outsql_object.out_create_SQL(df)

    # 链接mongodb数据库
    mongo_cli = MongoClient(mongodb_ip, 27017)
    # 选择数据库
    db = mongo_cli[db_name]
    # 选择集合
    col = db[table_name]

    t1 = time.time()
    with open(OUTPUT_PATH + os.sep + table_name+".csv", mode='w', encoding="utf-8") as f_all_csv, \
            open(OUTPUT_PATH + os.sep + table_name+"_bad被舍弃的数据.csv", mode='w', encoding="utf-8") as f_all_bad_csv:

        # mongodb_find_result = col.find()


        index = start_rowkey_id
        data_csv = ""
        default_value_list = df['default'].values.tolist()
        Data_cleaning_regular_list = df['{}_Data_cleaning_regular'.format(table_name)].values.tolist()  # 数据清理的正则表达式
        hbase_name_index = df.loc[(df['hbase字段名'] == 'NAME')].index[0]  # NAME不能为空
        hbase_tel_index = df.loc[(df['hbase字段名'] == 'TEL')].index[0]  # tel不能为空
        hbase_PROVINCE_index = df.loc[(df['hbase字段名'] == 'PROVINCE')].index[0]   # PROVINCE不能为空
        hbase_CITY_index = df.loc[(df['hbase字段名'] == 'CITY')].index[0]   # CITY 不能为空
        # print("Data_cleaning_regular_list:", Data_cleaning_regular_list)
        for item in mongodb_find_result:
            item.pop('_id')
            # item = {'shareholder_list': [], 'manager': '陈麦', 'tel': '0186-88990752', 'establish': '2004年04月23日', 'company_name': '深圳驰名回收旧货有限公司', 'fixed_tel': '0755-22929848', 'status': '在业', 'province_name': '广东', 'city_name': '深圳', 'area_name': '罗湖区', 'change_log_list': [], 'register_capital': '100 (万元)', 'member_list': [], 'company_url': 'http://www.11467.com/shenzhen/co/728562.htm', 'addr': '深圳 深圳市罗湖区清水河5路9栋3楼', 'company_classe': '办公家具公司', 'code': '518023', 'email': '1740966757@qq.com'}

            node_list = default_value_list[:]   # 字段值，按顺序保存在一个list中,此处应该是复制一个list对象，而不是指定引用
            node_list_old = default_value_list[:]
            try:
            # if True:
                for order_n in df.index:
                    # hbase_field_name = df.at[order_n, 'hbase字段名']
                    key = df.at[order_n, '{}_fieldname'.format(table_name)]
                    if key is np.nan:
                        continue
                    value = item.get(key, None)

                    if value == None:
                        continue

                    node_list_old[order_n] = str(value)  # 保存字段的原始值


                    regular_value = df.at[order_n, '{}_regular'.format(table_name)]
                    default_value = default_value_list[order_n]
                    value = standard_value(value, regular_value, Data_cleaning_regular_list[order_n], default_value)

                    node_list[order_n] = value

                 # 省份为空 ，就根据城市 给判断一个省份
                # input("!!!!!!!!!!!!!!!!!!!!!!!" + node_list[hbase_PROVINCE_index])
                if node_list[hbase_PROVINCE_index] == "未知" or node_list[hbase_PROVINCE_index] == "" or \
                        node_list[hbase_PROVINCE_index] == "中国":
                    for province in province_map_city.keys():
                        if node_list[hbase_CITY_index] in province_map_city[province]:
                            node_list[hbase_PROVINCE_index] = province

            except Exception as e:
                print("ERROR item=", item)
                print("ERROR key=", key)
                print("ERROR e=", e)
                continue

            # 输出被认为tel不合法的数据的tel字段到文件中
            if node_list[hbase_tel_index] == "未知" or node_list[hbase_tel_index] == "" or node_list[hbase_name_index] == "未知" or node_list[hbase_name_index] == "":
                # # 观察被舍弃的数据中 tel 不为空的情况，原始数据是什么样子
                # if node_list[hbase_tel_index] != "未知" and node_list[hbase_tel_index] != "":
                #     print(item)
                #     input("bad数据 {tel},{name}".format(tel=node_list[hbase_tel_index], name=node_list[hbase_name_index]))
                str_bad_csv = "bad数据 " + node_list_old[hbase_tel_index] + " " + node_list_old[hbase_name_index]+" 原数据： " + ','.join(node_list_old) + '\n'
                f_all_bad_csv.write(str_bad_csv)

            elif node_list[hbase_name_index] != "未知" and node_list[hbase_name_index] != "" and \
                    node_list[hbase_tel_index] != "未知" and node_list[hbase_tel_index] != "":
                index += 1
                node_list.insert(0, str(index))   # 第一个字段是ID
                node_list.append(table_name)   # 最后一个字段是web_source
                str_csv = ','.join(node_list) + '\n'
                data_csv += str_csv

                t2 = time.time()
                if divmod(index, 10000)[1] == 0:
                    print("10000 cost time:", t2-t1)
                    str_sql = outsql_object.out_upset_SQL(df, node_list) + '\n'
                    print(item)
                    print(str_sql)
                    print(str_csv)
                    f_all_csv.write(data_csv)
                    data_csv = ""
                    t1 = time.time()

                    ##################
                    # input(node_list)
            else:
                pass

        else:
            f_all_csv.write(data_csv)
    return index


def modify(filepath_list, outfilepath):
    """
    清理过的数据， 做数据枚举值、 数据转换等操作数，并合并csv文件
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
    index_INDUSTRY = column_name_list.index('INDUSTRY')

    with open(outfilepath, mode='w', encoding='utf-8') as fw:
        for filepath in filepath_list:
            with open(filepath, mode="r", encoding='utf-8') as fr:
                index = 0
                for iline in fr:
                    if iline.strip() == "":
                        continue
                    field_value_list = iline.split(',')
                    field_value_list = [x.strip() for x in field_value_list]

                    if field_value_list[index_NAME] == "未披露":
                        continue

                    if field_value_list[index_TEL] == "未披露":
                        continue

                    try:
                        field_value_list[index_REGISTER_TIME] = re.sub("[\s]+", "", field_value_list[index_REGISTER_TIME])
                        value = field_value_list[index_REGISTER_TIME]
                        if value != "未知" and value != "":
                            field_value_list[index_REG_TIME] = enumerate_valueof_enum(value, REGISTER_TIME_ENUM)
                            field_value_list[index_REGISTER_TIME] = field_value_list[index_REGISTER_TIME] + "年"

                        field_value_list[index_REGISTER_CAPITAL] = re.sub("[\s]+", "", field_value_list[index_REGISTER_CAPITAL])
                        value = field_value_list[index_REGISTER_CAPITAL]
                        if value != "未知" and value != "":
                            field_value_list[index_REG_CAPITAL] = enumerate_valueof_enum(value, REGISTER_CAPITAL_ENUM)
                            field_value_list[index_REGISTER_CAPITAL] = field_value_list[index_REGISTER_CAPITAL] + "万人民币"

                    except Exception as e:
                        print(iline)
                        print(field_value_list)
                        print(index)
                        raise KeyError(str(e))
                    iline_new = ','.join(field_value_list)+"\n"
                    fw.write(iline_new)
                    index += 1


if __name__ == "__main__":
    endindex = 0
    filepath_list = []
    outfilepath = OUTPUT_PATH + os.sep + r"all_new.csv"

    for webname in WEB_SOURCE_LIST:
        print(webname, " begin get data")
        mongodb_ip = dbconfig[webname]['mongodb_ip']
        dbname = dbconfig[webname]['dbname']
        tablename = dbconfig[webname]['tablename']
        filepath_list.append(OUTPUT_PATH + os.sep + "{}.csv".format(webname))
        print("mongodb_ip：", mongodb_ip)

        t1all = time.time()
        endindex = run(mongodb_ip=mongodb_ip, db_name=dbname, table_name=tablename, start_rowkey_id=endindex)
        t2all = time.time()
        print(endindex, "{} total cost time:".format(webname), t2all - t1all)

    # endindex = 0
    # t1all = time.time()
    # endindex = run(mongodb_ip='192.168.1.45', db_name="shunqi", table_name="shunqi", start_rowkey_id=endindex)
    # t2all = time.time()
    # print(endindex, "shunqi total cost time:", t2all-t1all)
    #
    # t1all = time.time()
    # endindex = run(mongodb_ip='192.168.1.166', db_name="tianyan", table_name="tianyan", start_rowkey_id=endindex)
    # t2all = time.time()
    # print(endindex, "tianyan total cost time:", t2all-t1all)


    modify(filepath_list, outfilepath)


'''
hbase字段名	hbase列族	index_table	shunqi_fieldname	shunqi_regular	shunqi_Data_cleaning_regular	tianyan_fieldname	tianyan_regular	default	tianyan_Data_cleaning_regular	tips
PROVINCE	rowkey	FALSE	province_name			province		中国		
CITY	rowkey	FALSE	city_name			city		其他城市		
REG_CAPITAL	rowkey	FALSE	reg_capital					0		注册规模枚举值
REG_TIME	A	TRUE	reg_time					0		注册年份区间枚举值
URBAN_AREA	A	FALSE	area_name					其他区		
NAME	A	TRUE	company_name			company_web_top	company_name	未知		
TEL	A	TRUE	tel		(0\d{2,3}-\d{7,8}(-\d{3,5}){0,1})|(1[35847]\d{9})	company_web_top	tel	未知	(0\d{2,3}-\d{7,8}(-\d{3,5}){0,1})|(1[35847]\d{9})	
INDUSTRY	A	TRUE	company_classe			basic_info	industry	未知		
LAGEL_PERSON	A	FALSE	shareholder_list	shareholder_name		basic_info	legal_person	未知		
REGISTER_CAPITAL	A	FALSE	register_capital		(\d+)[.]*.*万	basic_info	register_capital	未知	(\d+)[.]*.*万	
REGISTER_TIME	A	FALSE	establish		(\d{4}).*年	basic_info	register_time	未知	(\d{4}).*年	
ADDRESS	A	FALSE	addr			basic_info	register_addr	未知		
EMAIL	A	FALSE	email			company_web_top	email	未知		
BUSINESS_LICENSE	A	FALSE	business_license			basic_info	organization_code	未知		
STATUS	A	FALSE	status			basic_info	company_status	未知			
'''