import yaml
import jieba
import os
import json
import pandas as pd
from config.setting import config_path
from config.setting import OUTPUT_PATH
from config.setting import INPUT_PATH
from config.setting import column_name_list
# 存在如下行业分类
# str = '''IT/通信/电子/互联网 金融业 房地产/建筑业 法律 商业服务 医疗/健康 贸易/批发/零售/租赁业 生产/加工/制造 交通/运输/物流/仓储 服务业 体育/休闲/旅游/娱乐 能源/矿产/环保 政府/非盈利机构 媒体 教育 农/林/牧/渔 跨领域经营 其他'''
# for item in str.split(' '):
#     print(item+":")
#     for n in item.split("/"):
#         print(" {}: true".format(n))

################################################################################
# 行业分类_关键词列表人工获取
# 行业分页脚本， 根据配置文件industry.yaml中每个行业的关键词 去匹配行业类型
################################################################################

class industry_classify(object):
    """
    读取分词关键字对应行业分类的配置文件
    """
    def __init__(self, yaml_path=config_path + os.sep + 'industry.yaml'):
        stream = open(yaml_path, 'r', encoding='utf-8')
        self.industry_dict = yaml.load(stream)
        self.index = 0

        ignore_word_str = '''|
        公司
        有限
        责任
        政府
        ’
        ？
        ，
        、
        注册
        其他
        未分类
        分类
        未分
        (
        )
        '''.split('\n')
        ignore_word_list = [x.strip() for x in ignore_word_str if x.strip() != "" ]
        self.ignore_word = dict.fromkeys(ignore_word_list, True)
        self.industry_keys = sorted(self.industry_dict.keys())

    def insert(self, key, value):
        key = key.strip()
        value = value.strip()
        match_key_dict = self.industry_dict.get(key, None)
        if match_key_dict is not None:
            match_key_dict.update({value: True})

    def save(self):
        tmp_yaml_path = OUTPUT_PATH + os.sep +'industry'+str(self.index)+'.yaml'
        self.index += 1
        with open(tmp_yaml_path, mode='w', encoding='utf-8') as f:
            yaml.dump(self.industry_dict, f, default_flow_style=False, encoding='UTF-8', allow_unicode = True)

    def industry_judge(self, word):
        for key in self.industry_dict.keys():
            if word in self.industry_dict[key].keys():
                # print(word, " 命中分类 ", key)
                return key
        else:
            return None
        pass

    def interaction_industry_judge(self, word_list):
        """
        输入词列表 ，人工输入属于那个分类
        :param word_list:
        :return:
        """
        word_list = [x for x in word_list if x.strip() != ""]
        if len(word_list) == 0:
            return None

        print("该公司 分词结果列表：", word_list)
        print("存在一下分类，请选择你分类结果,")
        for keyn, keyname in enumerate(self.industry_keys):
            print(keyn, keyname, end=' ')
        print()

        for word in word_list:
            input_key = input("word:"+word+" 你要作为那个分类的关键字 不能作为关键字请回车").strip()

            if input_key == "save":
                self.save()
                input_key = input("word:" + word + " 你要作为那个分类的关键字 不能作为关键字请回车").strip()

            if input_key.isdigit():
                input_key = int(input_key)
                if input_key in range(0, keyn):
                    key_name = self.industry_keys[input_key]
                    self.insert(key_name, word)
                    return key_name
                else:
                    print("输入数据没有对应枚举项")
                    continue
        else:
            return None

    def interaction_industry_judge_continue(self, word_list):
        """
        输入词列表 ，人工输入属于那个分类
        :param word_list:
         :param  continue_flag 单个词匹配以后 ，其他词是否继续提示 手工 输入 匹配分类
        :return:
        """
        word_list = [x for x in word_list if x.strip() != ""]
        if len(word_list) == 0:
            return None

        # print("该公司 分词结果列表：", word_list)
        print("存在一下分类，请选择你分类结果,")
        for keyn, keyname in enumerate(self.industry_keys):
            print(keyn, keyname, end=' ')
        print()

        for index, word in enumerate(word_list):
            input_key = input(str(index)+" word:" + word + " 你要作为那个分类的关键字 不能作为关键字请回车").strip()

            if input_key == "save":
                self.save()
                input_key = input(str(index) + "word:" + word + " 你要作为那个分类的关键字 不能作为关键字请回车").strip()

            if input_key.isdigit():
                input_key = int(input_key)
                if input_key in range(0, keyn+1):
                    key_name = self.industry_keys[input_key]
                    self.insert(key_name, word)
                else:
                    print("输入数据 {} 没有对应枚举项".format(word))

            if divmod(index,10)[1] == 0:
                print("存在一下分类，请选择你分类结果,")
                for keyn, keyname in enumerate(self.industry_keys):
                    print(keyn, keyname, end=' ')
                print()
        else:
            self.save()
            return None


def industry_to_stand(industry_tmp, industry_object):
    """
    对行业分类 做枚举化
    :param industry_tmp:
    :param industry_object:
    :return:
    """
    ignore_word = industry_object.ignore_word
    seg_list = jieba.cut_for_search(industry_tmp)  # 搜索引擎模式
    seg_list = [x for x in seg_list if x not in ignore_word]
    for word in seg_list:
        key_industry = industry_object.industry_judge(word)
        if key_industry is not None:
            return key_industry
    else:
        # 手工设置 分类 与 对应关键词
        # key_industry = industry_object.interaction_industry_judge(seg_list)
        # return key_industry
        return "生产/加工/制造"



def run(startid=None, endid=None):
    """
    读取csv文件， 将对应行业字段取出，进行分词，将分词列表 与 行业分类关键词配置文件数据 做匹配
    匹配不到数据， 提示交互界面， 人工新增关键字匹配规则
    :param startid: csv文件读取的起始行
    :param endid: csv文件读取的终止行
    :return:
    """
    industry_object = industry_classify()

    with open(OUTPUT_PATH + os.sep + 'all_new.csv', mode='r', encoding='utf-8') as f, \
            open(OUTPUT_PATH + os.sep + 'fgood_all_new.csv', mode='w', encoding='utf-8') as fgood, \
            open(OUTPUT_PATH + os.sep + 'fbad_all_new.csv', mode='w', encoding='utf-8') as fbad:

        index_NAME = column_name_list.index('NAME')
        index_INDUSTRY = column_name_list.index('INDUSTRY')

        indexn = 1
        index_feild = index_INDUSTRY   # 需要修改的字段
        index_industry = index_INDUSTRY  # 行业字段
        index_name = index_NAME  # 企业名字段

        for iline in f:

            iline_list = iline.split(',')
            industry_tmp = iline_list[index_feild]
            # print(indexn, " industry_tmp:", industry_tmp)

            industry_key = industry_to_stand(iline_list[index_name], industry_object)
            if industry_key == "生产/加工/制造":
                industry_key = industry_to_stand(industry_tmp, industry_object)

            #print("industry_key:", industry_key)
            if industry_key is not None:
                iline_list[index_industry] = industry_key
                data = ','.join(iline_list)
                # print("fgood write:", data)
                fgood.write(data)
                fgood.flush()
            else:
                data = ','.join(iline_list)
                # print("fbad write:", data)
                fbad.write(data)
                fbad.flush()
            indexn += 1



def run_wordlist(filepath='ind_new.txt'):
    """
    读取将分词列表 与 行业分类关键词配置文件数据 做匹配
    匹配不到数据， 提示交互界面， 人工新增关键字匹配规则
    :param filepath: 分词列表的 txt 文件 ，分割数据
    :return:
    """

    with open(INPUT_PATH + os.sep + 'ind.txt', mode='r', encoding='utf-8') as f:
        data = f.read()
        data = data.replace("[", "").replace("]", "").replace("\'", "")
        word_list = data.split(',')
        industry_object = industry_classify()
        industry_object.interaction_industry_judge_continue(word_list)


def merge_yaml(file_path_list=[INPUT_PATH + os.sep + 'INDUSTRY_qq.yaml',
                               INPUT_PATH + os.sep + 'industry_zy.yaml',
                               INPUT_PATH + os.sep + 'industry_c.yaml']):
    industry_dict_all = industry_classify(file_path_list[0]).industry_dict

    for index, yml_file_path in enumerate(file_path_list[1:]):
        industry_dict_tmp = industry_classify(yml_file_path).industry_dict
        for key in industry_dict_all.keys():
            key_valueskey_1 = industry_dict_all[key].keys()
            key_valueskey_2 = industry_dict_tmp[key].keys()
            key_valueskey = list(set(key_valueskey_1) | set(key_valueskey_2))
            industry_dict_all[key] = dict.fromkeys(key_valueskey, True)

    tmp_yaml_path = OUTPUT_PATH + os.sep + 'industry' + "merge" + '.yaml'
    with open(tmp_yaml_path, mode='w', encoding='utf-8') as f:
        yaml.dump(industry_dict_all, f, default_flow_style=False, encoding='UTF-8', allow_unicode=True)



# with open('ind.txt', mode='r', encoding='utf-8') as f:
#     data = f.read()
#     data = data.replace("[", "").replace("]", "").replace("\'", "")
#     word_list1 = data.split(',')
#     word_list1 = [x.strip() for x in word_list1 if x.strip() != ""]
#     print(len(word_list1))
# with open('ind2.txt', mode='r', encoding='utf-8') as f:
#     data = f.read()
#     word_list2 = data.split('\n')
#     word_list2 = [x.strip() for x in word_list2 if x.strip() !=""]
#     print(len(word_list2))
# word_list = list( set(word_list2) - set(word_list1) )
# print(len(word_list))

if __name__ == "__main__":
    pass
    run()
    #
    # run_wordlist()
    # merge_yaml()