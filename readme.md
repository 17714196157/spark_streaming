#流式数据清理与整合
##功能与需求
流式处理获取数据的数据清理 与 字段枚举化
输出到文件系统或者kafka队列中

###文件目录结果说明
根目录下是流式处理 spark脚本 需要用spark-submit命令启动
code 目录下是批量处理数据的脚本
code 目录暇 soark_RDD_opt 是 spark 调用的函数
config 目录下是配置文件
output 是批量处理数据的脚本输出csv文件的目录

###依赖第三方库
pip install pykafka
pip install mongoengine
pip install pyyaml
pip install phoenixdb