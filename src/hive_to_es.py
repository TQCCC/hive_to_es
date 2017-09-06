#! /usr/bin/env python
# -*- coding:utf-8 -*-
import sys
import pyhs2
import time
from elasticsearch import Elasticsearch
from elasticsearch import helpers as elasticsearch_helper
# 在Python2撤销下行注释
# import ConfigParser as ConfigParser

# 在Python3将下行注释
import configparser as ConfigParser

# Linux环境撤销下两行注释:
# reload(sys)
# sys.setdefaultencoding('utf8')

"""
Created by tangqingchang on 2017-09-02
python hive_to_es.py <配置文件路径>
"""


def get_list(data, f=','):
    ls = data.split(f)
    return ls


def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))


def log(content):
    sys.stdout.write("[{t}]".format(t=get_time()))
    print(content)


def s2m(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "%02d:%02d:%02d" % (h, m, s)


def get_file_content(path):
    """
    读文件
    :param path:
    :return:
    """
    f = open(path, 'r')
    return f.read()


def run_hive_query(hql):
    """
    跑hiveQL
    :param hql:
    :return:
    """
    with hive_conn.cursor() as cursor:
        cursor.execute(hql)
        res = cursor.fetchall()
        cursor.close()
        return res


def add_row_number_into_hql(hql):
    """
    拼接为支持分页的HQL
    :param hql:
    :return:
    """
    ql = hql.lstrip()
    start_pos = len("SELECT")
    left = ql[:start_pos + 1]
    right = ql[start_pos:]
    left = left + "ROW_NUMBER() OVER () AS row_number,"
    return "SELECT * FROM(" + left + right + ")t_paging"


def add_paging_limit_into_hql(hql, start_row, to_row):
    """
    拼接为支持分页的HQL，加入分页信息
    :param hql:
    :param start_row:
    :param to_row:
    :return:
    """
    return add_row_number_into_hql(hql) + " WHERE row_number BETWEEN " + str(start_row) + " AND " + str(to_row)


if len(sys.argv) < 2:
    log("参数不足")
    exit(0)

config = ConfigParser.ConfigParser()
config.readfp(open(sys.argv[1], "r"))

HQL_PATH = config.get("hive", "hql_path")
MAX_PAGE_SIZE = 30000
try:
    PAGE_SIZE = int(config.get("hive", "page_size"))
except:
    log("没有找到分页配置，使用默认值")
    PAGE_SIZE = MAX_PAGE_SIZE

ES_INDEX = config.get("es_bulk", "index")
ES_TYPE = config.get("es_bulk", "type")

PAGE_SIZE = min(PAGE_SIZE, MAX_PAGE_SIZE)

log("HQL文件: " + HQL_PATH)
log("ES_INDEX: " + ES_INDEX)
log("ES_TYPE: " + ES_TYPE)
log("分页大小: " + str(PAGE_SIZE))
log(">>>>>>>>>>初始化结束>>>>>>>>>>")

# 开始记录时间
start_time = time.time()
try:
    es = Elasticsearch(hosts=get_list(config.get("es", "hosts")),
                       http_auth=(config.get("es", "username"),
                                  config.get("es", "password")))
except:
    log("ES连接出错")
    exit(0)

try:
    hive_conn = pyhs2.connect(host=config.get("hive", "host"),
                              port=config.get("hive", "port"),
                              authMechanism=config.get("hive", "authMechanism"),
                              user=config.get("hive", "user"),
                              database=config.get("hive", "database"))
except:
    log("Hive连接出错")
    exit(0)

USER_HQL = get_file_content(HQL_PATH).lstrip()
if not (USER_HQL.startswith("select") or USER_HQL.startswith("SELECT")):
    log("只允许SELECT语句")
    exit(0)

log("HQL文件内容: " + USER_HQL)

# es准备
if es.indices.exists(index=ES_INDEX) is True:

    try:
        if config.get("es_bulk", "overwrite") is not None:
            log("全量添加结果集")
            # 删除type下所有数据
            es.delete_by_query(index=ES_INDEX,
                               body={"query": {"match_all": {}}},
                               doc_type=ES_TYPE,
                               params={"conflicts": "proceed"})
    except:
        log("增量添加结果集")
        pass
else:
    log("增量添加结果集")
    es.indices.create(index=ES_INDEX)
    log("已新创建index：" + ES_INDEX)

es_columns = get_list(config.get("es_bulk", "columns"))
log("插入到ES的各个字段(es_columns): " + str(es_columns))

prepare_hql = ("SELECT COUNT(*), MIN(row_number) FROM (" + add_row_number_into_hql(USER_HQL) + ")t_count")
log("Prepare HQL: " + prepare_hql)
log("开始获取总行数和分页起始行...")

pre_result = run_hive_query(prepare_hql)
log(pre_result)
total_count = int(pre_result[0][0])

if total_count == 0:
    log("数据结果为0，退出")
    exit(0)

current_row_num = int(pre_result[0][1])

page_count = int((total_count + PAGE_SIZE - 1) / PAGE_SIZE)

log("结果集合总数: " + str(total_count))
log("分页大小: " + str(PAGE_SIZE))
log("总页数: " + str(page_count))
log("起始行：" + str(current_row_num))

# 开始查询
for p in range(0, page_count):
    log("==================第%s页开始===================" % (p + 1))
    s = time.time()
    log("当前行: " + str(current_row_num))

    start_row = current_row_num
    to_row = current_row_num + PAGE_SIZE - 1
    log("开始行号: " + str(start_row))
    log("结束行号: " + str(to_row))

    final_hql = add_paging_limit_into_hql(USER_HQL, start_row, to_row)

    log("开始执行: ")
    log(final_hql)
    hive_result = run_hive_query(final_hql)

    actions = []
    # log("该页结果：")
    log("获得查询结果")
    for r in hive_result:
        src_list = []
        obj = {}
        for i in range(0, len(es_columns)):
            # r[i+1]是因为第一个字段是用于分页依据的row_number，不需要放入结果集合
            src_list.append((es_columns[i], r[i + 1]))
            _source = dict(src_list)
            # ('_id', r[0]),
            obj_list = [('_index', ES_INDEX), ('_type', ES_TYPE), ('_source', _source)]
            obj = dict(obj_list)

        # log(obj)
        actions.append(obj)

    log("开始插入结果到ES...")
    if len(actions) > 0:
        elasticsearch_helper.bulk(es, actions)
    log("插入ES结束...")
    e = time.time()
    log("该页查询时间：" + str(s2m(e - s)))
    current_row_num = current_row_num + PAGE_SIZE

end_time = time.time()
log(">>>>>>>>>>>>>>>>全部结束，花费时间：" + str(s2m(end_time - start_time)) + ">>>>>>>>>>>>>>>>")
