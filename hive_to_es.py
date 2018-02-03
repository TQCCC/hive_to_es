#! /usr/bin/env python
# -*- coding:utf-8 -*-
import codecs
import logging
import time
import sys

import re
from impala.dbapi import connect as big_data_connection
from elasticsearch import Elasticsearch
from elasticsearch import helpers as elasticsearch_helper

from imp import reload

reload(sys)
try:
    # Python3
    import configparser as ConfigParser
except:
    # Python2
    import ConfigParser

    sys.setdefaultencoding('utf8')

"""
Created by tangqingchang on 2017-09-02
python hive_to_es.py config=<配置文件路径.ini> [可选，需要导入的表: tables=table1,table2...]
"""

# TODO: 使用多线程

def get_map(param_list):
    """
    解析键值对形式的参数数组，返回dict
    :param param_list: 参数数组，如sys.argv
    :return:
    """
    param_dict = {}
    try:
        for pair in param_list:
            ls = pair.split('=')
            param_dict[ls[0]] = ls[1]
    except:
        return {}
    return param_dict


def get_list(s, f=','):
    """
    分割字符串为数组
    :param s: 字符串
    :param f: 分隔符，默认是','
    :return:
    """
    if (not s) or (not s.strip()) or (s.strip() == ""):
        return []
    else:
        ls = s.split(f)
        return ls


logging.basicConfig(level=logging.INFO)


def log(*content):
    """
    输出日志
    :param content:
    :return:
    """
    log_content = "[{t}]".format(t=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    for c in content:
        log_content += str(c)
    logging.info(log_content)


def s2t(seconds):
    """
    秒转化为时间字符串
    :param seconds:
    :return:
    """
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "%02d:%02d:%02d" % (h, m, s)


def get_file_content(path):
    """
    读文件
    :param path:
    :return:
    """
    file = codecs.open(path, 'r+', 'utf-8', 'ignore')
    data = file.read()
    file.close()
    return data


def run_query(sql):
    """
    执行mpala-SQL或者hiveQL获得结果
    :param sql:
    :return:
    """
    cur = big_data_conn.cursor()
    cur.execute(sql)
    des = cur.description
    res = cur.fetchall()
    res_data = []

    # 拼接成字典
    for r in res:
        d = dict()
        for i, v in enumerate(r):
            if '.' in des[i][0]:
                d[des[i][0].split('.')[1]] = v
                pass
            else:
                d[des[i][0]] = v
        res_data.append(d)
    return res_data


def add_paging_and_where_info_into_hql(**kwargs):
    """
    拼接为支持分页的HQL，加入分页信息
    :param kwargs
    :return:
    """
    hql = kwargs['hql']
    start_row = kwargs['start_row']
    to_row = kwargs['to_row']
    where = kwargs.get("where", "")

    ql = hql.lstrip()
    start_pos = hql.upper().find(re.findall("FROM\s", hql.upper())[0])
    left = ql[:start_pos]
    right = ql[start_pos:]
    left = left + ", ROW_NUMBER() OVER () AS row_number_flag "
    with_row_number_hql = "SELECT * FROM(" + left + right + ")t_paging"

    if len(where) > 0:
        return with_row_number_hql + " WHERE " + where + " AND row_number_flag BETWEEN " + str(
            start_row) + " AND " + str(
            to_row) + " ORDER BY row_number_flag"
    else:
        return with_row_number_hql + " WHERE row_number_flag BETWEEN " + str(start_row) + " AND " + str(
            to_row) + " ORDER BY row_number_flag"


def add_paging_and_where_info_into_impala_sql(**kwargs):
    """
    拼接为支持分页的Impala-SQL，加入分页信息
    :param kwargs
    :return:
    """

    impala_sql = kwargs['impala_sql']
    start_row = kwargs['start_row']
    to_row = kwargs['to_row']
    where = kwargs.get("where", "")

    ql = impala_sql.lstrip()
    start_pos = impala_sql.upper().find(re.findall("FROM\s", impala_sql.upper())[0])
    left = ql[:start_pos]
    right = ql[start_pos:]
    left = left + ", 0 AS `row_number_flag` "
    with_flag_sql = "SELECT * FROM(" + left + right + ")t_paging"

    page_size = to_row - start_row + 1

    if len(where) > 0:
        return with_flag_sql + " WHERE " + where + " ORDER BY `row_number_flag` LIMIT " + str(
            page_size) + " OFFSET " + str(start_row - 1)
    else:
        return with_flag_sql + " ORDER BY `row_number_flag` LIMIT " + str(page_size) + " OFFSET " + str(start_row - 1)


def get_paging_and_where_supported_sql(sql, start_row, to_row, where, platform):
    """
    获得支持分页信息的SQL
    :param sql:
    :param start_row: 起始行最小是1
    :param to_row:
    :param platform: hive or impala
    :return:
    """
    if platform == "hive":
        return add_paging_and_where_info_into_hql(hql=sql, start_row=start_row, to_row=to_row, where=where)
    elif platform == "impala":
        return add_paging_and_where_info_into_impala_sql(impala_sql=sql, start_row=start_row, to_row=to_row,
                                                         where=where)
    else:
        return ""


def config(k, v, fallback=None):
    """
    获取不到配置信息时返回fallback
    :param k:
    :param v:
    :param fallback:
    :return:
    """
    try:
        return main_config.get(k, v)
    except:
        return fallback


if len(sys.argv) < 2:
    log("参数不足")
    print("例子：")
    print("python hive_to_es.py config=<配置文件路径.ini> [可选，需要导入的表: tables=table1,table2...]")
    exit(0)

params_dict = get_map(sys.argv[1:])

main_config = ConfigParser.ConfigParser()
main_config.readfp(codecs.open(params_dict['config'], mode='r+', encoding='utf-8'))
es = Elasticsearch(hosts=get_list(config("es", "hosts")),
                   http_auth=(config("es", "username"),
                              config("es", "password")))

# 导数据途经默认hive
BY = config("es", "by", fallback="hive")
log("导数据途径：", BY)
big_data_conn = big_data_connection(host=config(BY, "host"),
                                    port=int(config(BY, "port")),
                                    database=config(BY, "database"),
                                    user=config(BY, "user", fallback=""),
                                    auth_mechanism=config(BY, "auth_mechanism", fallback=""),
                                    )
# 导入ES的index默认使用数据库名称
DEFAULT_ES_INDEX = config("es", "default_index", fallback=config(BY, "database"))
MAX_PAGE_SIZE = 30000


def run_job(job_config):
    """
     一个任务
    :return:
    """
    log("*************************", job_config['table'], "开始*************************")
    PAGE_SIZE = job_config["page_size"]
    ES_INDEX = job_config["es_index"]
    ES_TYPE = job_config["es_type"]
    COLUMNS = job_config['columns']
    ID_COLUMN = job_config['id_column']
    WHERE = job_config['where']
    COLUMN_MAPPING = job_config['column_mapping']
    OVERWRITE = job_config["overwrite"]
    SQL_PATH = job_config["sql_path"]

    if len(SQL_PATH) > 0:
        log("SQL文件: ", SQL_PATH)
        try:
            USER_SQL = get_file_content(SQL_PATH).strip()
            if len(COLUMNS) > 0:
                USER_SQL = "SELECT " + COLUMNS + " FROM (" + USER_SQL + ") AS columns_chosen"
        except Exception as e:
            log("读取SQL文件出错，退出: ", e)
            return
    else:
        log("无SQL文件，直接导表数据")
        if len(COLUMNS) > 0:
            USER_SQL = "SELECT " + COLUMNS + " FROM " + job_config['table']
            pass
        else:
            USER_SQL = "SELECT * FROM " + job_config['table']

    log("ES_INDEX: ", ES_INDEX)
    log("ES_TYPE: ", ES_TYPE)
    log("分页大小: ", PAGE_SIZE)
    log("是否全量：", OVERWRITE)
    log("自选字段：", COLUMNS)
    log("ID_COLUMN：", ID_COLUMN)
    log("自定义where条件：", WHERE)
    log("字段名称映射：", COLUMN_MAPPING)
    log("SQL内容: ", USER_SQL)
    if not (USER_SQL.startswith("select") or USER_SQL.startswith("SELECT")):
        log("只允许SELECT语句, 退出该任务")
        return
    log(">>>>>>>>>>>>>>>初始化结束>>>>>>>>>>>>>>>")

    # 开始记录时间
    start_time = time.time()

    current_row_num = 1
    result_size = PAGE_SIZE
    p = 1

    # 开始查询
    while result_size == PAGE_SIZE:
        log("==================第%s页开始===================" % p)
        s = time.time()
        log("当前行: ", current_row_num)

        start_row = current_row_num
        to_row = current_row_num + PAGE_SIZE - 1
        log("开始行号: ", start_row)
        log("结束行号: ", to_row)

        final_sql = get_paging_and_where_supported_sql(USER_SQL, start_row, to_row, where=WHERE, platform=BY)

        try:
            log("开始执行: ")
            log(final_sql)
            result_data = run_query(final_sql)
        except Exception as e:
            log(">>>>>>>>>>>>>>>SQL执行失败，结束该任务：", e, ">>>>>>>>>>>>>>>>>>")
            return

        if p == 1:
            # es准备
            if es.indices.exists(index=ES_INDEX) is True:
                if OVERWRITE == "true":
                    log("全量添加结果集")
                    # 删除type下所有数据
                    es.delete_by_query(index=ES_INDEX,
                                       body={"query": {"match_all": {}}},
                                       doc_type=ES_TYPE,
                                       params={"conflicts": "proceed"})
                else:
                    log("增量添加结果集")
                    pass
            else:
                es.indices.create(index=ES_INDEX)
                log("已新创建index：", ES_INDEX)

        actions = []
        for r in result_data:
            _source = dict()
            obj = dict()
            # 根据字段名称映射生成目标文档
            for k in r:
                if k == 'row_number_flag' or k == ID_COLUMN:
                    continue
                if COLUMN_MAPPING.get(k) is not None:
                    _source[COLUMN_MAPPING.get(k)] = r[k]
                else:
                    _source[k] = r[k]
            obj['_index'] = ES_INDEX
            obj['_type'] = ES_TYPE
            obj['_source'] = _source

            try:
                if len(ID_COLUMN) > 0:
                    obj['_id'] = r[ID_COLUMN]
            except:
                pass

            actions.append(obj)

        log("开始插入结果到ES...")
        if len(actions) > 0:
            elasticsearch_helper.bulk(es, actions)
        log("插入ES结束...")
        e = time.time()
        log("该页查询时间：", s2t(e - s))

        current_row_num = current_row_num + PAGE_SIZE
        result_size = len(result_data)
        p = p + 1

    end_time = time.time()
    log("************************", job_config['table'], ": 全部结束，花费时间：", s2t(end_time - start_time),
        "************************")


if params_dict.get('tables') is not None:
    result_tables = get_list(params_dict['tables'])
else:
    result_tables = get_list(config("table", "tables", fallback=""))

for result in result_tables:
    job_conf = dict()

    job_conf['table'] = result
    job_conf['columns'] = config(result, "columns", fallback="")
    job_conf['id_column'] = config(result, "id_column", fallback="")
    job_conf['column_mapping'] = get_map(get_list(config(result, "column_mapping", fallback="")))
    job_conf['es_index'] = config(result, "es_index", fallback=DEFAULT_ES_INDEX)
    job_conf['es_type'] = config(result, "es_type", fallback=result)

    job_conf['page_size'] = min(int(config(result, "page_size", fallback=MAX_PAGE_SIZE)),
                                MAX_PAGE_SIZE)
    # 默认全量导表
    job_conf['overwrite'] = config(result, "overwrite", fallback="true")

    job_conf['sql_path'] = config(result, "sql_path", fallback="")

    job_conf['where'] = config(result, "where", fallback="")
    try:
        run_job(job_conf)
    except Exception as e:
        log(result, "执行job出错：", job_conf, ": ", e)

big_data_conn.close()
