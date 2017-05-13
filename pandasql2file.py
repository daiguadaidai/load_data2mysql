#!/usr/bin/env python
#-*- coding:utf-8 -*-

from sqlalchemy import create_engine
from pandasql import sqldf
import pandas as pd
import time
import sys
import os

pysqldf = lambda q: sqldf(q, globals())

reload(sys)
sys.setdefaultencoding('utf-8')


class SQL2File(object):

    sqls = {}

    def __init__(self):
        pass

    def conn(self, host='127.0.0.1', port=3306,
                   username='root', password='root',
                   database='report'):
        """创建和数据库相关的链接"""

        self.conf = {
            'host': host,
            'port': port,
            'username': username,
            'password': password,
            'database': database,
        }

        self.conf_str = ('mysql+mysqldb://{username}:{password}@{host}:{port}/{database}'
                         '?charset=utf8'.format(**self.conf))
        self.engine = create_engine(self.conf_str)

    def get_file_names(self, dir='./sqls', exclude=[]):
        """读取指定目录下的文件并且进行, 不进行递归读取
        Args:
            dir: 读取文件的目录
            exclude: 排除那些文件
        """
        for parent, dirnames, filenames in os.walk(dir):
            valid_file_names = list(set(filenames) - set(exclude))
            return ['{parent}/{name}'.format(parent=parent, name=name) for name in valid_file_names]

    def parse_sql_by_files(self, file_names=[]):
        """通过给的文件集合进行一个个解析"""
        for file_name in file_names:
            self.parse_sql_by_file(file_name = file_name)

    def parse_sql_by_file(self, file_name=''):
        """解析文件中sql"""
        if not file_name:
            print 'file not directory'
            return None

        sql_file = open(file_name, 'r')

        table_name = ''
        sql_lines = []
        parse_type = 0 # 0:表名  1:sql语句

        for line in sql_file:
            
            # pdb.set_trace()
            # 解析出是表名还是语句
            if not line.strip():
                continue
            elif line.replace(' ', '').lower().startswith('--table_name'):
                table_name = line.split(':').pop().strip()
                continue
            elif line.replace(' ', '').lower().startswith('--statment'):
                parse_type = 1 # 下面解析的是语句
                continue

            # 将sql语句每一个报错到list中
            if parse_type == 1:
                sql_lines.append(line)

        self.sqls[table_name] = ''.join(sql_lines)

    def sql2csv(self, file_name='/tmp/sql.xls'):
        """执行sql并且导出为Excel
        Args:
            sql: 需要执行的SQL
        """
        self.exec_padasql(sql = sql)
        self.df.to_csv(file_name)

    def sql2excel(self, file_name='/tmp/sql.xlsx'):
        """执行sql并且导出为Excel
        Args:
            sql: 需要执行的SQL
        """
        writer = pd.ExcelWriter(file_name, engine='openpyxl')
        self.df.to_excel(writer, 'Sheet1')
        writer.save()

    def exec_padasql(self, sql=''):
        """执行pandsql, 如果sql为空则不不执行"""
        if not sql:
            return None

        # 执行sqls中的每一条SQL, 并且以 sqls 中 key 变为存放DataFrame的变量名
        for table_name in self.sqls:

            exe_sql = 'pd.read_sql_query(self.sqls["{table_name}"], self.engine)'.format(
                table_name = table_name
            )

            print exe_sql

            # 将数据库查询出来的数据保存到本地变量中
            try:
                globals()[table_name] = eval(exe_sql)
            except:
                globals()[table_name] = eval(exe_sql)

        self.df = pysqldf(sql)

def main():

    conf = {
        'host': '127.0.0.1',
        'port': '3306',
        'username': 'root',
        'password': 'root',
        'database': 'report',
    }

    sql2file = SQL2File()
    sql2file.conn(**conf)

    file_names = sql2file.get_file_names(dir = './sqls')

    sql2file.parse_sql_by_files(file_names = file_names)

    final_sql = '''
SELECT
    oa.ASSOCIATOR_ID,
    oa.ACCOUNT_ID AS '商家账号',
    oa.MY_TERMINAL_CODE AS '推广号',
    oa.ASSOCIATOR_NAME AS '商家名称',
    oa.PROVINCE_NAME AS '省',
    oa.CITY_NAME AS '市',
    oa.DISTRICT_NAME AS '区',
    oa.FIRST_ORDER_TIME AS '第一次订单时间',
    om.total_member_count as '用户数',
    om.today_member_count as '今日用户数',
    toa.TERMINAL_COUNT AS '推广商家数',
    slo.yestoday_amount as '前一天交易金额',
    slo.today_amount '今日交易金额',
    slo.today_order_count '今日订单数',
    slo.order_amount '交易总额',
    slo.order_count AS '交易订总数'
FROM oa
LEFT JOIN om
    ON om.ASSOCIATOR_ID = oa.ASSOCIATOR_ID
LEFT JOIN slo
    ON slo.ASSOCIATOR_ID = oa.ASSOCIATOR_ID
LEFT JOIN toa
    ON toa.ASSOCIATOR_ID = oa.ASSOCIATOR_ID
    '''

    excel_name = '/tmp/nbuy52db_info_{dt}.xlsx'.format(dt = str(time.time()))
    sql2file.exec_padasql(sql = final_sql)
    sql2file.sql2excel(file_name = excel_name)
    


if __name__ == '__main__':
    main()
