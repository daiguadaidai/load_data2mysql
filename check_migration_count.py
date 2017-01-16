#!/usr/bin/env python
# -*- coding:utf-8 -*-

import multiprocessing
import time
import sys
from sqlalchemy import create_engine

reload(sys)
sys.setdefaultencoding('utf-8')

class CheckMigrationCount(object):
    """通过执行数据库获得每个表的行数并保存成dict"""

    def __init__(self, host='127.0.0.1', port=3306, username='',
                       password='', database='', charset='utf8',
                       tag=''):
        """通过获得数据库的链接参数创建数据库连接"""

        self.conf = {
            'host': host,
            'port': port,
            'username': username,
            'password': password,
            'database': database,
            'charset': charset,
        }

        self.tag = tag

        self.conf_str = ('mysql+mysqldb://{username}:{password}@{host}:{port}/'
                         '{database}?charset={charset}'.format(**self.conf))

        # 获取数据库连接
        self.engine = create_engine(self.conf_str)
        self.conn = self.engine.connect()

    def get_tables(self, exclude=[]):
        """通过执行 SHOW TABLES 获取数据库所有的表
        Args:
            exclude: 排除那些表
                example: exclude=['table_name1', 'view_name1']
        Return: none
        Raise: none
        """
        sql = 'SHOW TABLES'

        result = self.conn.execute(sql)

        self.tables = [
            record[0]
            for record in result
            if record[0] not in exclude
        ]

    def get_table_counts(self):
        """循环表并且执行SQL获得表的行数
        Example: SELECT COUNT(*) FROM xxx;
        """

        sql = 'SELECT COUNT(*) FROM {table}'

        self.table_counts = {} # 用于保存表名和表行数

        # 循环执行 sql 语句
        for table in self.tables:
            count_sql = sql.format(table = table)
            result = self.conn.execute(count_sql)
            table_count = result.fetchone()

            # 将行数保存到 table_counts中
            self.table_counts[table] = table_count[0]

    @classmethod
    def check_table_counts(self, resource, target):
        """将传入的 table_counts 和自己的 table_counts进行比较并排序
        Args:
            resource: 一个 table_counts dict 实例
            target: 另一个 table_counts dict 实例
                example: {
                             'baichuan': {
                                 'table_name1': 1,
                                 'table_name2': 2,
                             },
                         }
        Return: None
        Raise: None
        """

        # 获取源数据库的 tag 标记, 和 数据库表的具体行数
        resource_tag = resource.keys()[0]
        resource_table_counts = resource.get(resource_tag, {})

        # 获取目标数据库的 tag 标记, 和 数据库表的具体行数
        target_tag = target.keys()[0]
        target_table_counts = target.get(target_tag, {})

        # 循环比较两个库的表行数是否是相同
        for key in sorted(resource_table_counts.keys()):

            is_ok = 'fail'
            if (resource_table_counts.get(key, -1) ==
                target_table_counts.get(key, -1)):
                is_ok = 'ok'

            print '-----------------------------------------------------------'
            print resource_tag, key, resource_table_counts.get(key, -1)
            print target_tag, key, target_table_counts.get(key, -1)
            print is_ok


def worker(q, conf={}, exclude=[]):
    """传入CheckMigrationCount实例执行操作计算表行数操作"""

    cmc = CheckMigrationCount(**conf)
    cmc.get_tables(exclude = exclude)
    cmc.get_table_counts()

    # 执行完后把数据库表信息放入队列
    q.put({cmc.tag: cmc.table_counts})


def checker(q):
    """获取队列中对象并且进行比较"""
    resource = q.get()
    target = q.get()
    CheckMigrationCount.check_table_counts(
        resource = resource,
        target = target,
    )


if __name__ == '__main__':
    conf1 = {
        'username': 'xxx',
        'password': 'xxx',
        'host': 'xxx',
        'port': 3306,
        'database': 'xxx',
        'charset': 'utf8',
        'tag': 'baichuan', # 用于标记是哪个实例, 方便输出确认
    }

    conf2 = {
        'username': 'xxx',
        'password': 'xxx',
        'host': 'xxx',
        'port': 3306,
        'database': 'xxx',
        'charset': 'utf8',
        'tag': 'jushita ', # 用于标记是哪个实例, 方便输出确认
    }


    # 不需要比较的表
    exclude = [
        'easy_agent_trade',
        'easy_agent_view',
        'guide_income_detail',
        'store_all_trade',
        'store_guide_group_condition',
        'store_trade_online',
        'web_tmallshoplist',
    ]

    q = multiprocessing.Queue(maxsize = 2) # 定义一个队列

    # 创建多线程执行获得表行数操作
    p1 = multiprocessing.Process(target = worker, args = (q, conf1, exclude,))
    p2 = multiprocessing.Process(target = worker, args = (q, conf2, exclude,))

    # 开始多进程进行获取各自数据库表行数
    p1.start()
    p2.start()

    # 冻结当前(主)进程
    p1.join()
    p2.join()

    # 检测
    checker(q)
