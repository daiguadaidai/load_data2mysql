#!/usr/bin/env python
#-*- coding:utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy import Table
from sqlalchemy import MetaData
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert

import traceback
import sys

reload(sys)
sys.setdefaultencoding('utf8')


@compiles(Insert)
def append_string(insert, compiler, **kw):
    """定义使用sqlalchemy table.insert额外传入参数的装饰器
    Example:
        conn.execute(
             table.insert(append_string='ON DUPLICATE KEY UPDATE value=VALUES(value)),
             {"id":1, "value":"v1"},
             {"id":2, "value":"v2"}
        )
    """
    s = compiler.visit_insert(insert, **kw)
    if 'append_string' in insert.kwargs:
        return s + " " + insert.kwargs['append_string']
    return s

class SelectData2Mysql(object):
    """将select出来的数据传输到另外的数据库表"""

    def __init__(self, qdl_host='127.0.0.1', qdl_port=3306, qdl_username='root',
                       qdl_password='root', qdl_database='', qdl_charset='utf8',
                       dml_host='127.0.0.1', dml_port=3306, dml_username='root',
                       dml_password='root', dml_database='', dml_charset='utf8'):
        """初始化传输的数据库两端数据库连接"""
        self.qdl_conf = {
            'username': qdl_username,
            'password': qdl_password,
            'host': qdl_host,
            'port': qdl_port,
            'database': qdl_database,
            'charset': qdl_charset,
        }
        self.dml_conf = {
            'username': dml_username,
            'password': dml_password,
            'host': dml_host,
            'port': dml_port,
            'database': dml_database,
            'charset': dml_charset,
        }
        # 生成数据库连接串
        self.qdl_str = ('mysql+mysqldb://{username}:{password}@{host}:{port}/{database}'
                        '?charset={charset}'.format(**self.qdl_conf))
        self.dml_str = ('mysql+mysqldb://{username}:{password}@{host}:{port}/{database}'
                        '?charset={charset}'.format(**self.dml_conf))
        # 获得连接数据库对象
        self.qdl_engine = create_engine(self.qdl_str)
        self.dml_engine = create_engine(self.dml_str)

        # 获得数据库连接
        self.qdl_conn = self.qdl_engine.connect()
        self.dml_conn = self.dml_engine.connect()

    def execute_select_sql(self, sql):
        """执行SELECT SQL"""
        self.qdl_rs = self.qdl_conn.execute(sql)

    def execute_insert(self, size=10000, cols=[], is_insert_ignore=False):
        """执行插入语句
        Args:
            size: 多少行插入一次(commit一次)
            cols: 如果重复需要更新的字段
            is_insert_ignore: 如果重复是否忽略
        判断执行顺序:
            1. 如果有设 is_insert_ignore 这执行 INSERT IGNORE INTO
            2. 如果有设 cols 执行 INSERT INTO ON DUPLICATE UPDATE
            3. 如果什么都没 INSERT INTO
        """
        if is_insert_ignore:
            self.execute_insert_dup_ignore(size=size)
        elif cols:
            self.execute_insert_dup_update(size=size, cols=cols)
        else:
            self.execute_insert_dup_update(size=size)

    def execute_insert_dup_update(self, size=10000, cols=[]):
        """通过select获得的数据在另一个表执行
        INSERT INTO 或
        INSERT INTO ON DUPLICATE UPDATE
        """
        # 获取 ON DUPLICATE 语句
        append_string = self._set_dup_on_update_col(cols=cols)

        for rows, count, total_count in self._fetchall_dict(self.qdl_rs, size=size):
            self.dml_conn.execute(
                self.dml_table.insert(append_string=append_string),
                rows
            )
            print '{count}/{total_count}'.format(count=count, total_count=total_count)

    def execute_insert_dup_ignore(self, size=10000):
        """通过select获得的数据在另一个表执行
        INSERT IGNORE INTO
        """
        for rows, count, total_count in self._fetchall_dict(self.qdl_rs, size=size):
            self.dml_conn.execute(
                self.dml_table.insert().prefix_with('IGNORE'),
                rows
            )
            print '{count}/{total_count}'.format(count=count, total_count=total_count)

    def bind_dml_table(self, table_name):
        """绑定一个需要执行DML 的表"""
        # 绑定表结构
        self.dml_metadata = MetaData(self.dml_conn)
        self.dml_table = Table(table_name, self.dml_metadata, autoload=True)

    def _set_dup_on_update_col(self, cols=[]):
        """通过传入的列名, 构造需要跟新的字段"""
        if not cols:
            return ''

        append_string = 'ON DUPLICATE KEY UPDATE {col_values}'
        col_values = ['{col}=VALUES({col})'.format(col=col) for col in cols if col]
        append_string = append_string.format(col_values=','.join(col_values))
        print append_string
        return append_string

    def _fetchall_dict(self, rs, size=10000):
        """将查询出来的结果集转化成 dict
        Args:
            rs: 查询的结果集
            size: fetchmany 的大小
        Return:
            rows: 一个 dict 结果集的 列表
            fetch_count: fetch 到第几次了
            need_fetch_count: 总共需要fetch多少次
        """
        need_fetch_count = (rs.rowcount / size) + 1
        for i in range(need_fetch_count):
            yield [
                dict(zip(rs.keys(), row))
                for row in rs.fetchmany(size=size)
            ], i + 1, need_fetch_count


def main():

    sql = """
    SELECT express_id,
        express_name,
        express_code,
        create_time
    FROM ord_express_bak;
    """

    conf = {
        'qdl_username': 'HH',
        'qdl_password': 'oracle',
        'qdl_host': '192.168.1.233',
        'qdl_port': 3306,
        'qdl_database': 'test',
        'qdl_charset': 'utf8',

        'dml_username': 'HH',
        'dml_password': 'oracle',
        'dml_host': '192.168.1.233',
        'dml_port': 3306,
        'dml_database': 'test',
        'dml_charset': 'utf8',
    }

    select_data2mysql = SelectData2Mysql(**conf)
    select_data2mysql.bind_dml_table('ord_express')
    select_data2mysql.execute_select_sql(sql)
    select_data2mysql.execute_insert(size=5, cols=['create_time'], is_insert_ignore=True)


if __name__ == '__main__':
    main()
