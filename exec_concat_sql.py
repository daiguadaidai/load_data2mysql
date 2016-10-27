#!/usr/bin/env python
#-*- coding:utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import traceback
import sys

reload(sys)
sys.setdefaultencoding('utf8')
 

class ExecConcatSQL(object):
    """通过执行select SQL 获取 execute_sql 字段再运行"""

    def __init__(self, username='root', password='root',
                       host='127.0.0.1', port=3306, database='',
                       charset='utf8'):
        """初始化数据库连接"""
        self.db_conf = {
            'username': username,
            'password': password,
            'host': host,
            'port': port,
            'database': database,
            'charset': charset,
        }

        # 生成数据库连接串
        self.db_str = ('mysql+mysqldb://{username}:{password}@{host}:{port}/{database}'
                       '?charset={charset}'.format(**self.db_conf))
        # 获得连接数据库对象
        self.qdl_engine = create_engine(self.db_str)
        self.dml_engine = create_engine(self.db_str)

        self.DB_Session = sessionmaker(bind=self.qdl_engine)

        self.qdl_session = self.DB_Session()
        self.dml_session = self.DB_Session()

    def execute_sql(self, sql, commit_limit=10000):
        """执行select sql 执行dml sql"""
        self._exec_select_sql(sql)
        self._exec_dml_sql(commit_limit=commit_limit)

    def _exec_select_sql(self, sql):
        """执行SELECT sql"""
        self.rs = self.qdl_session.execute(sql)
        self.execute_sql_col_index = self.rs.keys().index('execute_sql')

    def _exec_dml_sql(self, commit_limit=10000):
        """通过执行的select 获取的execute_sql 在数据库中执行"""
        try:
            for index, row in enumerate(self.rs.fetchall()):
                dml_sql = row[self.execute_sql_col_index]
                print dml_sql
                self.dml_session.execute(dml_sql)
                if index % commit_limit == 0: self.dml_session.commit()
        except Exception as e:
            self.dml_session.rollback() # 出错回滚
            print traceback.format_exc()
        else:
            self.dml_session.commit() # 提交事务
        finally:
            self.qdl_session.close()
            self.dml_session.close()
       

def main():
    commit_limit = 10000

    sql = """
    SELECT goods_id,
        MIN(retail_price),
        CONCAT('UPDATE pro_goods SET supplier_price=', MIN(pgs.retail_price), ' WHERE goods_id="', goods_id, '";') AS execute_sql
    FROM pro_goods_sku AS pgs
    GROUP BY goods_id LIMIT 0, 1000;
    """

    db_conf = {
        'username': 'cloud',
        'password': 'cloud',
        'host': '192.168.0.152',
        'port': 3307,
        'database': 'retail_share',
        'charset': 'utf8',
    }

    exec_concat_sql = ExecConcatSQL(**db_conf)
    exec_concat_sql.execute_sql(sql, commit_limit=commit_limit)
    

if __name__ == '__main__':
    main()
