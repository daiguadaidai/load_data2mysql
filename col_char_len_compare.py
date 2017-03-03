#!/usr/bin/env python
#-*- coding:utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import traceback
import numpy as np
import pandas as pd
import sys
import copy

reload(sys)
sys.setdefaultencoding('utf8')


class ColCharLenCompare():
    """字段字符长度统计
    统计出字段实际长度和定义的长度
    """
    
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
        self.db_engine = create_engine(self.db_str)

        self.db_session = sessionmaker(bind=self.db_engine)

        self.session = self.db_session()
        
    def _exec_select_sql(self, sql=None):
        """执行SELECT语句并且获得数据"""
        rs = self.session.execute(sql)
        
        return rs
        
    def _get_col_define_len(self, sql=''):
        """获得所有表字段的定义长度"""
        rs = self._exec_select_sql(sql = sql)

        col_define_len_info = {}

        for row in rs.fetchall():
            table_name = row[0] # 获得表名
            col_name = row[1] # 获得字段名

            # 拼凑 dict 小标(table_name.col_name)
            col_define_len_info_index = '{table_name}.{col_name}'.format(
                                        table_name = table_name,
                                        col_name = col_name)

            table_cols = zip(rs.keys(), row)

            col_define_len_info[col_define_len_info_index] = dict(
                (table_name, col_name)
                for table_name, col_name in table_cols
            )

        return col_define_len_info
        
        
    def _get_col_actual_len(self, table_name='', col_name=''):
        """获得所有表字段的实际长度"""

        sql = """
        SELECT MAX(LENGTH({col_name}))
        FROM {table_name}
        """.format(table_name = table_name,
                   col_name = col_name)

        try:
            rs = self._exec_select_sql(sql)

            # 获取字段实际长度
            row = rs.fetchone()
            col_len = row[0]
        except:
            col_len = -1

        return col_len

    def get_col_char_len(self, sql=''):
        """获取字段定义长度和实际长度 并且进行合并"""

        col_define_len_info = self._get_col_define_len(sql)

        self.col_define_len_info = copy.deepcopy(col_define_len_info)

        for key, value in col_define_len_info.iteritems():
            actual_col_len = self._get_col_actual_len(
                                    table_name = value['TABLE_NAME'],
                                    col_name = value['COLUMN_NAME'])
 
            # 拼凑小角标
            col_index = '{table_name}.{col_name}'.format(
                                        table_name = value['TABLE_NAME'],
                                        col_name = value['COLUMN_NAME'])

            # 将实际长度保存在col_define_len_info中
            
            self.col_define_len_info[col_index]['ACTUAL_LEN'] = actual_col_len

    def export_excel(self, col_names=[], file_name=''):
        """通过给出的字段名导出数据excel"""
        df = pd.DataFrame(self.col_define_len_info.values())

        self.df = df.sort(['TABLE_NAME'])
        self.df.columns = col_names

        self.df.to_excel(file_name, encoding='utf-8')
        

def main():
    sql = """
    SELECT TABLE_NAME,
        COLUMN_NAME,
        CHARACTER_OCTET_LENGTH
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = 'wx_005_fb'
        AND COLUMN_TYPE LIKE'varchar%';
    """

    db_conf = {
        'username': 'wx_005',
        'password': 'wx1234',
        'host': '103.236.255.184',
        'port': 3306,
        'database': 'wx_005_fb',
        'charset': 'utf8',
    }

    col_char_len_compare = ColCharLenCompare(**db_conf)

    col_char_len_compare.get_col_char_len(sql)

    col_names = ['实际长度', '定义长度', '字段名', '表名']
    file_name = '/tmp/col_char_len_compare.xlsx'
    col_char_len_compare.export_excel(col_names = col_names,
                                      file_name = file_name)


if __name__ == '__main__':
    main()
