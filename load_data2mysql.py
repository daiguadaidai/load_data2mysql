#!/usr/bin/env python
#-*- coding:utf-8 -*-

from sqlalchemy import create_engine
import pandas as pd
import argparse

class LoadData2Mysql(object):
    """加载各种数据到数据库"""

    def __init__(self, username='root', password='root',
                       host='127.0.0.1', port=3306, database='',
                       charset='utf8'):
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
        self.engine = create_engine(self.db_str)

        self.df = None # 初始化数据结构

        # 初始化能传输数据的方法
        self.func_dict = {
            'excel': self.read_excel2mysql,
            'csv': self.read_csv2mysql,
        }

    def read_file2mysql(self, file, type, table, *args, **kargs):
        """通过给的文件名，文件类型，表明执行不同的数据载入方法"""
        return self.func_dict[type](file, table)

    def read_excel2mysql(self, file, table, *args, **kargs):
        """加载excel数据到MySQL中Excel中的数据标题要对应MySQL中的字段， 
           注意: 字段只允许少，不允许多。 
        """ 
        print 'start load data ...'
        print 'file name: {file}'.format(file = file)
        print 'table name: {table}'.format(table = table)
        # 读取excel数据
        self.df = pd.read_excel(file)
        # 将数据保存到数据库中
        self.df.to_sql(table,
                       self.engine,
                       if_exists='append',
                       index=False)
        print '=============== load successful ==============='

        return True

    def read_csv2mysql(self, file, table, *args, **kargs):
        """加载csv数据到MySQL中csv中的数据标题要对应MySQL中的字段， 
        Args:
            file: 文件名
            table: 执行需要插入数据的表名
            sep: csv 文件的分隔符
            delimiter: csv 文件的行分割符
        Return: True/False
        注意: 字段只允许少，不允许多。 
        """ 
        print 'start load data ...'
        print 'file name: {file}'.format(file = file)
        print 'table name: {table}'.format(table = table)
        # 读取excel数据
        self.df = pd.read_csv(file, **kargs)
        # 将数据保存到数据库中
        self.df.to_sql(table,
                       self.engine,
                       if_exists='append',
                       index=False)
        print '=============== load successful ==============='

        return True


def parse_args():
    """解析命令行传入参数"""
    usage = """
Description:
    The script load a file data to mysql table
    """
    # 创建解析对象并传入描述
    parser = argparse.ArgumentParser(description = usage)
    # 添加 MySQL Host 参数
    parser.add_argument('--host', dest='host', required = True,
                      action='store', default='127.0.0.1',
                      help='Connect MySQL host', metavar='HOST')
    # 添加 MySQL Port 参数
    parser.add_argument('--port', dest='port',
                      action='store', default=3306, required = True,
                      help='Connect MySQL port', metavar='PORT')
    # 添加 MySQL username 参数
    parser.add_argument('--username', dest='username', required = True,
                      action='store', default='root',
                      help='Connect MySQL username', metavar='USERNAME')
    # 添加 MySQL password 参数
    parser.add_argument('--password', dest='password', required = True,
                      action='store', default='root',
                      help='Connect MySQL password', metavar='PASSWORD')
    # 添加 MySQL database 参数
    parser.add_argument('--database', dest='database', required = True,
                      action='store', default='',
                      help='Select A database', metavar='DATABASE')
    # 添加 MySQL table 参数
    parser.add_argument('--table', dest='table', required = True,
                      action='store', default='',
                      help='Select A table', metavar='TABLE')
    # 添加 data file type 参数
    parser.add_argument('--type', dest='type', required = True,
                      action='store', default='',
                      help='Input file type [excel | csv]', metavar='TYPE')
    # 添加 data file name 参数
    parser.add_argument('--file', dest='file', required = True,
                      action='store', default='',
                      help='Input file name [goods.xlsx | goods.csv]', metavar='FILE')
    # 添加 csv 文件的每个字段的分隔符 参数
    parser.add_argument('--sep', dest='sep',
                      action='store', default=',',
                      help='Direct csv file seperator default ","', metavar='SEP')
    # 添加 csv 文件每一行的分隔符 参数
    parser.add_argument('--delimiter', dest='delimiter',
                      action='store', default=None,
                      help='Direct csv file every row delimiter default "\\n"', metavar='DELIMITER')

    args = parser.parse_args()

    return args

def main():
    args = parse_args() # 解析传入参数

    # 加载数据库连接参数
    load_data2mysql = LoadData2Mysql(host = args.host, port = args.port,
                                     username = args.username, password = args.password,
                                     database = args.database)
    # 读取文件将数据存入MySQL
    load_data2mysql.read_file2mysql(args.file, args.type, args.table,
                                    sep=args.sep, delimiter=args.delimiter)
    
if __name__ == '__main__':
    main()
