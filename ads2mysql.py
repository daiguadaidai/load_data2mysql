#!/usr/bin/env python
#-*- coding:utf-8 -*-

from sqlalchemy import create_engine
import pandas as pd
import datetime
import sys
import gc

reload(sys)
sys.setdefaultencoding('utf-8')

class ADS2MySQL(object):
    """将数据从 ADS传输到MySQL
    Author: Chenh
    Date: 2017-04-21
    """

    def __init__(self, from_host='127.0.0.1', from_port=3306, from_username='root',
                       from_password='root', from_database='test',
                       to_host='127.0.0.1', to_port=3306, to_username='root',
                       to_password='root', to_database='test'):

        """初始化数据库链接
        """

        # 源数据库链接信息
        self.from_conf = {
            'host': from_host,
            'port': from_port,
            'username': from_username,
            'password': from_password,
            'database': from_database,
            'charset': 'utf8',
        }

        # 目标据库链接信息
        self.to_conf = {
            'host': to_host,
            'port': to_port,
            'username': to_username,
            'password': to_password,
            'database': to_database,
            'charset': 'utf8',
        }



        self.from_conf_str = ('mysql+mysqldb://{username}:{password}@{host}:{port}/{database}'
                         '?charset={charset}'.format(**self.from_conf))
        self.to_conf_str = ('mysql+mysqldb://{username}:{password}@{host}:{port}/{database}'
                         '?charset={charset}'.format(**self.to_conf))
       
        self.from_engine = create_engine(self.from_conf_str)
        self.to_engine = create_engine(self.to_conf_str)

        print self.from_conf_str
        print self.to_conf_str

    def from_select_sqls(self, start_id=0, end_id=0,
                               start_date='0000-00-00', end_date='0000-00-00'):
        """存放着需要在源数据库中执行的SQL,并且标记了需要通过什么字段进行join
        """
        print start_id, end_id
        print start_date, end_date

        self.sqls = [
            {
                'sql': """
                    SELECT ASSOCIATOR_ID,
                        DELETE_FLAG,
                        SUBSIDY_RATE
                    FROM opt_associator
                    WHERE ASSOCIATOR_ID >= {start_id}
                        AND ASSOCIATOR_ID < {end_id}
                        AND CREATION_TIME < '{end_date}'
                """.format(start_id = start_id,
                           end_date = end_date,
                           end_id = end_id),
                'join_col': 'ASSOCIATOR_ID'
            },
            {
                'sql': """
                    SELECT
                        ASSOCIATOR_ID,
                        COUNT(CASE WHEN ORDER_STATUS IN (1,3,4) THEN 1 ELSE 0 END) AS ORDER_NUM,   
                        SUM(ORDER_AMOUNT) AS ORDER_TOTAL_AMOUNT,
                        SUM(CASE WHEN ORDER_STATUS IN (1,3,4) THEN ORDER_AMOUNT ELSE 0.0 END) AS ORDER_AMOUNT,			
                        SUM(CASE WHEN ORDER_STATUS IN (1,3,4) THEN PAY_MONEY ELSE 0.0 END) AS PAY_MONEY,						
                        SUM(CASE WHEN ORDER_STATUS IN (1,3,4) THEN SILVER_CATTLE ELSE 0.0 END) AS SILVER_CATTLE,  
                        SUM(CASE WHEN ORDER_STATUS IN (1,3,4) THEN RED_CATTLE ELSE 0.0 END) AS RED_CATTLE,   
                        SUM(CASE WHEN ORDER_STATUS IN (1,3,4) THEN DISCOUNT ELSE 0.0 END) AS DISCOUNT
                    FROM shop_line_order 
                    WHERE DELETE_FLAG = 0
                         AND ASSOCIATOR_ID >= {start_id}
                         AND ASSOCIATOR_ID < {end_id}
                         AND PAYMENT_TIME >= '{start_date}'
                         AND PAYMENT_TIME < '{end_date}'
                    GROUP BY ASSOCIATOR_ID
                """.format(start_id = start_id,
                           end_id = end_id,
                           start_date = start_date,
                           end_date = end_date),
                'join_col': 'ASSOCIATOR_ID'
            },
            {
                'sql': """
                    SELECT
                        slo.ASSOCIATOR_ID,
                        datediff('{start_date}', slo.NO_TRANSACTION_DAY) AS NO_TRANSACTION_DAY
                    FROM (
                       SELECT
                           ASSOCIATOR_ID,
                           MAX(PAYMENT_TIME) AS NO_TRANSACTION_DAY
                       FROM shop_line_order
                       WHERE DELETE_FLAG = 0
                           AND ASSOCIATOR_ID >= {start_id}
                           AND ASSOCIATOR_ID < {end_id}
                           AND CREATION_TIME < '{end_date}'
                       GROUP BY ASSOCIATOR_ID
                    ) AS slo
                """.format(start_id = start_id,
                           end_id = end_id,
                           start_date = start_date,
                           end_date = end_date),
                'join_col': 'ASSOCIATOR_ID'
            },
            {
                'sql': """
                    SELECT
                        scssi.ASSOCIATOR_ID,
                        scssi.SETTLE_MNY AS ALL_SETTLE_MNY,
                        scssi.SURPLUS_SETTLE_MNY,
                        scssi.AlREADY_SETTLE_MNY
                    FROM (
                        SELECT
                            ASSOCIATOR_ID,
                            SUM(ALL_SETTLE_MNY) AS SETTLE_MNY,
                            SUM(CASE WHEN STATE IN (1,3,4) THEN  ALL_SETTLE_MNY ELSE 0.0 END) AS SURPLUS_SETTLE_MNY,
                            SUM(CASE WHEN STATE IN (5) THEN  ALL_SETTLE_MNY ELSE 0.0 END) AS AlREADY_SETTLE_MNY
                        FROM sp_cost_settlement_sheet_item          
                            WHERE IS_SHOW = 1
                            AND DELETE_FLAG = 0
                            AND ASSOCIATOR_ID >= 0
                            AND ASSOCIATOR_ID < 10000
                            AND PAYMENT_TIME >= '2017-03-21'
                            AND PAYMENT_TIME < '2017-03-22'
                        GROUP BY ASSOCIATOR_ID
                    ) AS scssi
                """.format(start_id = start_id,
                           end_id = end_id,
                           start_date = start_date,
                           end_date = end_date),
                'join_col': 'ASSOCIATOR_ID'
            },
            {
                'sql': """
                    SELECT
                        ASSOCIATOR_ID,
                        SUM(MONEY) AS ALL_SUBSIDY_MNY,
                        SUM(CASE WHEN STATE IN (2,4,6,7) THEN MONEY ELSE 0.0 END) AS SURPLUS_SUBSIDY_MNY
                    FROM sp_subsidy_settlement_sheet_item
                    WHERE IS_SHOW = 1
                        AND DELETE_FLAG = 0
                        AND ASSOCIATOR_ID >= {start_id}
                        AND ASSOCIATOR_ID < {end_id}
                        AND PAYMENT_TIME >= '{start_date}'
                        AND PAYMENT_TIME < '{end_date}'
                    GROUP BY ASSOCIATOR_ID
                """.format(start_id = start_id,
                           end_id = end_id,
                           start_date = start_date,
                           end_date = end_date),
                'join_col': 'ASSOCIATOR_ID'
            },
            {
                'sql': """
                    SELECT
                        ASSOCIATOR_ID,
                        SUM(MONEY) AS ALL_PROFIT_MNY,
                        SUM(CASE WHEN STATE IN (2, 4, 6, 7) THEN  MONEY ELSE 0.0 END) AS SURPLUS_PROFIT_MNY,
                        SUM(CASE WHEN STATE IN (1) THEN  MONEY ELSE 0.0 END) AS NOCONFIRM_PROFIT_MNY,
                        SUM(CASE WHEN STATE IN (2, 6) THEN  MONEY ELSE 0.0 END) AS ISCONFIRM_PROFIT_MNY, 
                        SUM(CASE WHEN STATE IN (4, 7) THEN  MONEY ELSE 0.0 END) AS APPLYING_PROFIT_MNY,            
                        SUM(CASE WHEN STATE IN (5) THEN  MONEY ELSE 0.0 END) AS AlREADY_PROFIT_MNY
                    FROM sp_profit_settlement_sheet_item 
                    WHERE IS_SHOW = 1 
                        AND IS_WX_SELF = 0 
                        AND DELETE_FLAG = 0
                        AND ASSOCIATOR_ID >= {start_id}
                        AND ASSOCIATOR_ID < {end_id}
                        AND PAYMENT_TIME >= '{start_date}'
                        AND PAYMENT_TIME < '{end_date}'
                    GROUP BY ASSOCIATOR_ID
                """.format(start_id = start_id,
                           end_id = end_id,
                           start_date = start_date,
                           end_date = end_date),
                'join_col': 'ASSOCIATOR_ID'
            },
            {
                'sql': """
                    SELECT BIND_ASS_ID AS ASSOCIATOR_ID,
                        COUNT(1) AS ASSOCIATOR_NUM
                    FROM opt_my_associator
                    WHERE DELETE_FLAG = 0
                        AND BIND_ASS_ID IS NOT NULL
                        AND BIND_ASS_ID >= {start_id}
                        AND BIND_ASS_ID < {end_id}
                        AND CREATION_TIME >= '{start_date}'
                        AND CREATION_TIME < '{end_date}'
                    GROUP BY BIND_ASS_ID
                """.format(start_id = start_id,
                           end_id = end_id,
                           start_date = start_date,
                           end_date = end_date),
                'join_col': 'ASSOCIATOR_ID'
            },
            {
                'sql': """
                    SELECT BIND_ASS_ID AS ASSOCIATOR_ID,
                        COUNT(1) AS MEMBER_NUM									
                    FROM opt_my_member
                    WHERE DELETE_FLAG = 0
                        AND BIND_ASS_ID IS NOT NULL
                        AND BIND_ASS_ID >= {start_id}
                        AND BIND_ASS_ID < {end_id}
                        AND CREATION_TIME >= '{start_date}'
                        AND CREATION_TIME < '{end_date}'
                    GROUP BY BIND_ASS_ID
                """.format(start_id = start_id,
                           end_id = end_id,
                           start_date = start_date,
                           end_date = end_date),
                'join_col': 'ASSOCIATOR_ID'
            },
        ]

        for sql in self.sqls:
            print sql['sql']


    def init_all_df(self):
        """初始化构造所有需要的 dateframe
        """

        try:
            self.table_datas = [
                {
                    'df': pd.read_sql_query(item['sql'], self.from_engine),
                    'join_col': item['join_col']
                }
                for item in self.sqls
            ]
        except:
            self.table_datas = [
                {
                    'df': pd.read_sql_query(item['sql'], self.from_engine),
                    'join_col': item['join_col']
                }
                for item in self.sqls
            ]

    def join_df(self):
        """对所有的df进行join, 变量存放在 self.table_datas
        """
        self.df_final = None
        for table_data in self.table_datas:
            if not isinstance(self.df_final, pd.core.frame.DataFrame):
                self.df_final = table_data['df']
            else:
                self.df_final = self.df_final.merge(
                    table_data['df'],
                    how = 'left',
                    left_on = table_data['join_col'],
                    right_on = table_data['join_col']
                )

    def add_column(self, col_name='', value=None):
        """向self.df 中添加一个字段和值
        col_name: 需要添加的字段名
        value: 需要添加值
        """

        # 添加时间值 昨天
        self.df[col_name] = value

    def filter_none(self, value=0):
        """过滤df中None的值
        """
        # 将None数据变成0
        self.df = self.df_final.fillna(value)

    def change_col_type(self, col_name='', dtype=''):
        """过滤数据"""

        # 转化数据类型
        self.df[col_name] = df[col_name].astype(dtype)
        self.df[col_name] = df[col_name].astype(dtype)

    def rollback_col_data(self, df_col_name='', df_final_col_name=''):
        """将需要写入MySQL df 字段值，回滚成 final_df 中的字段值
        df_col_name: 需要写入 MySQL 的DataFrame
        df_final_col_name: 从ADS中查询出来的并且已经关联的 DataFrame
        """
        if not df_col_name and not df_final_col_name:
            return False
        elif not df_col_name and df_final_col_name:
            df_col_name = df_final_col_name
        elif df_col_name and not df_final_col_name:
            df_final_col_name = df_col_name
           
        self.df[df_col_name] = self.df_final[df_final_col_name]


    def df2mysql(self, table_name=None):
        self.df.to_sql(table_name,
                       self.to_engine,
                       if_exists= 'append',
                       index = False)

    def get_max_ass_id(self):
        sql = 'SELECT MAX(associator_id) AS associator_id FROM opt_associator'


def main():
    conf = {
        'from_host': '127.0.0.1',
        'from_port': '10008',
        'from_username': 'root',
        'from_password': 'root',
        'from_database': 'test',

        'to_host': '127.0.0.1',
        'to_port': '3306',
        'to_username': 'root',
        'to_password': 'root',
        'to_database': 'test',
    }

    # 开始统计的时间
    # start_date = datetime.date(2017, 04, 22)
    # end_date = datetime.date(2017, 04, 24)
    oneday = datetime.timedelta(days=1)
    start_date = datetime.date.today() - oneday
    end_date = datetime.date.today()

    while start_date < end_date:
        # 获得 start_date + 1天 的时间
        next_date = start_date + oneday

        # 初始化循环变量
        start_id = 0
        end_id = 10000
        insert_times = 10
        offset = 10000

        ads2mysql = ADS2MySQL(**conf)

        for i in range(insert_times):
            # 获取需要执行的sql
            ads2mysql.from_select_sqls(start_id = start_id,
                                       end_id = end_id,
                                       start_date = str(start_date),
                                       end_date = str(next_date))


            # 执行sql将数据保存到 dateframe 中
            ads2mysql.init_all_df()
           
            # 对查询出来的数据进行关联
            ads2mysql.join_df()

            # 过滤 None 值
            ads2mysql.filter_none()

            # 由于有些字段要保留从数据库查出来的值，需要rollback数据
            ads2mysql.rollback_col_data(df_col_name = 'NO_TRANSACTION_DAY',
                                        df_final_col_name = 'NO_TRANSACTION_DAY')

            # 添加日期字段
            ads2mysql.add_column(col_name = 'date_date', value = str(start_date))
            print ads2mysql.df
            print ads2mysql.df.dtypes

            # 数据写入MySQL
            ads2mysql.df2mysql(table_name='test')


            # 获取下一次循环ID
            start_id += offset
            end_id += offset
            print '======================== split ==============================='


        # 计算下一天开始的时间
        start_date += oneday

        # 手动回收内存
        del ads2mysql
        gc.collect()


if __name__ == '__main__':
    main()
