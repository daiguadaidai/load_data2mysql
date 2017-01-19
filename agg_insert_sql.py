#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
import pandas as pd
import datetime
import gc
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

class RetailReport(object):
    _engine = None

    def __init__(self, engine):
        self.engine = engine

    @property
    def engine(self):
        """engine 是一个属性 - getter 方法"""
        return self._engine

    @engine.setter
    def engine(self, value):
        """engine 属性的 setter 方法"""
        self._engine = value

    def get_sale_total_table_by_time(self, start_time='1979-01-01 00:00:00',
                                           end_time='2200-01-1 00:00:00'):
        """获得销售总表"""
        sqls = [
            {
                'sql': """
                    SELECT order_id
                    FROM ord_order
                    WHERE order_type IN(2, 3, 4, 5, 7)
                """,
                'join_col': 'order_id',
                'how': 'inner',
            },
            {
                'sql': """
                    SELECT order_goods_sku_id,
                        order_id,
                        goods_id,
                        num AS sku_num
                    FROM ord_order_goods_sku
                """,
                'join_col': 'order_id',
                'how': 'inner',
            },
            {
                'sql': """
                    SELECT order_goods_sku_id,
                        after_sale_goods_num
                    FROM ord_after_sale_sku
                """,
                'join_col': 'order_goods_sku_id',
                'how': 'left',
            },
        ]
        
        table_datas = [
            {
                'df': pd.read_sql_query(item['sql'], self.engine),
                'join_col': item['join_col'],
                'how': item['how'],
            }
            for item in sqls
        ]
        
        final_df = None
        for table_data in table_datas:
            if not isinstance(final_df, pd.core.frame.DataFrame):
                final_df = table_data['df']
            else:
                final_df = final_df.merge(
                    table_data['df'],
                    how=table_data['how'],
                    left_on=table_data['join_col'],
                    right_on=table_data['join_col']
                )
        
        # 定义需要聚合的列
        agg_col = {
            'sku_num': 'sum',
            'after_sale_goods_num': 'sum',
        }
        
        group_df = final_df.groupby(['goods_id']).agg(agg_col)
        self.agg_df = group_df.fillna(0)

        # 删除没用变量空出内存
        del final_df
        del group_df

        gc.collect()

    def add_sharding_key(self, table_name='', pri_id='', join_col='',
                               how='inner', where='', is_use_pri=False):
        """为聚合的结构添加sharding_key"""

        sql_param = {
            'table_name': table_name,
            'pri_id': pri_id,
            'where': 'WHERE {where}'.format(where=where) if where else ''
        }

        item = {
            'sql': """
                SELECT {pri_id},
                    sharding_key
                FROM {table_name}
                {where}
            """.format(**sql_param),
            'join_col': pri_id,
            'how': how,
        }

        if is_use_pri:
            sharding_df = pd.read_sql_query(item['sql'], self.engine, index_col=pri_id)

            self.agg_df = self.agg_df.merge(
                sharding_df,
                how=item['how'],
                left_index = True,
                right_index = True,
            )
        else:
            sharding_df = pd.read_sql_query(item['sql'], self.engine),

            self.agg_df = self.agg_df.merge(
                sharding_df,
                how=item['how'],
                left_on=item['join_col'],
                right_on=item['join_col']
            )

        self.agg_df['sharding_key'] = self.agg_df['sharding_key'].astype(object)
      

    def get_stat(self):
        """操作统计"""
        self.agg_df['sales_num'] = self.agg_df['sku_num'] - self.agg_df['after_sale_goods_num']

    def create_insert_sql(self):
        """根据agg_df获得相关的INSERT SQL"""

        row_size = self.agg_df.shape[0] # 获取行数

        header = 'INSERT INTO pro_goods(goods_id, real_sales_num, sharding_key, category_id, supplier_id, type_id, brand_id, name, new_time, carriage_time, article_no) VALUES'
        print header

        i = 0
        for index, row in self.agg_df.iterrows():
            i += 1

            comma = ','

            if i == row_size:
                comma = ''

            values_str = '({id}, {num}, {sharding_key}, 0, 0, 0, 0, "", "1991-01-01", "1991-01-01", ""){comma}'.format(
                                                 id = index,
                                                 num = row['sales_num'],
                                                 sharding_key = row['sharding_key'],
                                                 comma = comma)
            print values_str


        tail = 'ON DUPLICATE KEY UPDATE real_sales_num = VALUES(real_sales_num);'
        print tail


def main():
    conf = {
        'host': 'xxx',
        'port': '3306',
        'username': 'xxx',
        'password': 'xxx',
        'database': 'xxx',
        'charset': 'utf8',
    }
    
    conf_str = ('mysql+mysqldb://{username}:{password}@{host}:{port}/{database}'
                '?charset={charset}'.format(**conf))

    engine = create_engine(conf_str)

    # 获得上(周一)到(周日)的时间
    end_time = datetime.datetime.now() + datetime.timedelta(days=-1)

    retail_report = RetailReport(engine=engine)

    # 获取销售总表
    retail_report.get_sale_total_table_by_time()
    retail_report.get_stat()
    retail_report.add_sharding_key(table_name = 'pro_goods',
                                   pri_id = 'goods_id',
                                   join_col = 'goods_id',
                                   how = 'inner',
                                   where = '',
                                   is_use_pri = True)

    retail_report.create_insert_sql()
 
if __name__ == '__main__':
    main()
