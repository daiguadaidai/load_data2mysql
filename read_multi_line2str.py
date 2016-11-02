#!/usr/bin/env python
#-*- coding:utf-8 -*-

from sqlalchemy import create_engine
import traceback
import sys

reload(sys)
sys.setdefaultencoding('utf8')


class ReadMultiLine2Str(object):
    """指定一个结束字符,将文件分成多个块
    如, 读取的文本如下:
    aaaaaaaaaaaaaaa.
    bbbbbbbbbbbbbbb,
    ccccccccccccccc-
    ddddddddddddddd
    上面文本如果以 逗号(,)未终止字符文本将分为两字符串
    第一个字符串:
    aaaaaaaaaaaaaaa.
    bbbbbbbbbbbbbbb,
    第二个字符串
    ccccccccccccccc-
    ddddddddddddddd
    """

    def __init__(self, file_name, terminal='\n'):
        self.file_name = file_name
        self.terminal = terminal

    def get_lines(self):
        """读取文件每一行并且返回数据多行字符串生成器"""
        with open(self.file_name) as f:
            lines = []
            for line in f:
                if self.terminal == '\n': yield line # 如果以 \n 结尾则直接返回一行

                strip_line = line.strip() # 每一行去除头尾空白
                if not strip_line: continue

                lines.append(line) # 读取的行有数据则加入列表

                # 如果碰到指定的结尾返回组装的字符串
                if strip_line.endswith(self.terminal):
                    yield ''.join(lines)
                    lines = []

            # 如果文件的最后一行没有 指定的结尾字符则也算是有数据
            if lines: yield ''.join(lines)


def main():

    ## 设置数据库配置
    conf = {
        'host': '192.168.1.233',
        'port': 3306,
        'user': 'HH',
        'passwd': 'oracle',
        'db': 'test',
        'charset': 'utf8'
    }
    db_str = ('mysql+mysqldb://{username}:{password}@{host}:{port}/{database}'
              '?charset=utf8'.format(username = conf.get('user', ''),
                                     password = conf.get('passwd', ''),
                                     host = conf.get('host', ''),
                                     port = conf.get('port', 3306),
                                     database = conf.get('db', '')))
    engine = create_engine(db_str)
    conn = engine.connect()
    conn.execute('SET AUTOCOMMIT=1') # 设置自动提交

    file_name = 'tmp/sql.txt' # 设置读取SQL语句的文件
    terminal = ';' # 指定终止字符
    # 获取读取文件的实例
    read_multi_line2str = ReadMultiLine2Str(file_name, terminal=terminal)

    for line in read_multi_line2str.get_lines(): # 获取每一行条SQL并且进行执行
        try:
            print line
            conn.execute(line) # 设置自动提交
        except Exception as e:
            print '!! execute failure !!'
            print traceback.format_exc()
        else:
            print 'execute successful !'
        finally:
            print '--------------------------'

    conn.close()

if __name__ == '__main__':
    main()
