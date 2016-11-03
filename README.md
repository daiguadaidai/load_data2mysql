# load_data2mysql
读取相关文件的数据并加载到数据库中(如excel, csv等数据)

##### 需要安装的Python模块
```
pip install sqlalchemy
pip install pandas
```

##### 基本使用方法
```
python load_data2mysql.py --help
usage: load_data2mysql.py [-h] --host HOST --port PORT --username USERNAME
                          --password PASSWORD --database DATABASE --table
                          TABLE --type TYPE --file FILE

Description: The script load a file data to mysql table

optional arguments:
  -h, --help           show this help message and exit
  --host HOST          Connect MySQL host
  --port PORT          Connect MySQL port
  --username USERNAME  Connect MySQL username
  --password PASSWORD  Connect MySQL password
  --database DATABASE  Select A database
  --table TABLE        Select A table
  --type TYPE          Input file type [excel | csv]
  --file FILE          Input file name [goods.xlsx | goods.csv]

python load_data2mysql.py \
    --host 192.168.1.233 \
    --port 3306 \
    --username HH \
    --password oracle \
    --database test \
    --table ord_express \
    --type excel \
    --file /tmp/ord_express.xlsx
```

## exec_concat_sql.py
此程序是使用执行SELECT 语句中的 execute_sql 的内容来再次在数据库中执行, 主要是为了小程度上解决MyCat不能使用多表更新

##### 需要拼凑的 SELECT 语句
在select语句中必须要有 execute_sql字段, 不然执行会报错

##### SELECT 语句

```
SELECT goods_id,
    MIN(retail_price),
    CONCAT('UPDATE pro_goods SET supplier_price=', MIN(pgs.retail_price), ' WHERE goods_id="', goods_id, '";') AS execute_sql
FROM pro_goods_sku AS pgs
GROUP BY goods_id LIMIT 0, 1000;
```
生成的 execute_sql 语句如下:
```
UPDATE pro_goods SET supplier_price=19.00 WHERE goods_id="353622052830208";
UPDATE pro_goods SET supplier_price=100.00 WHERE goods_id="354117941197824";
UPDATE pro_goods SET supplier_price=159.00 WHERE goods_id="354912289946624";
UPDATE pro_goods SET supplier_price=2.00 WHERE goods_id="357265838181376";
UPDATE pro_goods SET supplier_price=2.12 WHERE goods_id="357839261404160";
```

##### 使用方法
1. 编辑exec_concat_sql.py

`main()` 函数中有一个 `sql` 变量修改它就好, 换成自己的SQL就好
```
sql = """
SELECT goods_id,
    MIN(retail_price),
    CONCAT('UPDATE pro_goods SET supplier_price=', MIN(pgs.retail_price), ' WHERE goods_id="', goods_id, '";') AS execute_sql
FROM pro_goods_sku AS pgs
GROUP BY goods_id LIMIT 0, 1000;
"""

python exec_concat_sql.py
```

## select_data2mysql.py
该程序是将SELECT语句获得的结果在Insert到另外一个数据库表中(同一个库也是可以的),
程序可以设置需要更新的字段, 如果在Insert出现`DUPLICATE`的错误将对你指定的字段进行更新

#####  使用方法
1. 编写SELECT语句
```
sql = """
SELECT express_id,
    express_name,
    express_code,
    create_time
FROM ord_express_bak;
"""
```

2. 编辑`select_data2mysql.py`文件中数据库的配置

```
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
```

3. 设置出现插入重复需要更新的字段, 和每次插入数据的大小

```
select_data2mysql = SelectData2Mysql(**conf)
select_data2mysql.bind_dml_table('ord_express') # 需要执行Insert语句的表
select_data2mysql.execute_select_sql(sql)
# 设置每次Insert的大小和重复时需要更新的字段
select_data2mysql.execute_insert_dup_update(size=10000, cols=['create_time'])
```

> **Tips**: 由于有时候SELECT出来的数据太多，所以要分批次进行Insert, 具体每次Insert多少要根据自己的实际环境要确定.从而避免在数据库中执行大事务

## read_multi_line2str
读取文本文件中的每一行数据, 并指定一个结束符从而将多行变成一个字符串输出. 如下:
```
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
```

##### 效果展现
具体如何使用请看文件中的main方法
```
python read_multi_line2str.py
```

## create_class_property
通过一个实例生成类的 `getter` `setter` 方法

**主要代码解释** 
```
def main():
    import inspect
    import BackupBase # 导入需要生成 getter 和 setter 方法的类

    obj = BackupBase() # 实例化对象
    obj_class_name = obj.__class__.__name__ # 获得实例的类的名称

    for name, value in inspect.getmembers(obj): # 循环类的属性
        # 判断是否是私有变量
        if (name.startswith('_') and not name.startswith('__')
           and not name.startswith('_{class_name}'.format(class_name=obj_class_name))
           and not inspect.isroutine(value)):

            property = name.lstrip('_')
            create_getter_setter(property) # 创建getter setter 方法
```

**运行/结果:**
```
python create_class_property.py

    @property
    def cmd_path(self):
        """cmd_path 是一个属性 - getter 方法"""
        return self._cmd_path

    @cmd_path.setter
    def cmd_path(self, value):
        """cmd_path属性的 setter 方法"""
        self._cmd_path = value

    ...

    @property
    def remote_dir(self):
        """remote_dir 是一个属性 - getter 方法"""
        return self._remote_dir

    @remote_dir.setter
    def remote_dir(self, value):
        """remote_dir属性的 setter 方法"""
        self._remote_dir = value
```

