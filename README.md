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

使用方法:
##### 1.编辑exec_concat_sql.py
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
