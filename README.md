# load_data2mysql
读取相关文件的数据并加载到数据库中(如excel, csv等数据)

···
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

···
