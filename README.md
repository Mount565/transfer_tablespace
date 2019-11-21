# transfer_tablespace
Transfer MySQL tablespace from source server to target server 

## Run:
```
python3 transfer_tablespace.py --source_host=192.168.216.11 --source_dir=/dbfiles/mysql_home/data --source_mysql_user=dba --source_mysql_password=dba --source_mysql_port=3306 --source_schema=test --row_format=compact --table=test --target_hosts=192.168.216.200,192.168.216.201,192.168.216.202 --target_mysql_user=dba --target_dir=/dbfiles/mysql_home/data --target_mysql_password=dba --target_mysql_port=3306 --target_schema=test
```
