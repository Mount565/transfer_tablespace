import os, getopt, sys
import pymysql

# You must run this script on the source server， this is the way I suggest.
# Or run it on any server, thus needs module：pip install paramiko
# I'd like to make it depends as less modules as possible. sadly , it already depends on pymysql.

# python3 transfer_tablespace.py --source_host=192.168.216.11 --source_dir=/dbfiles/mysql_home/data --source_mysql_user=dba --source_mysql_password=dba --source_mysql_port=3306 --source_schema=test --row_format=compact --table=test
# --target_hosts=192.168.216.200,192.168.216.201,192.168.216.202 --target_mysql_user=dba --target_dir=/dbfiles/mysql_home/data --target_mysql_password=dba --target_mysql_port=3306 --target_schema=test

"""
Args:
--source_host
--source_dir
--source_mysql_user
--source_mysql_password
--source_mysql_port
--source_schema
# the source row_format, the target table must have the same row format with the source's
--row_format
# The table of which the tablespace to transfer , table name, the target host must has the same table name with the source
--table

# the first host must be the master if it is a master-slave architecture
--target_hosts
--target_mysql_user
--target_mysql_password
--target_mysql_port
--target_schema

"""


def usage():
    print("""
    Args:
--source_host
--source_dir
--source_mysql_user
--source_mysql_password
--source_mysql_port
--source_schema

# the source row_format, the target table must have the same row format with the source's
--row_format
# The table of which the tablespace to transfer , table name, the target host must has the same table name with the source
--table

# the first host must be the master,separated by ,
--target_hosts
--target_mysql_user
--target_mysql_password
--target_mysql_port
--target_schema
--target_dir
--target_ssh_user

    """)


def parse_arg(arglist, arg):
    try:
        opts, args = getopt.getopt(args=arglist, shortopts=None, longopts=["help", "source_host=", "source_dir=",
                                                                           "source_mysql_user=",
                                                                           "source_mysql_password=",
                                                                           "source_mysql_port=", "source_schema=",
                                                                           "row_format=", "table=", "target_ssh_user="
                                                                                                    "target_hosts=",
                                                                           "target_mysql_user=", "target_dir="
                                                                                                 "target_mysql_password=",
                                                                           "target_mysql_port=", "target_schema="])
        for k, v in opts:
            if k == arg:
                # print(v)
                return v
    except getopt.GetoptError:
        print("arg parse error")
        exit(1)


argv = sys.argv[1:]

if len(argv) != 15:
    usage()
    sys.exit(1)

# all the target hosts
target_hosts = []

# the master role of the target server, as the target servers maybe a replication cluster
target_master_host = ""
target_mysql_user = ""
target_mysql_password = ""
target_mysql_port = ""
target_schema = ""

target_dir = ""
# login target using ssh key
target_ssh_user = ""

source_host = ""
source_dir = ""
source_mysql_user = ""
source_mysql_password = ""
source_mysql_port = ""

table = ""
# source and target row format:Dynamic , Compact or others
# can be fetched by : show table status like 'table'
# we add this information to create table sql or we can just run the create table sql
# and after that run :alter table ... row_format=''
row_format = ""
source_schema = ""

ths = parse_arg("--target_hosts")
target_hosts = ths.split(",")
target_master_host = target_hosts[0]
target_mysql_user = parse_arg("--target_mysql_user")
target_mysql_password = parse_arg("--target_mysql_password")
target_mysql_port = parse_arg("--target_mysql_port")
target_schema = parse_arg("--target_schema")
target_dir = parse_arg("--target_dir")
target_ssh_user = parse_arg("--target_ssh_user")

source_host = parse_arg("--source_host")
source_dir = parse_arg("--source_dir")
source_mysql_user = parse_arg("--source_mysql_user")
source_mysql_password = parse_arg("--source_mysql_password")
source_mysql_port = parse_arg("--source_mysql_port")
source_schema = parse_arg("--source_schema")
table = parse_arg("--table")
row_format = parse_arg("--row_format")

create_sql = None
# source connection
source_conn = pymysql.connect(host=source_host, user=source_mysql_user, password=source_mysql_password,
                              port=source_mysql_port, db=source_schema)

source_cur = source_conn.cursor()
# fetch create table sql from source server
source_cur.execute("show create table {}".format(table))
rst = source_cur.fetchone()
if len(rst) > 0:
    create_sql = rst[1]
    # replace create table to create table if not exists to reduce possible errors
    str.replace(create_sql, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
    # print(create_sql)
else:
    print("Failed to fetch create table sql. Exit")
    exit(1)

# fetch the row_format of the table on source server
source_cur.execute("show table status like '{}'".format(table))
rst = source_cur.fetchone()
if len(rst) > 0:
    row_format = rst[3]
else:
    print("Failed to fetch row format. Exit")
    exit(1)

# lock table for export
source_cur.execute("flush table {} for export".format(table))

# target master connection, no need to connect to target slave
target_conn = pymysql.connect(host=target_master_host, user=target_mysql_user, password=target_mysql_password,
                              port=target_mysql_port, db=target_schema)

target_cur = target_conn.cursor()
# create table on taget master
target_cur.execute(create_sql)
# fix row_format if possible
target_cur.execute("alter table {} row_format={}".format(table, row_format))

print("Create table on target master successfully!")

# discard tablespace
target_cur.execute("ALTER TABLE {} DISCARD TABLESPACE;".format(table))

# transfer tablespaces to all the target servers
for target_host in target_hosts:
    os.system("scp {}.{ibd,cfg} {}@{}:{}".format(source_dir + "/" + table, target_ssh_user, target_host, target_dir))
      # change ownership to mysql
    os.system("ssh {}@{} '{}{}.{ibd,cfg}'".format(target_ssh_user, target_host, "sudo chown mysql.mysql ",
                                                  source_dir + "/" + table))

# unlock tables on source server
source_cur.execute("unlock tables")
source_cur.close()
source_conn.close()

# import tablespace on target master
target_cur.execute("ALTER TABLE receipt_info IMPORT TABLESPACE;")

target_cur.close()
target_conn.close()
