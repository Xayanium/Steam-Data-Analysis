# coding: utf-8
# @Author: Xayanium

import os
import pymysql
import json
from pathlib import Path
import happybase
# from pymysql import connect
# from app import table_view

HbaseTableConn = None

def query_mysql(sql, args, method):
    project_path = Path(__file__).parent.parent.resolve()
    with open(os.path.join(project_path, 'config.json'), 'r', encoding='utf-8') as f:
        config = json.load(f)['mysql']
        conn = pymysql.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password'],
            charset=config['charset']
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, args)#拼接
                if method == 'select':
                    result = cursor.fetchall()#select
                    return result
                else:
                    conn.commit()#执行
        except pymysql.MySQLError as e:
            print('error: ', e)
        finally:
            conn.close()

def hbase_connection():
    project_path = Path(__file__).parent.parent.resolve()
    with open(os.path.join(project_path, 'config.json'), 'r', encoding='utf-8') as f:
        config = json.load(f)['hbase']

        pool=happybase.ConnectionPool(size=10,host=config['host'],port=int(config['port']))
        return pool
        # try :
        #     conn.create_table(name=config['table_name'],families={config['family']:dict()})
        # except Exception as e:
        #     print("table: already exists:",e)
        # finally:
        #     return conn.table(name=config['table_name'])




if __name__ == '__main__':
    query_mysql('select * from user', [], 'select')
