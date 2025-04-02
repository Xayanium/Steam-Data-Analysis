# coding: utf-8
# @Author: Xayanium

import os
import json
import sys
from pathlib import Path

import pymysql
from dbutils.pooled_db import PooledDB
from pyhive import hive

# current_platform = sys.platform  # 检测当前运行的平台
# if current_platform == 'linux':
    # import happybase


class QueryData:
    def __init__(self):
        self.project_path = Path(__file__).parent.parent.resolve()
        self.mysql_pool = self.mysql_connection()
        self.hive_config = self.get_hive_config()
        # self.hbase_pool = self.hbase_connection()

    def mysql_connection(self):
        with open(os.path.join(self.project_path, 'config.json'), 'r', encoding='utf-8') as f:
            config = json.load(f)['mysql']
            pool = PooledDB(
                creator=pymysql,  # 使用 pymysql 连接数据库
                maxconnections=10,  # 连接池允许的最大连接数，0 和 None 表示不限制连接数
                mincached=2,  # 初始化时，连接池中至少创建的空闲的连接，0 表示不创建
                maxcached=5,  # 连接池中最多闲置的连接，0 和 None 不限制
                maxshared=3,  # 连接池中最多共享的连接数量，0 和 None 表示全部共享
                blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
                maxusage=None,  # 一个连接最多被重复使用的次数，None 表示无限制
                ping=0,  # ping MySQL 服务端，检查是否服务可用, 0=Never
                **config  # 数据库连接配置
            )
            return pool
            
    def get_hive_config(self):
        with open(os.path.join(self.project_path, 'config.json'), 'r', encoding='utf-8') as f:
            return json.load(f)['hive']

    def query_hive(self, sql, args=None):
        """
        执行Hive查询，不使用pandas
        """
        config = self.hive_config
        conn = hive.Connection(
            host=config['host'],
            port=config['port'],
            database=config['database']
            # auth_mechanism=config.get('auth_mechanism', 'PLAIN')
        )
        cursor = conn.cursor()
        try:
            # 将参数替换到SQL语句中
            if args:
                for arg in args:
                    if isinstance(arg, str):
                        # 如果是字符串类型，需要添加引号
                        arg_quoted = f"'{arg.replace('%', '')}'"
                        sql = sql.replace('%s', arg_quoted, 1)
                    else:
                        sql = sql.replace('%s', str(arg), 1)
            
            # 执行Hive查询
            cursor.execute(sql)
            
            # 获取结果
            result = cursor.fetchall()
            return result
        except Exception as e:
            print(f"Hive查询错误: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    def query_mysql(self, sql, args, method):
        conn = self.mysql_pool.connection()  # 从连接池中获取连接
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, args)
                if method == 'select':
                    result = cursor.fetchall()
                    return result
                else:
                    conn.commit()
        except pymysql.MySQLError as e:
            print(e)
        finally:
            conn.close()  # 将连接返回连接池

    # def query_hbase(self, table_name, args, method, row_key=None):
    #     if current_platform == 'linux':
    #         with self.hbase_pool.connection() as conn:
    #             table = conn.table(table_name)
    #             if method == 'get_search_data':
    #                 return table.scan(row_prefix=args, limit=1)  # 返回生成器(仅获得一个查询数据)
    #             elif method == 'get_table_data':
    #                 return table.scan(columns=args)
    #             elif method == 'insert':
    #                 try:
    #                     table.put(row_key.encode('utf-8'), args)
    #                 except Exception as e:
    #                     print(e)
    #             # elif method == 'get_id':
    #             #     return table.counter_inc(b'row_counter', b'games:counter')
    #             elif method == 'create':
    #                 try:
    #                     conn.create_table(name=table_name, families={args: {}})
    #                 except Exception as e:
    #                     print("table: already exists:", e)


# def hbase_connection():
#     project_path = Path(__file__).parent.parent.resolve()
#     with open(os.path.join(project_path, 'config.json'), 'r', encoding='utf-8') as f:
#         config = json.load(f)['hbase']
#         pool = happybase.ConnectionPool(size=10, host=config['host'], port=int(config['port']))
#         return pool


if __name__ == '__main__':
    pass
