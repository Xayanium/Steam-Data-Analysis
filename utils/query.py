# coding: utf-8
# @Author: Xayanium

import os
import pymysql
import json
from pathlib import Path


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
                cursor.execute(sql, args)
                if method == 'select':
                    result = cursor.fetchall()
                    return result
                else:
                    conn.commit()
        except pymysql.MySQLError as e:
            print('error: ', e)
        finally:
            conn.close()


if __name__ == '__main__':
    query_mysql('select * from user', [], 'select')
