# -*- coding: utf-8 -*-
# @Time    : 2024/10/13 13:32
# @Author  : Xayanium

import csv
import pymysql
from pymysql.cursors import DictCursor
from pathlib import Path
import os
import json
import subprocess

def create_csv():
    config_path = Path(__file__).parent.parent.resolve()
    with open(os.path.join(config_path, 'config.json'), 'rt', encoding='utf-8') as f:
        config = json.load(f)['mysql']
        connection = pymysql.connect(cursorclass=DictCursor, **config)
    try:
        with connection.cursor(DictCursor) as cursor:
            cursor.execute("""select * from games""")
            rows = cursor.fetchall()
            with open('games.csv', 'w', newline='', encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file)
                for row in rows:
                    writer.writerow(row.values())
    except Exception as e:
        print(e)
    finally:
        connection.close()


def exec_mapreduce():
    result = subprocess.run('python3 deal_data.py games.csv', shell=True, capture_output=True)
    print('output: ', result.stdout.decode('utf-8'))
    print('error: ', result.stderr.decode('utf-8'))


if __name__ == '__main__':
    create_csv()
    exec_mapreduce()

