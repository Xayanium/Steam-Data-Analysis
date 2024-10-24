# -*- coding: utf-8 -*-
# @Time    : 2024/10/12 10:41
# @Author  : Xayanium

import os
import sys
import math
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)
from utils.query import QueryData

query = QueryData()

def save_to_hbase(start_id, end_id):
    results = query.query_mysql("""
        select * from games where id between %s and %s
    """, [start_id, end_id], 'select')
    for result in results:
        data_map = {
            b'games:id': result[0].to_bytes(length=math.ceil(result[0].bit_length() / 8), byteorder='big'),
            b'games:title': result[1],
            b'games:icon': result[2],
            b'games:platform': result[3],
            b'games:release_date': result[4],
            b'games:review_summary': result[5],
            b'games:discount': result[6],
            b'games:original_price': result[7],
            b'games:final_price': result[8],
            b'games:detail_link': result[9],
            b'games:types': result[10],
            b'games:description': result[11],
            b'games:developer': result[12],
            b'games:publisher': result[13],
            b'games:image_link': result[14],
            b'games:video_link': result[15],
            b'games:review': result[16],
            b'games:sys_requirements': result[17],
        }

        query.query_hbase('steam_data', data_map, 'insert', result[1])

if __name__ == '__main__':
    # 从mysql转存入HBASE, 更改起始id和结束id
    save_to_hbase(1, 125)

