# coding: utf-8
# @Author: Xayanium

import os.path
import json
from pathlib import Path

from spiders.save_to_hbase import query
from utils.query import QueryData


# 游戏表格详情页展示数据
def get_table_data(query_conn: QueryData):
    try:
        results = query_conn.query_mysql("""
            select id, title, icon, platform, release_date, review_summary, final_price,
            types, description, video_link, developer, publisher
            from games
        """, [], 'select')

        # result = hbase_table_conn.scan(columns=[b'games:id', b'games:title', b'games:icon', b'games:platform',
        #                                         b'games:release_date', b'games:review_summary', b'games:final_price',
        #                                         b'games:types',
        #                                         b'games:description', b'games:video_link', b'games:developer',
        #                                         b'games:publisher'])
        # with hbase_conn.connection() as conn:
        #     table = conn.table('steam_data')
        #     result = table.scan(
        #         columns=[b'games:id', b'games:title', b'games:icon', b'games:platform',
        #                  b'games:release_date', b'games:review_summary', b'games:final_price',
        #                  b'games:types',
        #                  b'games:description', b'games:video_link', b'games:developer',
        #                  b'games:publisher']
        #     )
        # result = query.query_hbase(
        #     table_name='steam_data',
        #     args=[b'games:id', b'games:title', b'games:icon', b'games:platform',
        #      b'games:release_date', b'games:review_summary', b'games:final_price',
        #      b'games:types',
        #      b'games:description', b'games:video_link', b'games:developer',
        #      b'games:publisher'],
        #     method='get_table_data'
        # )
    except Exception:
        return []

    game_dict = []
    for result in results:
        game_dict.append(
            {
                'id': result[0],
                'name': result[1],
                'icon': result[2],
                'platform': list(json.loads(result[3])),
                'release_date': result[4],
                'review': result[5].split('\u3002')[0],  # 截取中文句号前内容
                'price': result[6],
                'types': list(json.loads(result[7])),
                'description': result[8],
                'video': result[9],
                'firm': [result[10], result[11]]
            }
        )
    # for row_key, x in result:
    #     game_dict.append(
    #         {
    #             'id': int.from_bytes(x.get(b'games:id', b'\x00\x00\x00\x00\x00\x00\x00\x00'), byteorder='big'),
    #             'name': x.get(b'games:title', b'').decode('utf-8'),
    #             'icon': x.get(b'games:icon', b'').decode('utf-8'),
    #             'platform': list(json.loads(x.get(b'games:platform', '[]'))),
    #             'release_date': x.get(b'games:release_date', b'').decode('utf-8'),
    #             'review': x.get(b'games:review_summary', b'').decode('utf-8').split('\u3002')[0],  # 截取中文句号前内容
    #             'price': x.get(b'games:final_price', b'').decode('utf-8'),
    #             'types': list(json.loads(x.get(b'games:types', '[]'))),
    #             'description': x.get(b'games:description', b'').decode('utf-8'),
    #             'video': x.get(b'games:video_link', b'').decode('utf-8'),
    #             'firm': [x.get(b'games:developer', b'').decode('utf-8'), x.get(b'games:publisher', b'').decode('utf-8')]
    #         }
    #     )
    return game_dict


# 游戏搜索详情页展示数据
def get_search_data(query_conn: QueryData, title):
    try:
        if title:
            results = query_conn.query_mysql("""
                select * from games where title like %s
            """, ['%' + title + '%'], 'select')[0]
            # results = hbase_table_conn.scan(row_prefix=title.encode('utf-8'), limit=1)  # 返回一个生成器
            # results = query.query_hbase(
            #     table_name='steam_data',
            #     args=title.encode('utf-8'),
            #     method='get_search_data'
            # )
            # with hbase_conn.connection() as conn:
            #     table = conn.table('steam_data')
            #     results = table.scan(row_prefix=title.encode('utf-8'), limit=1)

        else:
            results = query_conn.query_mysql("""
                        select * from games where id=1
                    """, [], 'select')[0]
            # results = hbase_table_conn.scan(limit=1)  # 返回一个生成器
            # results = query.query_hbase(
            #     table_name='steam_data',
            #     args=None,
            #     method='get_search_data'
            # )
            # with hbase_conn.connection() as conn:
            #     table = conn.table('steam_data')
            #     results = table.scan(limit=1)

    except Exception:
        return {}

    game_dict = {
        'id': results[0],
        'name': results[1],
        'icon': results[2],
        'platform': json.loads(results[3]),
        'release_date': results[4],
        'review_summary': results[5].split('\u3002')[0],  # 截取中文句号前内容
        'discount': results[6],
        'original_price': results[7],
        'final_price': results[8],
        'detail_link': results[9],
        'types': json.loads(results[10]),
        'description': results[11],
        'developer': results[12],
        'publisher': results[13],
        'image_link': results[14],
        'video_link': results[15],
        'review': json.loads(results[16]),
        'sys_requirements': json.loads(results[17])
    }
    # for key, result in results:
    #     game_dict = {
    #         'id': int.from_bytes(result.get(b'games:id', b'\x00\x00\x00\x00\x00\x00\x00\x00'), byteorder='big'),
    #         'name': result.get(b'games:title', b'').decode('utf-8'),
    #         'icon': result.get(b'games:icon', b'').decode('utf-8'),
    #         'platform': list(json.loads(result.get(b'games:platform', '[]'))),
    #         'release_date': result.get(b'games:release_date', b'').decode('utf-8'),
    #         'review_summary': result.get(b'games:review_summary', b'').decode('utf-8').split('\u3002')[0],  # 截取中文句号前内容
    #         'discount': result.get(b'games:discount', b'').decode('utf-8'),  #,
    #         'original_price': result.get(b'games:original_price', b'').decode('utf-8'),
    #         'final_price': result.get(b'games:final_price', b'').decode('utf-8'),
    #         'detail_link': result.get(b'games:detail_link', b'').decode('utf-8'),
    #         'types': result.get(b'games:type', b'').decode('utf-8'),
    #         'description': result.get(b'games:description', b'').decode('utf-8'),
    #         'developer': result.get(b'games:developer', b'').decode('utf-8'),
    #         'publisher': result.get(b'games:publisher', b'').decode('utf-8'),
    #         'image_link': result.get(b'games:image_link', b'').decode('utf-8'),
    #         'video_link': result.get(b'games:video_link', b'').decode('utf-8'),
    #         'review': json.loads(result.get(b'games:review', b'').decode('utf-8')),
    #         'sys_requirements': json.loads(result.get(b'games:sys_requirements', b'').decode('utf-8'))
    #     }
    return game_dict


# Mapreduce后的数据获取
def get_analysis_data(key):
    file_path = os.path.join(Path(__file__).parent.parent.resolve(), 'analyzer','SenrenBanka.json')
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)[key]
        return data


if __name__ == '__main__':
    pass
