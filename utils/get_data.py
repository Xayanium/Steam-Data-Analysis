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
        results = query_conn.query_hive("""
            select id, title, icon, platform, release_date, review_summary, final_price,
            types, description, video_link, developer, publisher
            from games
        """)
    except Exception as e:
        print(f"获取表格数据错误: {e}")
        return []

    game_dict = []
    for result in results:
        game_dict.append(
            {
                'id': result[0],
                'name': result[1],
                'icon': result[2],
                'platform': list(json.loads(result[3])) if result[3] is not None else [],
                'release_date': result[4],
                'review': result[5].split('\u3002')[0] if result[5] is not None else '',  # 截取中文句号前内容
                'price': result[6],
                'types': list(json.loads(result[7])) if result[7] is not None else [],
                'description': result[8],
                'video': result[9],
                'firm': [result[10], result[11]]
            }
        )
    return game_dict


# 游戏搜索详情页展示数据
def get_search_data(query_conn: QueryData, title):
    try:
        if title:
            results = query_conn.query_hive("""
                select * from games where title like %s
            """, ['%' + title + '%'])[0]  # Hive查询后取第一条结果
        else:
            results = query_conn.query_hive("""
                select * from games where id=1
            """)[0]  # Hive查询后取第一条结果

    except Exception as e:
        print(f"搜索数据错误: {e}")
        return {}

    game_dict = {
        'id': results[0],
        'name': results[1],
        'icon': results[2],
        'platform': json.loads(results[3]) if results[3] is not None else [],
        'release_date': results[4],
        'review_summary': results[5].split('\u3002')[0] if results[5] is not None else '',  # 截取中文句号前内容
        'discount': results[6],
        'original_price': results[7],
        'final_price': results[8],
        'detail_link': results[9],
        'types': json.loads(results[10]) if results[10] is not None else [],
        'description': results[11],
        'developer': results[12],
        'publisher': results[13],
        'image_link': results[14],
        'video_link': results[15],
        'review': json.loads(results[16]) if results[16] is not None else {},
        'sys_requirements': json.loads(results[17]) if results[17] is not None else {}
    }
    return game_dict


# Mapreduce后的数据获取
def get_analysis_data(key):
    file_path = os.path.join(Path(__file__).parent.parent.resolve(), 'analyzer','SenrenBanka.json')
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)[key]
        return data


if __name__ == '__main__':
    pass
