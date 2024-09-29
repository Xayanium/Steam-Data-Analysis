# coding: utf-8
# @Author: Xayanium
import os.path

from utils.query import query_mysql
import json


# 游戏表格详情页展示数据
def get_table_data():
    try:
        result = query_mysql("""
            select id, title, icon, platform, release_date, review_summary, final_price, 
            types, description, video_link, developer, publisher 
            from games
        """, [], 'select')
    except Exception:
        return []

    game_dict = []
    for x in result:
        game_dict.append(
            {
                'id': x[0],
                'name': x[1],
                'icon': x[2],
                'platform': list(json.loads(x[3])),
                'release_date': x[4],
                'review': x[5].split('\u3002')[0],  # 截取中文句号前内容
                'price': x[6],
                'types': list(json.loads(x[7])),
                'description': x[8],
                'video': x[9],
                'firm': [x[10], x[11]]
            }
        )

    return game_dict


# 游戏搜索详情页展示数据
def get_search_data(title):
    try:
        if title:
            result = query_mysql("""
                select * from games where title like %s
            """, ['%' + title + '%'], 'select')[0]
        else:
            result = query_mysql("""
                        select * from games where id=1
                    """, [], 'select')[0]
    except Exception:
        return {}

    game_dict = {
        'id': result[0],
        'name': result[1],
        'icon': result[2],
        'platform': json.loads(result[3]),
        'release_date': result[4],
        'review_summary': result[5].split('\u3002')[0],  # 截取中文句号前内容
        'discount': result[6],
        'original_price': result[7],
        'final_price': result[8],
        'detail_link': result[9],
        'types': json.loads(result[10]),
        'description': result[11],
        'developer': result[12],
        'publisher': result[13],
        'image_link': result[14],
        'video_link': result[15],
        'review': json.loads(result[16]),
        'sys_requirements': json.loads(result[17])
    }
    return game_dict


# Mapreduce后的数据获取
def get_analysis_data(key):
    dir_path = os.path.abspath(os.path.dirname(__file__))
    path = os.path.join(dir_path, 'SenrenBanka.json')
    with open(path, 'r', encoding='utf-8') as file:
        data = json.load(file)[key]
        return data


if __name__ == '__main__':
    # result = get_analysis_data('prices')
    # for k, v in result.items():
    #     print(k, v)
    print(get_analysis_data('types'))
    pass

