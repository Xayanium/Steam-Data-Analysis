# coding: utf-8
# @Author: Xayanium
import os.path

# from Cython.Utility.MemoryView import result
import json

global HbaseTableConn


# 游戏表格详情页展示数据
def get_table_data():
    try:
        # result = query_mysql("""
        #     select id, title, icon, platform, release_date, review_summary, final_price,
        #     types, description, video_link, developer, publisher
        #     from games
        # """, [], 'select')

        result=HbaseTableConn.scan(columns=['games:id','games:title','games:icon','games:platform',
            'games:release_date','games:review_summary','games:final_price','games:types',
            'games:description','games:video_link','games:developer','games:publisher'])

    except Exception:
        return []

    game_dict = []
    for row_key,x in result:
        game_dict.append(
            {
                'id': x[b'games:id'],
                'name': x[b'games:title'],
                'icon': x[b'games:icon'],
                'platform': list(json.loads(x[b'games:platform'])),
                'release_date': x[b'games:release_date'],
                'review': x[b'games:review_summary'].split('\u3002')[0],  # 截取中文句号前内容
                'price': x[b'games:final_price'],
                'types': list(json.loads(x[b'games:types'])),
                'description': x[b'games:description'],
                'video': x[b'games:video_link'],
                'firm': [x[b'games:developer'], x[b'games:publisher']]
            }
        )

    return game_dict


# 游戏搜索详情页展示数据
def get_search_data(title):
    try:
        if title:
            # result = query_mysql("""
            #     select * from games where title like %s
            # """, ['%' + title + '%'], 'select')[0]
            results=HbaseTableConn.scan(row_prefix=title,limit=1)
        else:
            # result = query_mysql("""
            #             select * from games where id=1
            #         """, [], 'select')[0]
            results=HbaseTableConn.scan(limit=1)
    except Exception:
        return {}

    for key,result in results:
        game_dict = {
            'id': result[b'games:id'],
            'name': result[b'games:title'],
            'icon': result[b'games:icon'],
            'platform': json.loads(result[b'games:platform']),
            'release_date': result[b'games:release_date'],
            'review_summary': result[b'games:view_summary'].split('\u3002')[0],  # 截取中文句号前内容
            'discount': result[b'games:discount'],
            'original_price': result[b'games:original_price'],
            'final_price': result[b'games:final_price'],
            'detail_link': result[b'games:detail_link'],
            'types': json.loads(result[b'games:type']),
            'description': result[b'games:description'],
            'developer': result[b'games:developer'],
            'publisher': result[b'games:publisher'],
            'image_link': result[b'games:image_link'],
            'video_link': result[b'games:video_link'],
            'review': json.loads(result[b'games:review']),
            'sys_requirements': json.loads(result[b'games:sys_requirements'])
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

