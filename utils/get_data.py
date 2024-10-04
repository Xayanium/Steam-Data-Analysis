# coding: utf-8
# @Author: Xayanium
import os.path

# from Cython.Utility.MemoryView import result
import json
# from urllib.parse import to_bytes


# 游戏表格详情页展示数据
def get_table_data(hbase_table_conn):
    try:
        # result = query_mysql("""
        #     select id, title, icon, platform, release_date, review_summary, final_price,
        #     types, description, video_link, developer, publisher
        #     from games
        # """, [], 'select')

        result=hbase_table_conn.scan(columns=[b'games:id', b'games:title', b'games:icon', b'games:platform',
            b'games:release_date',b'games:review_summary',b'games:final_price',b'games:types',
            b'games:description',b'games:video_link',b'games:developer',b'games:publisher'])

    except Exception:
        return []

    game_dict = []
    for row_key,x in result:
        game_dict.append(
            {
                'id':int.from_bytes(x.get(b'games:id', b'\x00\x00\x00\x00\x00\x00\x00\x00')) ,
                'name': x.get(b'games:title',b'').decode('utf-8'),
                'icon': x.get(b'games:icon',b'').decode('utf-8'),
                'platform': list(json.loads(x.get(b'games:platform','[]'))),
                'release_date': x.get(b'games:release_date',b'').decode('utf-8'),
                'review': x.get(b'games:review_summary',b'').decode('utf-8').split('\u3002')[0],  # 截取中文句号前内容
                'price': x.get(b'games:final_price',b'').decode('utf-8'),
                'types': list(json.loads(x.get(b'games:types','[]'))),
                'description': x.get(b'games:description',b'').decode('utf-8'),
                'video': x.get(b'games:video_link',b'').decode('utf-8'),
                'firm': [x.get(b'games:developer',b'').decode('utf-8'), x.get(b'games:publisher',b'').decode('utf-8')]
            }
        )

    return game_dict


# 游戏搜索详情页展示数据
def get_search_data(hbase_table_conn, title):
    try:
        if title:
            # result = query_mysql("""
            #     select * from games where title like %s
            # """, ['%' + title + '%'], 'select')[0]
            results=hbase_table_conn.scan(row_prefix=title.encode('utf-8'), limit=1)
        else:
            # result = query_mysql("""
            #             select * from games where id=1
            #         """, [], 'select')[0]
            results=hbase_table_conn.scan(limit=1)
    except Exception:
        return {}

    for key,result in results:
        game_dict = {
            'id':int.from_bytes(result.get(b'games:id', b'\x00\x00\x00\x00\x00\x00\x00\x00')) ,
            'name': result.get(b'games:title',b'').decode('utf-8'),
            'icon': result.get(b'games:icon',b'').decode('utf-8'),
            'platform': list(json.loads(result.get(b'games:platform','[]'))),
            'release_date': result.get(b'games:release_date',b'').decode('utf-8'),
            'review_summary': result.get(b'games:review_summary',b'').decode('utf-8').split('\u3002')[0],  # 截取中文句号前内容
            'discount': result.get(b'games:discount',b'').decode('utf-8'),  #,
            'original_price': result.get(b'games:original_price',b'').decode('utf-8'),
            'final_price': result.get(b'games:final_price',b'').decode('utf-8'),
            'detail_link': result.get(b'games:detail_link',b'').decode('utf-8'),
            'types': result.get(b'games:type',b'').decode('utf-8'),
            'description': result.get(b'games:description',b'').decode('utf-8'),
            'developer': result.get(b'games:developer',b'').decode('utf-8'),
            'publisher': result.get(b'games:publisher',b'').decode('utf-8'),
            'image_link': result.get(b'games:image_link',b'').decode('utf-8'),
            'video_link': result.get(b'games:video_link',b'').decode('utf-8'),
            'review': json.loads(result.get(b'games:review',b'').decode('utf-8')),
            'sys_requirements': json.loads(result.get(b'games:sys_requirements',b'').decode('utf-8'))
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

