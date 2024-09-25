# coding: utf-8
# @Author: Xayanium

import pymysql


conn = pymysql.connect(
        host='localhost',
        user='root',
        password='yzy2004518',
        database='steam_data',
        port=3306,
        charset='utf8mb4'
    )

cursor = conn.cursor()

