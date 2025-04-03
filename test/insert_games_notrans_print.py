import os
import json
import pymysql
from pyhive import hive
import sys
import locale

# 设置终端locale以支持UTF-8输出
try:
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
except:
    pass

# 检查终端编码
print(f"终端编码: {sys.stdout.encoding}")

def connect_to_hive():
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    hive_conf = config["hive"]
    conn = hive.Connection(
        host=hive_conf["host"],
        port=hive_conf["port"],
        # database=hive_conf["database"],
        username=hive_conf.get("username", "hive")
    )
    return conn

def connect_to_mysql():
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    mysql_conf = config["mysql"]
    conn = pymysql.connect(
        host=mysql_conf["host"],
        port=mysql_conf["port"],
        user=mysql_conf["user"],
        password=mysql_conf["password"],
        database=mysql_conf["database"],
        charset="utf8mb4",
        use_unicode=True,
        autocommit=True
    )
    return conn

def create_hive_steam_table(cursor):
    # 设置会话级别的编码相关参数，仅设置支持的参数
    cursor.execute("SET mapreduce.output.fileoutputformat.output.encoding=UTF-8")
    # 确保steam_data数据库存在
    cursor.execute("CREATE DATABASE IF NOT EXISTS steam_data")
    cursor.execute("USE steam_data")
    cursor.execute("DROP TABLE IF EXISTS games")
    create_table_sql = """
    CREATE TABLE games (
        id INT,
        title STRING,
        icon STRING,
        platform STRING,
        release_date STRING,
        review_summary STRING,
        discount STRING,
        original_price STRING,
        final_price STRING,
        detail_link STRING,
        types STRING,
        description STRING,
        developer STRING,
        publisher STRING,
        image_link STRING,
        video_link STRING,
        review STRING,
        sys_requirements STRING
    )
    STORED AS TEXTFILE
    TBLPROPERTIES ('textfile.encoding'='UTF-8')
    """
    cursor.execute(create_table_sql)
    print("Hive steam表已清空并重建")

def fetch_steam_from_mysql(cursor):
    # 确保MySQL连接使用正确的字符集
    cursor.execute("SET NAMES utf8mb4")
    cursor.execute("SET CHARACTER SET utf8mb4")
    cursor.execute("SET character_set_connection=utf8mb4")
    cursor.execute("SET collation_connection=utf8mb4_unicode_ci")
    
    # 查询数据
    cursor.execute("SELECT * FROM games")
    rows = cursor.fetchall()
    
    # 处理数据以确保字符集正确
    processed_rows = []
    for row in rows:
        processed_row = []
        for value in row:
            if isinstance(value, str):
                # 尝试处理可能的编码问题
                try:
                    # 尝试Latin1->UTF8转换，解决一些常见的编码问题
                    decoded_value = value.encode('latin1').decode('utf-8')
                    processed_row.append(decoded_value)
                except:
                    # 如果失败则保留原始值
                    processed_row.append(value)
            else:
                processed_row.append(value)
        processed_rows.append(tuple(processed_row))
    
    # 检查第一行数据的部分字段
    if processed_rows and len(processed_rows) > 0:
        print(f"第一条记录标题: {processed_rows[0][1]}")
        print(f"第一条记录描述前50字符: {processed_rows[0][11][:50] if processed_rows[0][11] else 'None'}")
    
    return processed_rows

def export_steam_to_hive(games, hive_cursor):
    hive_cursor.execute("USE steam_data")
    for row in games:
        values = []
        for value in row:
            if isinstance(value, str):
                # 确保字符串符合Hive SQL要求
                try:
                    # 先确保是有效的UTF-8
                    clean_value = value.replace("'", "''")
                    values.append(f"'{clean_value}'")
                except UnicodeError:
                    # 如果有Unicode问题，使用适当的替代字符
                    values.append("'无法显示的文本'")
            elif value is None:
                values.append("NULL")
            else:
                values.append(str(value))
                
        insert_sql = f"INSERT INTO games VALUES ({', '.join(values)})"
        print("Executing SQL:", insert_sql[:100] + "...")  # 只打印前100个字符
        hive_cursor.execute(insert_sql)
        
        # 立即查询并打印这条记录
        select_sql = f"SELECT id, title FROM games WHERE id = {row[0]}"
        hive_cursor.execute(select_sql)
        inserted_record = hive_cursor.fetchone()
        print(f"已插入记录ID: {row[0]}, 标题: {inserted_record[1] if inserted_record else 'Unknown'}")

def main():
    # 从MySQL中获取steam表数据
    mysql_conn = connect_to_mysql()
    mysql_cursor = mysql_conn.cursor()
    games = fetch_steam_from_mysql(mysql_cursor)
    print(f"Fetched {len(games)} records from MySQL games table")
    
    # 连接Hive，重建steam表后导入数据并打印每条记录
    hive_conn = connect_to_hive()
    hive_cursor = hive_conn.cursor()
    create_hive_steam_table(hive_cursor)
    export_steam_to_hive(games, hive_cursor)
    
    mysql_cursor.close()
    mysql_conn.close()
    hive_cursor.close()
    hive_conn.close()

if __name__ == '__main__':
    main()
