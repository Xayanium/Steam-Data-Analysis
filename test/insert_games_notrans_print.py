import os
import json
import pymysql
from pyhive import hive

def connect_to_hive():
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    hive_conf = config["hive"]
    conn = hive.Connection(
        host=hive_conf["host"],
        port=hive_conf["port"],
        database=hive_conf["database"]
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
    # cursor.execute("SET hive.default.fileformat.encoding=UTF-8")  # 此配置不被支持
    cursor.execute("SET mapreduce.output.fileoutputformat.output.encoding=UTF-8")
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
    
    # 查询数据前先检查编码
    cursor.execute("SELECT 'こんにちは世界' AS test")
    test_result = cursor.fetchone()
    print(f"编码测试 (日文'你好世界'): {test_result[0]}")
    
    # 查询数据
    cursor.execute("SELECT * FROM games")
    rows = cursor.fetchall()
    
    # 验证字符集是否正确
    cursor.execute("SHOW VARIABLES LIKE 'character_set%'")
    charset_settings = cursor.fetchall()
    print("MySQL字符集设置:", charset_settings)
    
    # 检查第一行数据的部分字段
    if rows and len(rows) > 0:
        print(f"第一条记录标题: {rows[0][1]}")
        print(f"第一条记录描述前50字符: {rows[0][11][:50] if rows[0][11] else 'None'}")
    
    return rows

def export_steam_to_hive(games, hive_cursor):
    hive_cursor.execute("USE steam_data")
    for row in games:
        values = []
        for value in row:
            if isinstance(value, str):
                # 确保字符串是正确的UTF-8编码 - 增强处理方式
                try:
                    # 先检测是否是有效的UTF-8
                    value.encode('utf-8')
                    clean_value = value.replace("'", "''")
                    values.append(f"'{clean_value}'")
                except UnicodeError:
                    # 如果不是有效的UTF-8，尝试不同的编码
                    try:
                        # 假设可能是latin1编码的数据
                        clean_value = value.encode('latin1').decode('utf-8')
                        clean_value = clean_value.replace("'", "''")
                        values.append(f"'{clean_value}'")
                    except:
                        print(f"处理字符串时出错，无法识别编码: {value}")
                        values.append("'编码错误'")
            elif value is None:
                values.append("NULL")
            else:
                values.append(str(value))
                
        insert_sql = f"INSERT INTO games VALUES ({', '.join(values)})"
        print("Executing SQL:", insert_sql[:100] + "...")  # 只打印前100个字符
        hive_cursor.execute(insert_sql)
        # 立即查询并打印这条记录，假设第一列为唯一标识id
        select_sql = f"SELECT * FROM games WHERE id = {row[0]}"
        hive_cursor.execute(select_sql)
        inserted_record = hive_cursor.fetchone()
        # print("Inserted record:", inserted_record)

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
