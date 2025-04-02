from pyhive import hive
import os
import json
import pymysql

def connect_to_hive():
    """连接到Hive服务器"""
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    hive_conf = config["hive"]
    conn = hive.Connection(
        host=hive_conf["host"],
        port=hive_conf["port"]
        # 移除不支持的hive.cli.encoding配置
    )
    return conn

def connect_to_mysql():
    """连接到MySQL数据库"""
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
        charset=mysql_conf["charset"]
    )
    return conn

def create_database(cursor):
    """创建steam_data数据库"""
    cursor.execute("CREATE DATABASE IF NOT EXISTS steam_data")
    print("数据库创建成功或已存在")

def create_games_table(cursor):
    """清空数据并重建games表"""
    cursor.execute("USE steam_data")
    cursor.execute("DROP TABLE IF EXISTS `games`")
    
    # 移除可能不支持的配置项
    cursor.execute("SET hive.exec.compress.output=false")
    
    create_table_sql = """
    CREATE TABLE `games` (
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
    """
    cursor.execute(create_table_sql)
    print("games表已清空并重建")

def fetch_games_from_mysql(mysql_cursor):
    """从MySQL中获取games表数据"""
    mysql_cursor.execute("SELECT * FROM games")
    return mysql_cursor.fetchall()

def export_games_to_hive(games, hive_cursor):
    """将MySQL导出的games数据写入Hive，处理UTF-8字符集"""
    # 只保留必要的设置
    hive_cursor.execute("SET hive.exec.compress.output=false")
    hive_cursor.execute("USE steam_data")
    
    for row in games:
        values = []
        for value in row:
            if isinstance(value, str):
                # 确保字符串正确编码并进行彻底的转义
                # 先解码再编码确保字符串是有效的UTF-8
                try:
                    # 双重转义单引号，先替换为两个单引号，这是SQL标准的转义方式
                    clean_value = value.replace("'", "''")
                    values.append(f"'{clean_value}'")
                except UnicodeError:
                    # 如果遇到编码问题，尝试强制转换
                    clean_value = value.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                    clean_value = clean_value.replace("'", "''")
                    values.append(f"'{clean_value}'")
            elif value is None:
                values.append("NULL")
            else:
                values.append(str(value))
        
        insert_sql = f"INSERT INTO `games` VALUES ({', '.join(values)})"
        print(f"正在执行插入语句...")
        try:
            hive_cursor.execute(insert_sql)
            print(f"数据插入成功")
        except Exception as e:
            print(f"插入失败: {e}")
            print(f"问题SQL: {insert_sql[:200]}...")  # 只打印前200个字符避免日志过长

def execute_sql_file(cursor, sql_file_path):
    """执行SQL文件"""
    try:
        with open(sql_file_path, 'r') as file:
            sql_content = file.read()
        
        # 分割SQL语句并执行
        sql_statements = sql_content.split(';')
        for statement in sql_statements:
            statement = statement.strip()
            # 跳过空语句和注释行
            if not statement or statement.startswith('--'):
                continue
                
            print(f"执行SQL语句: {statement}")
            cursor.execute(statement)
            print(f"执行SQL语句成功")
    except FileNotFoundError:
        print(f"找不到SQL文件: {sql_file_path}")
        print(f"当前工作目录: {os.getcwd()}")
        raise

def show_table_data(cursor):
    """显示表中的数据"""
    cursor.execute("SELECT * FROM `games`")
    results = cursor.fetchall()
    print("\n游戏表数据:")
    for row in results:
        print(row)

def main():
    """主函数"""
    try:
        # 使用相对路径获取SQL文件路径
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)
        sql_file_path = os.path.join(project_root, "test", "games.sql")
        
        print(f"SQL文件路径: {sql_file_path}")
        
        # 检查文件是否存在
        if not os.path.exists(sql_file_path):
            print(f"错误: SQL文件不存在: {sql_file_path}")
            return
        
        # 连接Hive并创建目标表
        conn_hive = connect_to_hive()
        hive_cursor = conn_hive.cursor()
        create_database(hive_cursor)
        create_games_table(hive_cursor)
        
        # 从MySQL获取数据并导入到Hive
        mysql_conn = connect_to_mysql()
        mysql_cursor = mysql_conn.cursor()
        games = fetch_games_from_mysql(mysql_cursor)
        print(f"从MySQL获取 {len(games)} 条数据")
        export_games_to_hive(games, hive_cursor)
        
        # 显示Hive中数据
        show_table_data(hive_cursor)
        
        # 关闭连接
        mysql_cursor.close()
        mysql_conn.close()
        hive_cursor.close()
        conn_hive.close()
        
        print("数据从MySQL导入Hive完成！")
        
    except Exception as e:
        print(f"执行过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
