import os
import json
import pymysql
import subprocess

def connect_to_mysql():
    """连接到MySQL数据库，确保使用UTF-8字符集"""
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
        use_unicode=True
    )
    return conn

def recreate_games_table(cursor):
    """删除并重新创建games表，使用UTF-8字符集"""
    # 删除现有表
    cursor.execute("DROP TABLE IF EXISTS games")
    
    # 创建新表，显式指定UTF-8字符集
    create_table_sql = """
    CREATE TABLE games (
        id INT PRIMARY KEY,
        title VARCHAR(255),
        icon TEXT,
        platform TEXT,
        release_date VARCHAR(100),
        review_summary TEXT,
        discount VARCHAR(50),
        original_price VARCHAR(50),
        final_price VARCHAR(50),
        detail_link TEXT,
        types TEXT,
        description TEXT,
        developer VARCHAR(255),
        publisher VARCHAR(255),
        image_link TEXT,
        video_link TEXT,
        review TEXT,
        sys_requirements TEXT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    cursor.execute(create_table_sql)
    print("MySQL games表已重新创建")

def import_backup_data(cursor, backup_file_path):
    """直接使用MySQL命令行工具执行SQL文件"""
    try:
        # 获取MySQL配置
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        mysql_conf = config["mysql"]
        
        # 设置环境变量MYSQL_PWD以传递密码（更安全的方式）
        env = os.environ.copy()
        env['MYSQL_PWD'] = mysql_conf["password"]
        
        # 构建mysql命令
        cmd = [
            'docker exec mysql mysql',
            f'-h{mysql_conf["host"]}',
            f'-P{mysql_conf["port"]}',
            f'-u{mysql_conf["user"]}',
            mysql_conf["database"],
            '--default-character-set=utf8mb4',
            '-e',
            f'source {"/tmp/games.sql"}'
        ]
        
        # 执行命令
        result = subprocess.run(cmd, capture_output=True, text=True, env=env)
        if result.returncode == 0:
            print(f"已从 {backup_file_path} 成功导入数据")
        else:
            print(f"导入数据失败: {result.stderr}")
            raise Exception(result.stderr)
    except Exception as e:
        print(f"导入数据时出错: {e}")
        raise

def main():
    """主函数"""
    try:
        # 连接MySQL
        conn = connect_to_mysql()
        cursor = conn.cursor()
        
        # 设置会话字符集
        cursor.execute("SET NAMES utf8mb4")
        cursor.execute("SET CHARACTER SET utf8mb4")
        cursor.execute("SET character_set_connection=utf8mb4")
        
        # 重建表
        recreate_games_table(cursor)
        
        # 导入备份数据
        backup_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "games.backup.sql")
        import_backup_data(cursor, backup_file_path)
        
        # 提交事务并关闭连接
        conn.commit()
        cursor.close()
        conn.close()
        
        print("MySQL games表重建和数据导入完成")
        
    except Exception as e:
        print(f"执行过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
