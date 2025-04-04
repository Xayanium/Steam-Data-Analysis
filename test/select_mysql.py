import os
import json
import pymysql
import sys
import locale

# 设置终端locale以支持UTF-8输出
try:
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
except:
    pass

# 检查终端编码
print(f"终端编码: {sys.stdout.encoding}")

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
        use_unicode=True,
        autocommit=True
    )
    return conn

def check_table_charset(cursor):
    """检查games表的字符集设置"""
    cursor.execute("SHOW CREATE TABLE games")
    result = cursor.fetchone()
    print("表结构信息:")
    print(result[1])  # 表创建语句，包含字符集信息
    
    # 检查数据库字符集设置
    cursor.execute("SHOW VARIABLES LIKE 'character_set%'")
    charset_settings = cursor.fetchall()
    print("\n数据库字符集设置:")
    for setting in charset_settings:
        print(f"{setting[0]}: {setting[1]}")

def check_data_for_encoding_issues(cursor):
    """查询games表数据并检查是否有乱码"""
    # 设置连接编码参数
    cursor.execute("SET NAMES utf8mb4")
    cursor.execute("SET CHARACTER SET utf8mb4")
    cursor.execute("SET character_set_connection=utf8mb4")
    cursor.execute("SET collation_connection=utf8mb4_unicode_ci")
    
    # 进行手动编码测试
    cursor.execute("SELECT '二十多年来' AS test_cn, 'hello world' AS test_en")
    test_result = cursor.fetchone()
    print(f"中文编码测试: {test_result[0]}")
    print(f"英文编码测试: {test_result[1]}")
    
    # 尝试不同的编码解析方式
    try:
        print(f"Unicode转义解析中文: {test_result[0].encode('raw_unicode_escape').decode('utf-8')}")
    except:
        print("Unicode转义解析失败")
    
    # 获取总行数
    cursor.execute("SELECT COUNT(*) FROM games")
    total_rows = cursor.fetchone()[0]
    print(f"\n游戏数据总数: {total_rows}条\n")
    
    # 获取并检查部分数据
    cursor.execute("SELECT id, title, description, platform, types, review_summary FROM games LIMIT 10")
    rows = cursor.fetchall()
    
    print("数据样本（检查是否有乱码）:")
    print("=" * 80)
    for row in rows:
        print(f"ID: {row[0]}")
        print(f"标题: {row[1]}")
        
        # 尝试多种解码方式
        desc = row[2]
        print(f"原始描述: {desc[:50]}..." if len(desc) > 50 else f"原始描述: {desc}")
        
        try:
            alt_desc = desc.encode('raw_unicode_escape').decode('utf-8')
            print(f"Unicode转义后描述: {alt_desc[:50]}..." if len(alt_desc) > 50 else f"Unicode转义后描述: {alt_desc}")
        except:
            print("Unicode转义处理失败")
            
        try:
            latin_desc = desc.encode('latin1').decode('utf-8')
            print(f"Latin1->UTF8处理后描述: {latin_desc[:50]}..." if len(latin_desc) > 50 else f"Latin1->UTF8处理后描述: {latin_desc}")
        except:
            print("Latin1->UTF8处理失败")
        
        print(f"平台: {row[3]}")
        print(f"类型: {row[4]}")
        print(f"评价: {row[5]}")
        print("-" * 80)

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
        cursor.execute("SET collation_connection=utf8mb4_unicode_ci")
        
        # 检查表字符集
        check_table_charset(cursor)
        
        # 检查数据编码问题
        check_data_for_encoding_issues(cursor)
        
        # 关闭连接
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"执行过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
