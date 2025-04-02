#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
import json
from datetime import datetime

def convert_mysql_to_hive(mysql_sql):
    """
    将MySQL SQL转换为Hive SQL
    """
    # 处理INSERT语句
    if mysql_sql.strip().upper().startswith('INSERT INTO'):
        return convert_insert_statement(mysql_sql)
    # 处理CREATE TABLE语句
    elif mysql_sql.strip().upper().startswith('CREATE TABLE'):
        return convert_create_table(mysql_sql)
    # 其他语句原样返回
    else:
        return mysql_sql

def convert_create_table(mysql_sql):
    """
    转换CREATE TABLE语句
    """
    # 替换反引号
    hive_sql = mysql_sql.replace('`', '')
    
    # 替换数据类型
    type_mappings = {
        'int': 'INT',
        'varchar': 'STRING',
        'text': 'STRING',
        'datetime': 'TIMESTAMP',
        'timestamp': 'TIMESTAMP',
        'decimal': 'DECIMAL',
        'float': 'FLOAT',
        'double': 'DOUBLE',
        'tinyint': 'TINYINT',
        'smallint': 'SMALLINT',
        'mediumint': 'INT',
        'bigint': 'BIGINT',
        'char': 'STRING',
        'binary': 'BINARY',
        'varbinary': 'BINARY',
        'blob': 'BINARY',
        'enum': 'STRING',
        'set': 'STRING',
        'json': 'STRING'
    }
    
    # 替换数据类型
    for mysql_type, hive_type in type_mappings.items():
        pattern = re.compile(r'\b' + mysql_type + r'\b', re.IGNORECASE)
        hive_sql = pattern.sub(hive_type, hive_sql)
    
    # 移除COLLATE和ENGINE部分
    hive_sql = re.sub(r'COLLATE\s+\w+(\s*\w*)*', '', hive_sql)
    hive_sql = re.sub(r'ENGINE\s*=\s*\w+.*$', '', hive_sql)
    
    # 添加Hive表属性
    hive_sql = hive_sql.strip()
    if not hive_sql.endswith(';'):
        hive_sql += ';'
    
    return hive_sql

def convert_insert_statement(mysql_sql):
    """
    转换INSERT语句为Hive兼容格式
    """
    # 提取表名
    table_match = re.search(r'INSERT\s+INTO\s+`?(\w+)`?', mysql_sql, re.IGNORECASE)
    if not table_match:
        return mysql_sql
    table_name = table_match.group(1)
    
    # 提取列名
    columns_match = re.search(r'$([^)]+)$', mysql_sql)
    if not columns_match:
        return mysql_sql
    columns = [col.strip().strip('`') for col in columns_match.group(1).split(',')]
    
    # 提取VALUES部分
    values_match = re.search(r'VALUES\s*($.+$)', mysql_sql, re.DOTALL)
    if not values_match:
        return mysql_sql
    values_str = values_match.group(1)
    
    # 处理VALUES中的JSON数组和特殊字符
    try:
        # 使用eval安全地解析VALUES部分
        values = eval(values_str)
    except:
        # 如果eval失败，尝试手动处理
        values = parse_values_manually(values_str)
    
    # 构建Hive INSERT语句
    hive_insert = f"INSERT INTO TABLE {table_name} VALUES ("
    
    # 处理每个值
    converted_values = []
    for i, val in enumerate(values):
        col_name = columns[i] if i < len(columns) else f'col{i+1}'
        
        if val is None:
            converted_val = 'NULL'
        elif isinstance(val, (int, float)):
            converted_val = str(val)
        elif isinstance(val, str):
            # 处理JSON字符串
            if (val.startswith('[') and val.endswith(']')) or (val.startswith('{') and val.endswith('}')):
                try:
                    json.loads(val)  # 验证是否是有效的JSON
                    converted_val = f"'{val.replace("'", "\\'")}'"
                except:
                    converted_val = f"'{val.replace("'", "\\'")}'"
            else:
                converted_val = f"'{val.replace("'", "\\'")}'"
        elif isinstance(val, (list, dict)):
            converted_val = f"'{json.dumps(val, ensure_ascii=False).replace("'", "\\'")}'"
        else:
            converted_val = f"'{str(val).replace("'", "\\'")}'"
        
        converted_values.append(converted_val)
    
    hive_insert += ', '.join(converted_values) + ');'
    
    return hive_insert

def parse_values_manually(values_str):
    """
    手动解析VALUES字符串
    """
    # 移除最外层的括号
    if values_str.startswith('(') and values_str.endswith(')'):
        values_str = values_str[1:-1]
    
    # 分割值
    values = []
    current = ''
    in_quotes = False
    in_json = 0  # 用于跟踪JSON嵌套层级
    
    for char in values_str:
        if char == "'" and not in_json:
            in_quotes = not in_quotes
            current += char
        elif char == '[' and not in_quotes:
            in_json += 1
            current += char
        elif char == ']' and not in_quotes:
            in_json -= 1
            current += char
        elif char == ',' and not in_quotes and in_json == 0:
            values.append(current.strip())
            current = ''
        else:
            current += char
    
    if current:
        values.append(current.strip())
    
    # 处理每个值
    processed_values = []
    for val in values:
        if val == 'NULL':
            processed_values.append(None)
        elif val.startswith("'") and val.endswith("'"):
            processed_val = val[1:-1]
            # 尝试解析JSON
            if (processed_val.startswith('[') and processed_val.endswith(']')) or (processed_val.startswith('{') and processed_val.endswith('}')):
                try:
                    processed_val = json.loads(processed_val)
                except:
                    pass
            processed_values.append(processed_val)
        elif val.isdigit():
            processed_values.append(int(val))
        elif val.replace('.', '', 1).isdigit():
            processed_values.append(float(val))
        else:
            processed_values.append(val)
    
    return processed_values

def process_sql_file(input_file, output_file):
    """
    处理整个SQL文件
    """
    with open(input_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # 分割SQL语句
    statements = re.split(r';\s*\n', sql_content)
    
    # 转换每个语句
    hive_statements = []
    for stmt in statements:
        stmt = stmt.strip()
        if stmt:
            hive_stmt = convert_mysql_to_hive(stmt)
            hive_statements.append(hive_stmt)
    
    # 写入输出文件
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(hive_statements))
    
    print(f"转换完成! 结果已保存到 {output_file}")

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("用法: python mysql_to_hive.py <输入文件> <输出文件>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    process_sql_file(input_file, output_file)