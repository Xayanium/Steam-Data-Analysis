# coding: utf-8
# @Author: Xayanium
# @Date: 2024/10/9


from utils.query import query_mysql

if __name__ == '__main__':
    query_mysql("""
    create table games(
            id int primary key auto_increment,
            title varchar(255),
            icon varchar(1024),
            platform varchar(255),
            release_date varchar(255),
            review_summary varchar(255),
            discount varchar(255),
            original_price varchar(255),
            final_price varchar(255),
            detail_link varchar(1024),
            
            types varchar(1024),
            description text,
            developer varchar(255),
            publisher varchar(255),
            image_link text,
            video_link text,
            review text,
            sys_requirements text
        );
    """, [], 'create')  # 创建games表
    query_mysql("""
    create table user(
        id int primary key auto_increment,
        username varchar(255),
        password varchar(255)
    )
    """, [], 'create')  # 创建user表
    print('successfully create tables games and user')





