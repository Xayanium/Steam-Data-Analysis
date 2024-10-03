# coding: utf-8
# @Author: Xayanium

import re
import time
import pymysql
from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import csv
import os
import json

from utils.query import  hbase_connection
HbaseTableConn = hbase_connection()


# 初始化csv配置文件, 链接数据库
def init():
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='yzy2004518',
        database='steam_data',
        port=3306,
        charset='utf8mb4'
    )

    """
    需要爬取的内容:
    
    搜索页:
    title: 游戏名
    icon: 游戏图标链接
    platform: 支持的游戏平台
    release_date: 发售日期
    review_summary: 评价总结
    discount: 折扣
    original_price: 初始价格
    final_price: 现当前价格
    detail_link: 详情页链接
    
    详情页:
    types: 游戏类型
    description: 游戏简介
    recent_review: 最近评测
    all_review: 全部评测
    developer: 开发商
    publisher: 发行商
    image_links: 预览图片的链接
    video_link: 预览视频的链接
    review: 评价摘选
    sys_requirements: 系统及配置要求
    """

    try:
        sql = """
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
            recent_review varchar(255),
            all_review varchar(255),
            developer varchar(255),
            publisher varchar(255),
            image_link text,
            video_link text,
            review text,
            sys_requirements text
        )
        """

        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()
    except Exception as e:
        print(e)
    finally:
        conn.close()

    if not os.path.exists('./temp1.csv'):
        with open('./temp1.csv', 'at', encoding='utf-8', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([
                'title', 'icon', 'platform', 'release_date', 'review_summary',
                'discount', 'original_price', 'final_price', 'detail_link'
            ])


# selenium启动浏览器
def start_browser():
    option = webdriver.ChromeOptions()
    option.add_experimental_option('debuggerAddress', '127.0.0.1:9222')
    driver = webdriver.Chrome(service=Service('/home/orician/.cache/selenium/chromedriver/linux64/129.0.6668.89/chromedriver'), options=option)
    # driver.implicitly_wait(0.5)  # 设置隐式等待0.5秒
    return driver


def save_to_csv(row_data: list):
    with open('./temp1.csv', 'at', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(
            row_data
        )
        print('stored')


def save_to_mysql():
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='root',
        database='steam_data',
        port=3306,
        charset='utf8mb4'
    )

    cursor = conn.cursor()
    with open('./temp1.csv', 'rt', encoding='utf-8', newline='') as file:
        reader = csv.reader(file)
        # b = HbaseTableConn.batch()
        for row in reader:
            cursor.execute("""
                insert into games (
                    title, icon, platform, release_date, review_summary,
                    discount, original_price, final_price, detail_link   
                ) 
                values(%s, %s, %s, %s, %s, %s, %s, %s, %s)  
            """, [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]])

            hbase_id= HbaseTableConn.counter_inc(b'row_counter',b'games:counter')
            data_map = {
                b'games:id':hbase_id.to_bytes(),
                b'games:title':row[0],
                b'games:icon':row[1],
                b'games:platform':row[2],
                b'games:release_date':row[3],
                b'games:review_summary':row[4],
                b'games:discount':row[5],
                b'games:original_price':row[6],
                b'games:final_price':row[7],
                b'games:detail_link':row[8],
            }
            HbaseTableConn.put(row[0],data_map)

        print('execute sql')
        conn.commit()
        cursor.close()
        conn.close()
        # b.send()


def spider_search_page(target_url):
    driver = start_browser()
    driver.get(target_url)
    game_list = driver.find_elements(by=By.XPATH, value="//a[contains(@class, 'search_result_row')]")
    for game in game_list:
        title = game.find_element(by=By.XPATH, value=".//span[@class='title']").text
        icon = game.find_element(by=By.XPATH, value=".//img").get_attribute('src')
        release_date = game.find_element(by=By.XPATH, value=".//div[contains(@class, 'search_released')]").text
        review_summary = game.find_element(
            by=By.XPATH,
            value=".//span[contains(@class, 'search_review_summary')]"
        ).get_attribute('data-tooltip-html').replace('<br>', ' ')
        detail_link = game.get_attribute('href')

        platform_list = game.find_elements(by=By.XPATH, value="./div[2]//span[contains(@class, 'platform_img')]")
        _platform = []
        for x in platform_list:
            if re.search('win', x.get_attribute('class')):
                _platform.append(re.search('win', x.get_attribute('class')).group())
            elif re.search('mac', x.get_attribute('class')):
                _platform.append(re.search('mac', x.get_attribute('class')).group())
            elif re.search('linux', x.get_attribute('class')):
                _platform.append(re.search('linux', x.get_attribute('class')).group())

        try:
            final_price = re.search(
                "[\d.,]+",
                game.find_element(by=By.XPATH, value=".//div[contains(@class, 'discount_final_price')]").text
            ).group()
        except Exception as e:
            final_price = 0

        try:
            discount = 100 - int(re.search(
                "\d+", game.find_element(by=By.XPATH, value=".//div[@class='discount_pct']").text
            ).group())
            original_price = float(re.search(
                "[\d.,]+",
                game.find_element(
                    by=By.XPATH,
                    value=".//div[contains(@class, 'discount_original_price')]"
                ).text
            ).group())
        except Exception:
            discount = 0
            original_price = final_price

        save_to_csv(
            [
                title,
                icon,
                json.dumps(_platform),
                release_date,
                review_summary,
                discount,
                original_price,
                final_price,
                detail_link
            ]
        )


if __name__ == '__main__':
    # start_browser()
    # init()
    target = f'https://store.steampowered.com/search/?page={1}&ndl=1'
    spider_search_page(target)
    save_to_mysql()
