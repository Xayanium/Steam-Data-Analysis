# -*- coding: utf-8 -*-
# @Time    : 2024/10/12 15:15
# @Author  : Xayanium

import math
import os.path
import re
import sys
import time
import json
import csv
from pathlib import Path

from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)
from utils.query import QueryData

query = QueryData()

class Spider(object):
    def __init__(self, id, spider_url, title):
        self.spider_url = spider_url
        self.id = id
        self.root_path = Path(__file__).parent.parent.resolve()
        self.conf_path = os.path.join(self.root_path, 'config.json')
        self.driver = self.start_browser()
        self.type_list = None
        self.description = None
        self.description = None
        self.developer = None
        self.publisher = None
        self.image_links = None
        self.video_link = None
        self.spider_result_list = None
        self.title = title

    def start_browser(self):
        option = webdriver.ChromeOptions()
        option.add_experimental_option('debuggerAddress', '127.0.0.1:9222')
        option.page_load_strategy = 'none'
        option.add_argument("--headless")  # 无头模式
        option.add_argument("--disable-gpu")
        option.add_argument("--no-sandbox")

        service = Service(ChromeDriverManager().install())  # 使用 webdriver-manager 自动下载和管理 Chromedriver
        driver = webdriver.Chrome(service=service, options=option)
        driver.implicitly_wait(1.5)  # 设置隐式等待3秒
        driver.get(self.spider_url)
        return driver

    # 存入数据库
    def save_to_databases(self, method, platform):
        if method == 'spider_search_page':
            with open('./temp1.csv', 'rt', encoding='utf-8', newline='') as file:
                reader = csv.reader(file)
                for row in reader:
                    query.query_mysql("""
                        insert into games (
                            title, icon, platform, release_date, review_summary,
                            discount, original_price, final_price, detail_link
                        )
                        values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]], 'insert')
                    if platform == 'linux':
                        data_map = {
                            b'games:title': row[0],
                            b'games:icon': row[1],
                            b'games:platform': row[2],
                            b'games:release_date': row[3],
                            b'games:review_summary': row[4],
                            b'games:discount': row[5],
                            b'games:original_price': row[6],
                            b'games:final_price': row[7],
                            b'games:detail_link': row[8],
                        }
                        # query.query_hbase(
                        #     table_name='steam_data',
                        #     args=data_map,
                        #     method='insert',
                        #     row_key=row[0]  # title作为row_key
                        # )
                        try:
                            with query.hbase_connection().connection() as conn:
                                table = conn.table('steam_data')
                                table.put(row[0], data_map)
                        except Exception as e:
                            print(e)

        elif method == 'spider_detail_page':
            query.query_mysql("""
                update games 
                set types=%s, description=%s, developer=%s, publisher=%s, image_link=%s, video_link=%s, review=%s, sys_requirements=%s
                where id=%s
            """, self.spider_result_list, 'update')
            if platform == 'linux':
                data_map = {
                    b'games:id': self.id.to_bytes(length=math.ceil(self.id.bit_length()/8), byteorder='big'),
                    b'games:types': self.spider_result_list[0],
                    b'games:description': self.spider_result_list[1],
                    b'games:developer': self.spider_result_list[2],
                    b'games:publisher': self.spider_result_list[3],
                    b'games:image_link': self.spider_result_list[4],
                    b'games:video_link': self.spider_result_list[5],
                    b'games:review': self.spider_result_list[6],
                    b'games:sys_requirements': self.spider_result_list[7],
                }
                # query.query_hbase(
                #     table_name='steam_data',
                #     args=data_map,
                #     method='insert',
                #     row_key=self.title  # title作为row_key
                # )
                try:
                    with query.hbase_connection().connection() as conn:
                        table = conn.table('steam_data')
                        table.put(self.title, data_map)
                except Exception as e:
                    print(e)

    # 缓存到csv文件里面
    @staticmethod
    def save_to_csv(row_data: list):
        with open('./temp1.csv', 'at', encoding='utf-8', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(
                row_data
            )
            print('csv stored')

    # 爬取搜索页数据
    def spider_search_page(self):
        game_list = self.driver.find_elements(by=By.XPATH, value="//a[contains(@class, 'search_result_row')]")
        for game in game_list:
            title = game.find_element(by=By.XPATH, value=".//span[@class='title']").text
            icon = game.find_element(by=By.XPATH, value=".//img").get_attribute('src')
            release_date = game.find_element(by=By.XPATH, value=".//div[contains(@class, 'search_released')]").text
            try:
                review_summary = game.find_element(
                    by=By.XPATH,
                    value=".//span[contains(@class, 'search_review_summary')]"
                ).get_attribute('data-tooltip-html').replace('<br>', ' ')
            except Exception:
                review_summary = ''
            detail_link = game.get_attribute('href')
            platform_list = game.find_elements(
                by=By.XPATH, value="./div[2]//span[contains(@class, 'platform_img')]"
            )
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
                    r"[\d.,]+",
                    game.find_element(by=By.XPATH, value=".//div[contains(@class, 'discount_final_price')]").text
                ).group()
            except Exception:
                final_price = 0

            try:
                discount = 100 - int(re.search(
                    r"\d+", game.find_element(by=By.XPATH, value=".//div[@class='discount_pct']").text
                ).group())
                original_price = float(re.search(
                    r"[\d.,]+",
                    game.find_element(
                        by=By.XPATH,
                        value=".//div[contains(@class, 'discount_original_price')]"
                    ).text
                ).group())
            except Exception:
                discount = 0
                original_price = final_price

            self.save_to_csv(
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

    # 模拟滚轮刷新页面
    def scroll(self):
        st_pos = 0
        ed_pos = 12534
        step = 2000
        while st_pos < ed_pos:
            scroll_script = f"window.scrollBy(0, {step})"
            self.driver.execute_script(scroll_script)
            st_pos += step
            time.sleep(0.1)

    # 爬取详情页数据
    def spider_detail_page(self):
        # self.start_browser()
        self.scroll()
        box = self.driver.find_element(By.XPATH, value="//div[@class='glance_ctn']")
        type_list = [
            x.text for x in box.find_elements(by=By.XPATH, value=".//*[@class='glance_tags popular_tags']/a") if x.text
        ]
        try:
            description = box.find_element(by=By.XPATH, value=".//div[@class='game_description_snippet']").text
        except NoSuchElementException:
            description = 'No Review'
        developer = box.find_elements(by=By.XPATH, value=".//div[@class='summary column']/a")[0].text
        publisher = box.find_elements(by=By.XPATH, value=".//div[@class='summary column']/a")[1].text

        try:
            image_link = box.find_element(by=By.XPATH, value=".//img").get_attribute('src')
        except NoSuchElementException:
            image_link = ''

        box = self.driver.find_element(by=By.XPATH, value="//div[@class='highlight_ctn']")
        try:
            video_link = box.find_element(by=By.XPATH, value=".//video").get_attribute('src')
        except NoSuchElementException:
            video_link = ''

        box = self.driver.find_element(by=By.XPATH, value="//div[@class='sysreq_contents']")
        lowest_req = [
            x.text
            for x in box.find_elements(by=By.XPATH, value=".//div[@class='game_area_sys_req_leftCol']//li")
            if x.text
        ]
        if not len(lowest_req):
            lowest_req = [
                x.text
                for x in box.find_elements(by=By.XPATH, value=".//div[@class='game_area_sys_req_full']//li")
                if x.text
            ]
        suggest_req = [
            x.text
            for x in box.find_elements(by=By.XPATH, value=".//div[@class='game_area_sys_req_rightCol']//li")
            if x.text
        ]
        sys_requirements = [lowest_req, suggest_req]

        try:
            box = self.driver.find_element(by=By.XPATH, value="//div[@id='Reviews_summary']")
            review_list = [
                x.text
                for x in box.find_elements(by=By.XPATH, value="(.//div[@class='content'])[position()<=8]")
                if x.text
            ]
        except Exception:
            review_list = []

        self.spider_result_list = [
            json.dumps(type_list, ensure_ascii=False), description, developer, publisher,
            image_link, video_link, json.dumps(review_list, ensure_ascii=False),
            json.dumps(sys_requirements, ensure_ascii=False), self.id
        ]


def process_spider(args: dict, platform):
    try:
        spider = Spider(args['id'], args['url'], args['title'])
        if args['method'] == 'spider_search_page':
            spider.spider_search_page()
        elif args['method'] == 'spider_detail_page':
            spider.spider_detail_page()
        spider.save_to_databases(args['method'], platform)
        print('finish one spider task')
    except Exception as e:
        print('spider pages error: ', e)
    # spider = Spider(args['id'], args['url'], args['title'])
    # if args['method'] == 'spider_search_page':
    #     spider.spider_search_page()
    # elif args['method'] == 'spider_detail_page':
    #     spider.spider_detail_page()
    # spider.save_to_databases(args['method'], platform)
    # print('finish one spider task')


if __name__ == '__main__':
    current_platform = sys.platform  # 检测当前运行的平台
    target = f'https://store.steampowered.com/search/?page={4}&ndl=1'  # steam搜索页第一页
    st_time = time.time()
    if os.path.exists('./temp1.csv'):  # 清理之前的csv缓存文件
        os.remove('./temp1.csv')

    # 爬取搜索页数据:
    # 单页:
    exec_list = [
        {'id': None, 'url': target, 'title': None, 'method': 'spider_search_page'}
    ]
    # 多页
    # exec_list = [
    #     {'id': None, 'title': None, 'method': 'spider_search_page',
    #      'url': f'https://store.steampowered.com/search/?page={i}&ndl=1'}
    #     for i in range(1, 3)
    # ]
    # for item in exec_list:
    #     process_spider(item, current_platform)


    # 爬取详情页数据:
    # 单条记录
    # elements = query.query_mysql('select id, detail_link, title from games where id = %s', 3, 'select')
    # 多条记录
    elements = query.query_mysql(
        'select id, detail_link, title from games where id between %s and %s',
        [66, 66], 'select'
    )

    _exec_list = [
        {'id': element[0], 'url': element[1], 'title': element[2], 'method': 'spider_detail_page'}
        for element in elements
    ]
    for _item in _exec_list:
        process_spider(_item, current_platform)

    print(time.time()-st_time)


