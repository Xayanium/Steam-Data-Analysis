# coding: utf-8
# @Author: Xayanium
import os.path
import re
import time
import pymysql
from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import json
import csv
from pathlib import Path

from utils.query import query_mysql , hbase_connection

HbaseTableConn = hbase_connection()

class Spider(object):
    def __init__(self, id, spider_url,title):
        self.spider_url = spider_url
        self.id = id
        self.driver = self.start_browser()
        self.root_path = Path(__file__).parent.parent.resolve()
        self.conf_path = os.path.join(self.root_path, 'config.json')
        self.type_list = None
        self.description = None
        self.description = None
        self.developer = None
        self.publisher = None
        self.image_links = None
        self.video_link = None
        self.params = None
        self.title = title

    def start_browser(self):
        with open(self.conf_path, 'r') as conf:
            driver_path = json.load(conf)['driver_path']
        option = webdriver.ChromeOptions()
        option.add_experimental_option('debuggerAddress', '127.0.0.1:9222')
        option.page_load_strategy = 'none'
        driver = webdriver.Chrome(service=Service(driver_path), options=option)
        driver.implicitly_wait(3)  # 设置隐式等待3秒
        driver.get(self.spider_url)
        return driver

    def scroll(self):
        st_pos = 0
        ed_pos = 12534
        step = 2000
        while st_pos < ed_pos:
            scroll_script = f"window.scrollBy(0, {step})"
            self.driver.execute_script(scroll_script)
            st_pos += step
            time.sleep(0.1)

    def save_to_mysql(self):
        # conn = pymysql.connect(
        #     host='localhost',
        #     port=3306,
        #     database='steam_data',
        #     user='root',
        #     password='yzy2004518',
        #     charset='utf8mb4'
        # )
        # cursor = conn.cursor()
        # cursor.execute("""
        #     update games
        #     set types=%s, description=%s, publisher=%s, developer=%s,
        #     image_links=%s, video_link=%s, review=%s, sys_requirements=%s
        #     where id=%s
        # """, self.params)
        # conn.commit()
        query_mysql("""
            update games 
            set types=%s, description=%s, developer=%s, publisher=%s, 
            image_link=%s, video_link=%s, review=%s, sys_requirements=%s
            where id=%s
        """, self.params, 'update')
        data_map = {
            b'games:id':self.id.to_bytes(),
            b'games:types':self.params[0],
            b'games:description':self.params[1],
            b'games:developer':self.params[2],
            b'games:publisher':self.params[3],
            b'games:image_link':self.params[4],
            b'games:video_link':self.params[5],
            b'games:review':self.params[6],
            b'games:sys_requirements':self.params[7],
        }
        HbaseTableConn.put(self.title,data_map)

    # 缓存到csv文件里面
    @staticmethod
    def save_to_csv(row_data: list):
        if os.path.exists('./temp1.csv'):
            os.remove('./temp1.csv')
        with open('./temp1.csv', 'at', encoding='utf-8', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(
                row_data
            )
            print('stored')

    def spider_search_page(self):
        game_list = self.driver.find_elements(by=By.XPATH, value="//a[contains(@class, 'search_result_row')]")
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
            description = ''
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

        box = self.driver.find_element(by=By.XPATH, value="//div[@id='Reviews_summary']")
        review_list = [
            x.text
            for x in box.find_elements(by=By.XPATH, value="(.//div[@class='content'])[position()<=8]")
            if x.text
        ]

        self.params = [
            json.dumps(type_list, ensure_ascii=False), description, developer, publisher,
            image_link, video_link, json.dumps(review_list, ensure_ascii=False),
            json.dumps(sys_requirements, ensure_ascii=False), self.id
        ]


if __name__ == '__main__':
    st_time = time.time()

    elements = query_mysql('select id, detail_link,title from games', [], 'select')
    for element in elements:
        spider = Spider(element[0], element[1],element[2])
        spider.spider_detail_page()
        spider.save_to_mysql()

    # elements = query_mysql('select detail_link from games where id = %s', 26, 'select')
    # for element in elements:
    #     spider = Spider(26, element[0])
    #     spider.spider_detail_page()
    #     spider.save_to_mysql()

    print(time.time()-st_time)






