# coding: utf-8
# @Author: Xayanium

import re
import time
import pymysql
from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import json


class Spider(object):
    def __init__(self, id, spider_url):
        self.spider_url = spider_url
        self.id = id
        self.driver = self.start_browser()
        self.type_list = None
        self.description = None
        self.description = None
        self.developer = None
        self.publisher = None
        self.image_links = None
        self.video_link = None
        self.params = None

    def start_browser(self):
        option = webdriver.ChromeOptions()
        option.add_experimental_option('debuggerAddress', '127.0.0.1:9222')
        # 23
        driver = webdriver.Chrome(service=Service('chromedriver.exe'), options=option)
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
            time.sleep(0.3)

    def save_to_mysql(self):
        conn = pymysql.connect(
            host='localhost',
            port=3306,
            database='steam_data',
            user='root',
            password='yzy2004518',
            charset='utf8mb4'
        )
        cursor = conn.cursor()
        cursor.execute("""
            update games 
            set types=%s, description=%s, publisher=%s, developer=%s, 
            image_links=%s, video_link=%s, review=%s, sys_requirements=%s
            where id=%s
        """, self.params)
        conn.commit()

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

        box = self.driver.find_element(by=By.XPATH, value="//div[@class='highlight_ctn']")
        image_links = [
            x.get_attribute('src')
            for x in box.find_elements(by=By.XPATH, value=".//div[contains(@class, 'highlight_strip_item')]/img")
        ]
        try:
            video_link = box.find_element(by=By.XPATH, value=".//video").get_attribute('src')
        except NoSuchElementException:
            video_link = ''

        sys_requirements = self.driver.find_element(by=By.XPATH, value="//div[@class='sysreq_contents']").text

        box = self.driver.find_element(by=By.XPATH, value="//div[@id='Reviews_summary']")
        review_list = [
            x.text
            for x in box.find_elements(by=By.XPATH, value=".//div[@class='content']")
            if x.text
        ]

        self.params = [
            json.dumps(type_list, ensure_ascii=False), description, developer, publisher,
            json.dumps(image_links), video_link, json.dumps(review_list, ensure_ascii=False),
            sys_requirements, self.id
        ]


if __name__ == '__main__':
    st_time = time.time()
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='yzy2004518',
        database='steam_data',
        port=3306,
        charset='utf8mb4'
    )
    cursor = conn.cursor()
    cursor.execute('select id, detail_link from games')
    # elements = cursor.fetchall()
    # conn.commit()
    # for element in elements:
    #     spider = Spider(element[0], element[1])
    #     spider.spider_detail_page()
    #     spider.save_to_mysql()
    cursor.execute('select detail_link from games where id = %s', 12)
    element = cursor.fetchone()
    spider = Spider(12, element[0])
    spider.spider_detail_page()
    spider.save_to_mysql()
    print(time.time()-st_time)
    # spider = Spider(elements[0][0], elements[0][1])






