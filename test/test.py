from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time

# 配置 Chrome 选项
chrome_options = Options()
chrome_options.add_argument("--headless")  # 无头模式
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")

# 使用 webdriver-manager 自动下载和管理 Chromedriver
service = Service(ChromeDriverManager().install())

# 初始化 WebDriver
driver = webdriver.Chrome(service=service, options=chrome_options)

try:
    # 打开一个网页
    driver.get("https://www.baidu.com")

    # 等待页面加载
    # time.sleep(1)

    # 查找搜索框并输入内容
    search_box = driver.find_element(By.ID, "kw")
    search_box.send_keys("Selenium Python")
    search_box.submit()

    # 等待搜索结果加载
    # time.sleep(1)

    # 获取搜索结果标题
    print(driver.title)

finally:
    # 关闭浏览器
    driver.quit()

