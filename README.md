**运行步骤:**

### 1. 下载文件解压

### 2. 利用pip安装第三方包

1. `pip install flask`
2. `pip install pymysql`
3. `pip install mrjob`

### 3. 修改config.json中的配置信息为自己的mysql信息

### 4. 在mysql中新建一个数据库, 并执行 `games.sql` 和 `user.sql`

### 5. 运行 `app.py` 文件



**项目结构:**

`spider` : 爬虫, 用来对 `steam` 页面爬取信息, 首先需要下载本地 `chrome` 浏览器对应版本的 `chromedriver` 插件; 

* 运行 `search_page.py` 可以爬取 `steam` 搜索页面的数据, 会生成一个临时 csv 文件, 每次运行 `search_page.py` 要删除这个csv文件; 
* 运行 `detail_page.py` 可以爬取详情页面的数据, 数据会直接存入 `mysql` 中



`utils` : 

* `query.py` : 使用 `pymysql` 操纵 `mysql` 进行增改查
* `games.csv` : 从 `mysql` 导出的 csv 格式的游戏详细数据
* `deal_data.py` : 使用 `mrjob` 操纵 `MapReduce` 进行计算, 生成 `SenrenBanka.json` 文件作为计算结果, 该文件需要单独运行, 使用 `python deal_data.py games.csv`
* `get_data.py` : 用于获取前端页面要展示的数据, flask 的 `app.py` 文件会使用该文件中的函数



`templates/static` : 前端静态文件

