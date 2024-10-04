**运行步骤:**

### 1. 下载文件解压

### 2. 利用pip安装第三方包

1. `pip install flask`
2. `pip install pymysql`
3. `pip install mrjob`
4. `pip install happybase`

### 3. 修改config.json中的配置信息为自己的mysql信息

### 4. 在mysql中新建一个数据库, 并执行 `games.sql` 和 `user.sql`

### 5. 修改docker-compose.yml文件，指定数据volume挂载到本地的目录，然后本地的项目根目录下执行 ` docker-compose -f .\docker-compose.yml up -d`

### 6. 执行`docker exec -it steam-data-analysis-hbase-docker-1 bash`进入容器内部，执行` echo $HOSTNAME`，复制输出结果

### 7. 打开Windows的"C:\Windows\System32\drivers\etc\hosts"文件，在文件末尾添加容器主机名和本地IP地址的映射，如：“127.0.0.1 xxxxx”（替换成你复制的主机名）

### 8. 你需要连接hbase并且创建名为“stream_data”的表。（如果你挂载的volume中有数据，则跳过此步）

### 8. 准备好Linux版本的Chrome和driver并且修改代码中的路径。然后运行Chrome启动文件。（确保Chrome和driver版本匹配）

### 9. 如果启动失败，你需要根据报错下载缺少的库，如：`sudo apt-get install libnss3 libx11-6 libasound2 libatk1.0-0 libatk-bridge2.0-0 libcups2 libxcomposite1 libxrandr2 libxdamage1 libxkbcommon0 libgbm1 libgtk-3-0`



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

