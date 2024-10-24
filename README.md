**运行步骤:**

### 1. 项目 git clone 到本地 / 下载压缩文件解压

### 2. 利用pip安装第三方包 (如果为项目创建了虚拟环境, 要在虚拟环境下执行)

1. `pip install flask`
2. `pip install pymysql`
3. `pip install mrjob`
4. `pip install selenium`
5. `pip install happybase`
6. `pip install webdriver-manager`

注: 由于happybase库需要gcc构建工具, 此库在windows上安装时可能会出错, 建议使用linux系统,
以Ubuntu22.04为例, 首先保证环境中有gcc工具, 然后安装Python开发包 `sudo apt-get install python3-dev`

### 3. 修改config.json中的配置信息为自己的mysql信息

### 4. 在mysql中新建名为 `steam_data` 数据库, 字符编码为 `utf8mb4`

### 5. 修改docker-compose.yml文件，指定数据volume挂载到本地的目录，然后在项目根目录下执行 `sudo docker-compose up -d`

 **注**: 要先安装 `docker` 和 `docker-compose` , 并且由于 `dockerhub` 被禁止访问, 不能直接从 `dockerhub` 中直接拉取镜像
 需要先下载 `docker desktop`, 再通过魔法从docker desktop中下载 `dajobe/hbase:latest` 镜像, 然后在终端中执行命令 
 `docker save <image_name> -o <file_path>`, 生成一个 .tar 文件, (如果docker desktop在windows安装, 要将tar文件传入linux系统中), 
 然后再在 linux 中执行 `docker load -i <file_path>`

### 6. 执行 `sudo docker exec -it steam-data-analysis_hbase-docker_1 bash` 进入容器内部，执行 `echo $HOSTNAME` ，获取容器主机名, `ctrl+p ctrl+q` 退出容器命令行

### 7. 在宿主机中添加容器主机名和本地 IP 地址的映射, linux系统打开 `/etc/hosts` 文件, Windows系统打开 `"C:\Windows\System32\drivers\etc\hosts"` 文件，在文件末尾添加容器主机名和本地IP地址的映射，如：“127.0.0.1 xxxxx”（替换成你复制的主机名)

### 8. 运行 `init.py` 文件, 自动在数据库中初始化所需要的表

#### 对于爬虫文件, 在linux上执行可能会遇到 steam访问不了/无图形化界面linux打不开chrome/运行不了爬虫文件, 在保留项目结构 (即目录Steam-Data_Analysis)的情况下将 `spiders` 文件夹
#### 以及 `config.json` 复制到 windows 系统中, `config.json` 中的 `mysql` 的配置应为 linux 系统上的 `mysql` 服务配置

如果爬虫一直失败, 可以使用位于 `test` 目录下的sql文件向 `mysql` 中导入数据, 并执行 `spiders` 目录中的 `save_to_hbase.py` 将mysql中数据导入 `hbase` 中

### 9. 在linux上安装chrome浏览器 (windows自行安装chrome并跳过该步)
注: 此项需要保证使用的linux系统有图形化界面, 如果没有图形化界面可能导致出错, 
1. `wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb`
2. `sudo dpkg -i google-chrome-stable_current_amd64.deb`
3. `google-chrome --version` 检查 chrome 版本
4. 在项目根目录下测试爬虫环境是否配置好 `python3 test.py`, 出现 '百度一下, 你就知道'代表成功
5. 自行搜索 `steam++` 在ubuntu上的安装教程, 执行配置 `steam++` 所需的安全证书

### 10. 根据自己执行爬虫文件的操作系统, 在 `chrome_run.txt` 中选择对应的 `chrome` 浏览器启动命令, 执行后自动打开浏览器(在爬取结束前请勿关掉)

### 11. 进入 `spiders` 目录, 运行 `python3 spider_pages.py` (linux) / `python spider_pages.py` (windows)

### 12. 在windows上运行爬虫程序后还需要执行该步, 将mysql中数据复制到HBASE中, 回到linux系统, 在 `spiders` 目录下执行 `python3 save_to_hbase.py`

### 13. 进入 `analyzer` 目录, 执行 `python3 run_analyzer.py `, 自动从数据库中读取数据并生成csv文件, 利用Mapreduce计算出结果并自动保存至 `SenrenBanka.json` 文件

### 14. 执行 `python3 app.py` (linux) / `python app.py` (windows), 将产生的url复制到浏览器可打开网页前端

### 15. 如果运行中出现报错, 请自行搜索解决方法



**项目结构:**

`spider` : 爬虫, 用来对 `steam` 页面爬取信息



`utils` : 一些复用的函数

* `query.py` : 使用 `pymysql` 操纵 `mysql` 进行增改查
* `get_data.py` : 用于获取前端页面要展示的数据, flask 的 `app.py` 文件会使用该文件中的函数

`analyser` :  利用mapreduce计算分析结果
* `games.csv` : 从 `mysql` 导出的 csv 格式的游戏详细数据
* `deal_data.py` : 使用 `mrjob` 操纵 `MapReduce` 进行计算, 生成 `SenrenBanka.json` 文件作为计算结果, 该文件需要单独运行, 使用 `python deal_data.py games.csv`
* `run_analyzer.py` : 自动化运行脚本

`templates/static` : 前端静态文件

