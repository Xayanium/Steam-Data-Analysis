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

## 项目重构后的逻辑

本项目经过重构后进行了以下改动：
- 使用 Hive 替换 MySQL：数据查询和分析不再直接依赖 MySQL，而是通过 Hive 进行大数据量的灵活查询，充分利用 Hive SQL 的特性。
- 使用 Spark 替换 pyMRjobs：原先基于 pyMRjobs 的 MapReduce 任务改为使用 Spark 实现分布式计算，获得更高的计算性能和更好的扩展性。

## Hive 操作逻辑

本项目通过 Hive 实现大数据量的查询与分析，其优势在于：
- 提供分布式存储，适合处理海量数据。
- 支持 Hive SQL，使查询更加灵活高效。
- 可与 Spark 联合使用，实现数据的高性能处理。

具体步骤：
1. 在 config.json 中配置 Hive 服务信息（如 host、port、database 等）。
2. 利用 Docker Compose 启动 Hive 容器（服务名为 hive4），并使用 Beeline 或 pyhive 连接 Hive。
3. 在 utils/query.py 中，通过 query_hive 方法连接 Hive，执行建表、数据导入与查询操作。
4. 爬虫从 MySQL 导出数据后，Hive 用于备份及数据二次查询，确保数据可靠性和处理速度。

## Spark 操作逻辑

本项目采用 Spark 替换了原先的 pyMRjobs 作为 MapReduce 框架，其优势在于：
- 分布式计算性能更高、扩展性更好。
- 基于 RDD/DataFrame 进行数据转换，语法更简洁清晰。
- 通过与 Hive 联合，实现数据的高效存储和查询。

具体步骤：
1. 利用 Docker Compose 启动 Spark 容器（服务名为 spark3.5.2）。
2. 在 analyzer/deal_data.py 中，创建 SparkSession 读取 CSV 数据，并将数据转化为 RDD：
   - 使用 map、flatMap、reduceByKey 等算子实现各类统计任务（如年份统计、平台统计、价格区间统计等）。
   - 通过 SparkRunner 封装任务运行，实现任务调度和异常处理。
3. 计算结果保存至 JSON 文件（SenrenBanka.json），供前端展示数据分析结果使用。

### Spark 本地模式说明

在未指定 master 地址时，Spark 默认采用本地模式运行：
- Spark 会自动使用类似 "local[*]" 的配置，在本地启动一个 Spark 会话。
- 本地模式下，所有任务都在单机上完成，不需要通过网络连接远程集群，因此无需指定地址和端口。
- 这种方式非常适合开发、测试和调试环境。

因此，在代码中调用 `SparkSession.builder.appName(self.appName).getOrCreate()` 即可自动创建本地 Spark 环境，无需手动指定地址和端口。

### Spark 分析任务

重构后的 [analyzer/deal_data.py](analyzer/deal_data.py) 文件中实现了以下数据分析任务：
- **YearCount**：统计游戏发布时间年份及数量。
- **ReviewCount**：统计不同评论级别游戏的分布。
- **PriceCount**：统计游戏价格区间内的数量分布（区间长度为 25）。
- **PlatformCount**：统计各游戏平台中游戏的数量。
- **TypeCount**：统计游戏标签及其出现次数，同时筛选出数量最多的标签。
- **MaxPrice**：筛选出价格最高的游戏信息。
- **MaxDiscount**：筛选出折扣最大的游戏信息。

以上任务均通过 SparkRunner 类进行封装与执行，实现了分布式计算与结果聚合。
