# coding: utf-8
# @Author: Xayanium

"""
需要处理的数据:

"""
import json
import time

from mrjob.job import MRJob
import csv
import ast


class YearCount(MRJob):
    # mapper接受文件中的一行数据作为value, key默认为None
    def mapper(self, _, line):
        # csv从迭代器中读取数据, 按逗号分割进行解析, 结果放入一个列表中返回
        row = next(csv.reader([line]))  # 将传入的数据字符串包裹为一个迭代器(列表)
        try:
            year = int(row[4].split(' ')[0])  # 截取出年份并转为数字
            yield year, 1  # mapper出<k, v>键值对
        except (IndexError, ValueError):
            pass

    # reducer中的接受的key是由mapper/combiner生成的key
    # reducer接受value是一个生成器, 可以生成mapper产生<k, v>中key相同的所有v
    def reducer(self, year, counts):
        yield year, sum(counts)


class ReviewCount(MRJob):
    def mapper(self, _, line):
        row = next(csv.reader([line]))
        try:
            review = row[5].split(' ')[0]
            yield review, 1
        except (IndexError, ValueError):
            pass

    def reducer(self, review, counts):
        yield review, sum(counts)


class PriceCount(MRJob):
    def mapper(self, _, line):
        row = next(csv.reader([line]))
        try:
            price = int(float(row[8])//25)*25
            yield price, 1
        except (IndexError, ValueError):
            pass

    def reducer(self, price, counts):
        yield price, sum(counts)


class PlatformCount(MRJob):
    def mapper(self, _, line):
        row = next(csv.reader([line]))
        try:
            platforms = row[3]
            for platform in ast.literal_eval(platforms):
                yield platform, 1
        except (IndexError, ValueError, SyntaxError):
            pass

    def reducer(self, price, counts):
        yield price, sum(counts)


if __name__ == '__main__':
    st = time.time()
    # 将输出写入文件
    try:
        # YearCount().make_runner() 创建一个用于执行 YearCount MRJob 作业的 runner 对象
        with YearCount().make_runner() as runner:
            runner.run()
            # runner.cat_output() 方法获取作业的所有输出并将其作为字符串返回
            # YearCount().parse_output() 方法将这些输出解析为键值对
            results = YearCount().parse_output(runner.cat_output())  # 得到一个生成器对象
            price_counts = {key: value for key, value in results}
            with open('counts/year_counts.txt', 'w', encoding='utf-8') as f_year:
                f_year.write(json.dumps(price_counts, ensure_ascii=False, indent=4))
    except FileNotFoundError:
        pass

    try:
        with ReviewCount().make_runner() as runner:
            runner.run()
            results = ReviewCount().parse_output(runner.cat_output())
            review_counts = {key: value for key, value in results}
            with open('counts/review_counts', 'w', encoding='utf-8') as f_review:
                f_review.write(json.dumps(review_counts, ensure_ascii=False, indent=4))
    except FileNotFoundError:
        pass

    try:
        with PriceCount().make_runner() as runner:
            runner.run()
            results = PriceCount().parse_output(runner.cat_output())
            price_counts = {key: value for key, value in results}
            with open('counts/price_count.txt', 'w', encoding='utf-8') as f_price:
                f_price.write(json.dumps(price_counts, ensure_ascii=False, indent=4))
    except FileNotFoundError:
        pass

    try:
        with PlatformCount().make_runner() as runner:
            runner.run()
            results = PlatformCount().parse_output(runner.cat_output())
            platform_counts = {key: value for key, value in results}
            with open('counts/platform_count.txt', 'w', encoding='utf-8') as f_platform:
                f_platform.write(json.dumps(platform_counts, ensure_ascii=False, indent=4))
    except FileNotFoundError:
        pass

    print(time.time()-st)


