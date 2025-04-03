# coding: utf-8
# @Author: Xayanium

"""
需要处理的数据:

"""
import json
import time
import sys
import csv
import ast
from pyspark.sql import SparkSession


# 新增通用 Runner 类
class SparkRunner:
    def __init__(self, job, appName):
        self.job = job
        self.appName = appName
        self.output = None

    def run(self):
        input_file = sys.argv[1] if len(sys.argv) > 1 else "games.csv"
        spark = SparkSession.builder.appName(self.appName).getOrCreate()
        rdd = spark.read.text(input_file).rdd.map(lambda r: r.value)
        self.output = self.job.spark_job(rdd)
        spark.stop()

    def cat_output(self):
        return self.output

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


# 重构后的 YearCount
class YearCount:
    def spark_job(self, rdd):
        def mapper(line):
            try:
                row = next(csv.reader([line]))
                year = int(row[4].split(' ')[0])
                return (year, 1)
            except (IndexError, ValueError, SyntaxError):
                return None
        return (rdd.map(mapper)
                   .filter(lambda x: x is not None)
                   .reduceByKey(lambda a, b: a + b)
                   .collect())
    
    def make_runner(self):
        return SparkRunner(self, "YearCount")

    @staticmethod
    def parse_output(output_lines):
        for line in output_lines:
            if isinstance(line, tuple):
                key, value = line
            else:
                key, value = line.strip().split("\t")
            yield (int(key) if isinstance(key, str) and key.isdigit() else key, int(value))


# 重构后的 ReviewCount
class ReviewCount:
    def spark_job(self, rdd):
        def mapper(line):
            try:
                row = next(csv.reader([line]))
                review = row[5].split(' ')[0]
                if review == 'review_summary':
                    return None
                return (review, 1)
            except (IndexError, ValueError, SyntaxError):
                return None
        return (rdd.map(mapper)
                   .filter(lambda x: x is not None)
                   .reduceByKey(lambda a, b: a + b)
                   .collect())
    
    def make_runner(self):
        return SparkRunner(self, "ReviewCount")
    
    @staticmethod
    def parse_output(output_lines):
        for line in output_lines:
            if isinstance(line, tuple):
                key, value = line
            else:
                key, value = line.strip().split("\t")
            yield (key, int(value))


# 重构后的 PriceCount
class PriceCount:
    def spark_job(self, rdd):
        def mapper(line):
            try:
                row = next(csv.reader([line]))
                price = int(float(row[8]) // 25) * 25
                return (f'({price}, {price+24})', 1)
            except (IndexError, ValueError, SyntaxError):
                return None
        return (rdd.map(mapper)
                   .filter(lambda x: x is not None)
                   .reduceByKey(lambda a, b: a + b)
                   .collect())
    
    def make_runner(self):
        return SparkRunner(self, "PriceCount")
    
    @staticmethod
    def parse_output(output_lines):
        for line in output_lines:
            if isinstance(line, tuple):
                key, value = line
            else:
                key, value = line.strip().split("\t")
            yield (key, int(value))


# 重构后的 PlatformCount
class PlatformCount:
    def spark_job(self, rdd):
        def mapper(line):
            try:
                row = next(csv.reader([line]))
                platforms = ast.literal_eval(row[3])
                return [(platform, 1) for platform in platforms]
            except (IndexError, ValueError, SyntaxError):
                return []
        return (rdd.flatMap(mapper)
                   .reduceByKey(lambda a, b: a + b)
                   .collect())
    
    def make_runner(self):
        return SparkRunner(self, "PlatformCount")
    
    @staticmethod
    def parse_output(output_lines):
        for line in output_lines:
            if isinstance(line, tuple):
                key, value = line
            else:
                key, value = line.strip().split("\t")
            yield (key, int(value))


# 重构后的 TypeCount
class TypeCount:
    def spark_job(self, rdd):
        def mapper(line):
            try:
                row = next(csv.reader([line]))
                tags = ast.literal_eval(row[10])
                return [(tag, 1) for tag in tags]
            except (IndexError, ValueError, SyntaxError):
                return []
        return (rdd.flatMap(mapper)
                   .reduceByKey(lambda a, b: a + b)
                   .collect())
    
    def make_runner(self):
        return SparkRunner(self, "TypeCount")
    
    @staticmethod
    def parse_output(output_lines):
        for line in output_lines:
            if isinstance(line, tuple):
                key, value = line
            else:
                key, value = line.strip().split("\t")
            yield (key, int(value))


# 重构后的 MaxDiscount (两阶段操作保持不变)
class MaxDiscount:
    def spark_job(self, rdd):
        def mapper(line):
            try:
                row = next(csv.reader([line]))
                discount = int(row[6])
                name = row[1]
                return (name, discount)
            except (IndexError, ValueError, SyntaxError):
                return None
        stage1 = (rdd.map(mapper)
                     .filter(lambda x: x is not None)
                     .reduceByKey(lambda a, b: a if a > b else b))
        # 第二阶段：求全局最大
        pairs = stage1.collect()
        if not pairs:
            return []
        max_discount = max(discount for _, discount in pairs)
        result = [(name, discount) for name, discount in pairs if discount == max_discount]
        return [(None, f"{name}\t{discount}") for name, discount in result]  # 输出格式为单行，parse时注意拆分
    
    def make_runner(self):
        return SparkRunner(self, "MaxDiscount")
    
    @staticmethod
    def parse_output(output_lines):
        for line in output_lines:
            if isinstance(line, tuple):
                _, out = line
            else:
                _, out = line.strip().split("\t", 1)
            name, discount = out.split("\t")
            yield (name, int(discount))


# 重构后的 MaxPrice (两阶段操作保持不变)
class MaxPrice:
    def spark_job(self, rdd):
        def mapper(line):
            try:
                row = next(csv.reader([line]))
                name = row[1]
                price = float(row[8])
                return (name, price)
            except (IndexError, ValueError, SyntaxError):
                return None
        stage1 = (rdd.map(mapper)
                     .filter(lambda x: x is not None)
                     .reduceByKey(lambda a, b: a if a > b else b))
        pairs = stage1.collect()
        if not pairs:
            return []
        max_price = max(price for _, price in pairs)
        result = [(name, price) for name, price in pairs if price == max_price]
        return [(None, f"{name}\t{price}") for name, price in result]
    
    def make_runner(self):
        return SparkRunner(self, "MaxPrice")
    
    @staticmethod
    def parse_output(output_lines):
        for line in output_lines:
            if isinstance(line, tuple):
                _, out = line
            else:
                _, out = line.strip().split("\t", 1)
            name, price = out.split("\t")
            yield (name, float(price))


if __name__ == '__main__':
    st = time.time()
    result_json = {}
    # YearCount
    try:
        with YearCount().make_runner() as runner:
            runner.run()
            results = YearCount.parse_output(runner.cat_output())
            years = {key: value for key, value in results}
            result_json['years'] = years
    except FileNotFoundError:
        pass
    # ReviewCount
    try:
        with ReviewCount().make_runner() as runner:
            runner.run()
            results = ReviewCount.parse_output(runner.cat_output())
            reviews = {key: value for key, value in results}
            result_json['reviews'] = reviews
    except FileNotFoundError:
        pass
    # PriceCount
    try:
        with PriceCount().make_runner() as runner:
            runner.run()
            results = PriceCount.parse_output(runner.cat_output())
            prices_list = sorted({key: value for key, value in results}.items(), key=lambda x: (len(x[0]), x[0]))
            result_json['prices'] = dict(prices_list)
    except FileNotFoundError:
        pass
    # PlatformCount
    try:
        with PlatformCount().make_runner() as runner:
            runner.run()
            results = PlatformCount.parse_output(runner.cat_output())
            platforms = {key: value for key, value in results}
            result_json['platforms'] = platforms
    except FileNotFoundError:
        pass
    # TypeCount
    try:
        with TypeCount().make_runner() as runner:
            runner.run()
            results = list(TypeCount.parse_output(runner.cat_output()))
            types_dict = {key: value for key, value in results}
            types_list = sorted(types_dict.items(), key=lambda x: x[1], reverse=True)
            result_json['max_types'] = {types_list[0][0]: types_list[0][1]} if types_list else {}
            result_json['types'] = types_dict
    except FileNotFoundError:
        pass
    # MaxPrice
    try:
        with MaxPrice().make_runner() as runner:
            runner.run()
            results = MaxPrice.parse_output(runner.cat_output())
            max_prices = {key: value for key, value in results}
            result_json['max_prices'] = max_prices
    except FileNotFoundError:
        pass
    # MaxDiscount
    try:
        with MaxDiscount().make_runner() as runner:
            runner.run()
            results = MaxDiscount.parse_output(runner.cat_output())
            max_discounts = {key: value for key, value in results}
            result_json['max_discounts'] = max_discounts
    except FileNotFoundError:
        pass

    with open('/app/analyzer/SenrenBanka.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps(result_json, ensure_ascii=False, indent=4))

    print(time.time()-st)


