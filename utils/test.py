# coding: utf-8
# @Author: Xayanium

import time
from mrjob.job import MRJob
import csv
import ast

class BaseCount(MRJob):
    def mapper(self, _, line):
        row = next(csv.reader([line]))
        try:
            # 抽象处理逻辑，调用子类中的方法获取键值对
            for key, value in self.process_row(row):
                yield key, value
        except (IndexError, ValueError):
            pass

    def reducer(self, key, counts):
        yield key, sum(counts)

    def process_row(self, row):
        # 子类应实现此方法以提供键值对生成逻辑
        raise NotImplementedError

class YearCount(BaseCount):
    def process_row(self, row):
        year = int(row[4].split(' ')[0])
        return [( year, 1 )]

class ReviewCount(BaseCount):
    def process_row(self, row):
        review = row[5].split(' ')[0]
        return [( review, 1 )]

class PriceCount(BaseCount):
    def process_row(self, row):
        price = int(float(row[8])//25)*25
        return [( price, 1 )]

class PlatformCount(BaseCount):
    def process_row(self, row):
        try:
            platforms = ast.literal_eval(row[3])
            return [(platform, 1) for platform in platforms]
        except (SyntaxError, ValueError):
            pass


if __name__ == '__main__':
    st = time.time()
    jobs = [YearCount, ReviewCount, PriceCount, PlatformCount]
    for job_class in jobs:
        job_class.run()
    print("Total time:", time.time() - st)