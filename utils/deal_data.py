# coding: utf-8
# @Author: Xayanium

"""
需要处理的数据:

"""
import json
import time

from mrjob.job import MRJob
from mrjob.step import MRStep
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
        except (IndexError, ValueError, SyntaxError):
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
            if review == 'review_summary':
                return
            yield review, 1
        except (IndexError, ValueError, SyntaxError):
            pass

    def reducer(self, review, counts):
        yield review, sum(counts)


class PriceCount(MRJob):
    def mapper(self, _, line):
        row = next(csv.reader([line]))
        try:
            price = int(float(row[8])//25)*25
            yield price, 1
        except (IndexError, ValueError, SyntaxError):
            pass

    def reducer(self, price, counts):
        yield f'{price, price+24}', sum(counts)


class PlatformCount(MRJob):
    def mapper(self, _, line):
        row = next(csv.reader([line]))
        try:
            platform_list = row[3]
            for platform in ast.literal_eval(platform_list):
                yield platform, 1
        except (IndexError, ValueError, SyntaxError):
            pass

    def reducer(self, price, counts):
        yield price, sum(counts)


class TypeCount(MRJob):
    def mapper(self, _, line):
        row = next(csv.reader([line]))
        try:
            type_tags = row[10]
            for tag in ast.literal_eval(type_tags):
                yield tag, 1
        except (IndexError, ValueError, SyntaxError):
            pass

    def reducer(self, tag, counts):
        yield tag, sum(counts)


# 直接使用TypeCount计算出来的值
# class MaxType(MRJob):
#     def mapper(self, _, line):
#         row = next(csv.reader([line]))
#         try:
#             type_tags = row[10]
#             for tag in ast.literal_eval(type_tags):
#                 yield tag, 1
#         except (IndexError, ValueError, SyntaxError):
#             pass
#
#     def reducer(self, tag, counts):
#         yield None, (tag, sum(counts))
#
#     @staticmethod
#     def reducer_max_types(_, tag_pairs):
#         max_count = 0
#         tags = []
#         for tag, count in tag_pairs:
#             if count > max_count:
#                 max_count = count
#                 tags = [tag]
#             elif count == max_count:
#                 tags.append(tag)
#         for tag in tags:
#             yield tag, max_count
#
#     def steps(self):
#         return [
#             MRStep(
#                 mapper=self.mapper,
#                 reducer=self.reducer
#             ),
#             MRStep(
#                 reducer=self.reducer_max_types
#             )
#         ]


class MaxDiscount(MRJob):
    def mapper(self, _, line):
        row = next(csv.reader([line]))
        try:
            discount = int(row[6])
            name = row[1]
            yield name, discount
        except (IndexError, ValueError, SyntaxError):
            pass

    def reducer(self, name, counts):
        yield None, (name, max(counts))

    @staticmethod
    def reducer_max_discount(_, discount_pairs):
        max_discount = 0
        names = []
        for name, discount in discount_pairs:
            if discount > max_discount:
                max_discount = discount
                names = [name]
            elif discount == max_discount:
                names.append(name)
        for name in names:
            yield name, max_discount

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            ),
            MRStep(
                reducer=self.reducer_max_discount
            )
        ]


class MaxPrice(MRJob):
    def mapper(self, _, line):
        row = next(csv.reader([line]))
        try:
            name = row[1]
            price = float(row[8])
            # print(price)
            yield name, price
        except (IndexError, ValueError, SyntaxError):
            pass

    def reducer(self, name, counts):
        yield None, (name, max(counts))

    @staticmethod
    def reducer_max_price(_, price_pairs):
        max_price = 0
        names = []
        for name, price in price_pairs:
            if price > max_price:
                max_price = price
                names = [name]
            elif price == max_price:
                names.append(name)
        for name in names:
            yield name, max_price

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            ),
            MRStep(
                reducer=self.reducer_max_price
            )
        ]


if __name__ == '__main__':
    st = time.time()
    # 将输出写入文件
    result_json = {}
    # YearCount
    try:
        # YearCount().make_runner() 创建一个用于执行 YearCount MRJob 作业的 runner 对象
        with YearCount().make_runner() as runner:
            runner.run()
            # runner.cat_output() 方法获取作业的所有输出并将其作为字符串返回
            # YearCount().parse_output() 方法将这些输出解析为键值对
            results = YearCount().parse_output(runner.cat_output())  # 得到一个生成器对象
            years = {key: value for key, value in results}
            result_json['years'] = years
    except FileNotFoundError:
        pass

    # ReviewCount
    try:
        with ReviewCount().make_runner() as runner:
            runner.run()
            results = ReviewCount().parse_output(runner.cat_output())
            reviews = {key: value for key, value in results}
            result_json['reviews'] = reviews
    except FileNotFoundError:
        pass

    # PriceCount
    try:
        with PriceCount().make_runner() as runner:
            runner.run()
            results = PriceCount().parse_output(runner.cat_output())
            prices_list = sorted({key: value for key, value in results}.items(), key=lambda x: (len(x[0]), x[0]))
            result_json['prices'] = dict(prices_list)
    except FileNotFoundError:
        pass

    # PlatformCount
    try:
        with PlatformCount().make_runner() as runner:
            runner.run()
            results = PlatformCount().parse_output(runner.cat_output())
            platforms = {key: value for key, value in results}
            result_json['platforms'] = platforms
    except FileNotFoundError:
        pass

    # TypeCount
    try:
        with TypeCount().make_runner() as runner:
            runner.run()
            results = TypeCount().parse_output(runner.cat_output())
            # 排序后是一个嵌套列表, 内层列表有两个元素, 分别为key, value
            types_list = sorted({key: value for key, value in results}.items(), key=lambda x: x[1], reverse=True)
            result_json['max_types'] = {types_list[0][0]: types_list[0][1]}  # 排序后得到的列表的第一个元素, 就是MaxType
            result_json['types'] = dict(types_list)
    except FileNotFoundError:
        pass

    # MaxType
    # 直接用TypeCount计算出来的值
    # try:
    #     with MaxType().make_runner() as runner:
    #         runner.run()
    #         results = MaxType().parse_output(runner.cat_output())
    #         max_types = {key: value for key, value in results}
    #         # with open('counts/max_types.txt', 'w', encoding='utf-8') as f_max_type:
    #         #     f_max_type.write(json.dumps(max_types, ensure_ascii=False, indent=4))
    #         result_json['max_types'] = max_types
    # except FileNotFoundError:
    #     pass

    # MaxPrice
    try:
        with MaxPrice().make_runner() as runner:
            runner.run()
            results = MaxPrice().parse_output(runner.cat_output())
            max_prices = {key: value for key, value in results}
            result_json['max_prices'] = max_prices
    except FileNotFoundError:
        pass

    # MaxDiscount
    try:
        with MaxDiscount().make_runner() as runner:
            runner.run()
            results = MaxDiscount().parse_output(runner.cat_output())
            max_discounts = {key: value for key, value in results}
            result_json['max_discounts'] = max_discounts
    except FileNotFoundError:
        pass

    with open('SenrenBanka.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps(result_json, ensure_ascii=False, indent=4))

    print(time.time()-st)


