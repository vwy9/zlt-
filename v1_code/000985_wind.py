# -*- coding = utf-8 -*-
# @Time: 2023/11/12 15:30
# @Author: Wu You
# @File：000985_wind.py
# @Desc: 说明：中证全指的成分股是否是由：wind一级行业指数的所有成分股组成，
#             如有遗漏或者差异记录一下
# @Software: PyCharm

import pandas as pd
import csv


csi_constituents = list(pd.read_csv('../reference/old/中证全有效成分股.csv')['S_CON_WINDCODE'])
wind_constituents = list(pd.read_csv('wind_components.csv')['S_CON_WINDCODE'])

wind_constituents = list(set(wind_constituents))
print(len(wind_constituents))
csi_constituents = list(set(csi_constituents))
print(len(csi_constituents))


def diff_miss():
    diff_constituents = []
    miss_constituents = []

    for stock in csi_constituents:
        if stock not in wind_constituents:
            diff_constituents.append(stock)
    diff_constituents = list(set(diff_constituents))
    print('在中证全指中但不在wind一级行业分类的成分股共有：', len(diff_constituents))
    print('分别为：', diff_constituents)

    for stock in wind_constituents:
        if stock not in csi_constituents:
            miss_constituents.append(stock)
    miss_constituents = list(set(miss_constituents))
    print('在wind一级行业分类中但不在中证全指的成分股共有：', len(miss_constituents))
    print('分别为：', miss_constituents)

    with open('../reference/components_miss_diff/filtered_miss_constituents_wind.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Stock'])  # 写入表头
        writer.writerows([[stock] for stock in miss_constituents])



if __name__ == "__main__":
    # detail_index()
    diff_miss()
