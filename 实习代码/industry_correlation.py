# -*- coding = utf-8 -*-
# @Time: 2023/11/12 15:30
# @Author: Wu You
# @File：industry_correlation.py
# @Desc: 说明：统计指数内行业关联性
#        行业 - WIND一级行业
#        数据库 - Wind金融数据库
# @Software: PyCharm

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

from connect import connect_to_database, wind
from nearest_trading_day import calculate_trade_days
from industry_detail import industry_distribution


# 指定中文字体文件的路径
font_path = './reference/Arial Unicode.ttf'
# 加载中文字体
font = FontProperties(fname=font_path)
# 设置中文字体
plt.rcParams['font.family'] = font.get_name()


# heatmap
def industry_correlation_heatmap(data, s_d, e_d):
    # 计算协方差矩阵
    covariance_matrix = data.cov()

    # 生成相关热力图
    plt.figure(figsize=(10, 8))
    sns.heatmap(covariance_matrix, annot=True, cmap='coolwarm', fmt=".2f", linewidths=0.5)
    plt.title('Industry Correlation Heatmap')
    plt.xticks(rotation=45)
    plt.yticks(rotation=0)
    plt.savefig(f'./result/heatmap/{s_d}_{e_d}')
    plt.show()


if __name__ == "__main__":

    start_date = int(input("请输入开始日期:"))
    end_date = int(input("请输入结束日期:"))
    category = input("请输入指数：")

    conn = connect_to_database(wind)

    # 找到最近的交易日
    # nearest_trading_day = find_nearest_trading_day(start_date, conn)

    data_use = calculate_trade_days(start_date, end_date, conn)
    df1 = pd.DataFrame()
    category_chinese_added = False  # 标志变量，跟踪是否已经添加过category_chinese列

    for a in data_use:
        print(a, "当前交易日详细信息：")
        a = int(a)
        df, name = industry_distribution(conn, a, category)
        df_temp = df[[f'{a}_DangRi']].set_index(df['category_chinese'])
        df1 = pd.concat([df1, df_temp], axis=1)  # 将df_temp添加到df1中并进行横向拼接

    # 去除重复的列
    df1 = df1.loc[:, ~df1.columns.duplicated()]

    # 打印df1
    print(df1)
    df1.to_csv('./result/date_range_ror_detail.csv', index=True)
    # 创建一个字典用于保存结果
    result_dict = {}

    # 遍历DataFrame并获取每行的category和对应的值
    for index, row in df1.iterrows():
        category = row['category_chinese']
        values = row.drop('category_chinese').values.tolist()

        # 将category和对应的值添加到结果字典中
        result_dict[category] = values

    print(result_dict)
    data = pd.DataFrame(result_dict)
    industry_correlation_heatmap(data, start_date, end_date)
    conn.close()



