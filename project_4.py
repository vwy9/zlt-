# -*- coding = utf-8 -*-
# @Time: 2023/11/22 9:15
# @Author: Wu You
# @File：project_4.py
# @Desc: 说明：预处理，定义功能函数
# @Software: PyCharm
# 市值加权指数 = ∑(股票价格 × 股票的发行股本) / 指数基期总市值 × 指数基期点位
# 等权重指数 = ∑(股票价格 × 相同权重) / 指数基期总市值 × 指数基期点位
# 流通市值加权指数 = ∑(股票价格 × 股票的流通股本) / 指数基期总市值 × 指数基期点位
import decimal
from _decimal import Decimal
from datetime import date
import time
import ray

import pandas as pd
import os

from v2_code.connect import connect_to_database, wind
from v2_code.factor_class import Factorcalculation
from v2_code.components_class import Componentsacquisition
from v2_code.utilities_class import Tradedates, Datapreprocess, Plot


# 市场权重
def assign_market_weight(components_df, weight_list):
    if components_df['S_INFO_WINDCODE'].startswith(('300', '301')):
        return weight_list[0]
    elif components_df['S_INFO_WINDCODE'].startswith(('688', '689')):
        return weight_list[1]
    elif components_df['S_INFO_WINDCODE'].startswith(("000", "001", "002", "003", "004")):
        return weight_list[2]
    elif components_df['S_INFO_WINDCODE'].startswith(("600", "601", "603", "605")):
        return weight_list[3]

@ray.remote
def process_data(date):
    conn = connect_to_database(wind)
    index = '000016.SH'
    c = Componentsacquisition(conn, index, date)
    name_str, components_df = c.components_ac()

    # index_industry_distribution = c.industry_distribution()
    # Plot.single_industry_plt(index_industry_distribution, name_str)

    # 获取components_list全部的quote---csv数据
    components_list = components_df['S_INFO_WINDCODE'].tolist()
    Datapreprocess.components_save_to_csvf(components_list, date)
    index_list = [index]
    Datapreprocess.index_save_to_csvf(index_list, date)
    Datapreprocess.pre_quote_information(date, conn)
    Datapreprocess.resample_3s(date)
    Datapreprocess.weight_martrix(date)
    # Plot.quote_3s_matplt(date, index)

    conn.close()

    return None


if __name__ == "__main__":


    # market_weight = input('请输入创业、科创、深主、沪主的权重配比： ')
    # market_weight_list = list(map(int, re.findall(r'\d+', market_weight)))
    start_date = 20231201
    end_date = 20231225
    conn = connect_to_database(wind)
    trade_day = Tradedates(conn)
    trade_dates = trade_day.calculate_trade_days(start_date, end_date)['TRADE_DAYS'].tolist()
    conn.close()
    ray.init()
    futures = [process_data.remote(date) for date in trade_dates]
    results = ray.get(futures)
    ray.shutdown()

