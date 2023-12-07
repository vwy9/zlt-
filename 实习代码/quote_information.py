# -*- coding = utf-8 -*-
# @Time: 2023/11/22 9:15
# @Author: Wu You
# @File：quote_information.py
# @Desc: 说明：预处理，定义功能函数
# @Software: PyCharm
# 市值加权指数 = ∑(股票价格 × 股票的发行股本) / 指数基期总市值 × 指数基期点位
# 等权重指数 = ∑(股票价格 × 相同权重) / 指数基期总市值 × 指数基期点位
# 流通市值加权指数 = ∑(股票价格 × 股票的流通股本) / 指数基期总市值 × 指数基期点位
import decimal
from _decimal import Decimal
from datetime import date, datetime
import re

import numpy as np
import pandas as pd
import os
import math

from brokenaxes import brokenaxes
from matplotlib import pyplot as plt

from connect import connect_to_database, wind
from components_information_acquisition import components_ac, remove_from_list, \
     remove_nomore_n_days, add_elements, st_remove, sh50_components_freeshare, industry_distrubution
from nearest_trading_day import calculate_trade_days
from pre_parquet import pre_process_quote, rsy_calculation
from industry_market_plt import double_index_industry_plt



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


# 保存quote
def save_to_csvf(components_list):
    # 筛选出以 "sh" 结尾的元素
    listsh = [component for component in components_list if component.endswith("SH")]
    listsh_numbers_only = [item.split('.')[0] for item in listsh]
    # 筛选出以 "sz" 结尾的元素
    listsz = [component for component in components_list if component.endswith("SZ")]
    listsz_numbers_only = [item.split('.')[0] for item in listsz]

    # 定义你想要创建的文件夹的路径
    folder_path = f"./quote/{date}"

    # 使用 os.path.exists() 检查文件夹是否已经存在
    if not os.path.exists(folder_path):
        # 如果文件夹不存在，则使用 os.makedirs() 创建文件夹
        os.makedirs(folder_path)

    for stock in listsh_numbers_only:
        file_path = f'/Volumes/data/tick/stock/{date}/quote/sh_{stock}_{date}_quote.parquet'  # 替换为实际的文件路径
        df = pd.read_parquet(file_path)
        df.to_csv(f'./quote/{date}/{stock}.SH.csv')

    for stock in listsz_numbers_only:
        file_path = f'/Volumes/data/tick/stock/{date}/quote/sz_{stock}_{date}_quote.parquet'  # 替换为实际的文件路径
        df = pd.read_parquet(file_path)
        df.to_csv(f'./quote/{date}/{stock}.SZ.csv')


# # 上证50靠档
# def process_rate(value):
#     if value <= 15:
#         return math.ceil(value)
#     elif value > 15 and value <= 20:
#         return 20
#     elif value > 20 and value <= 30:
#         return 30
#     elif value > 30 and value <=40:
#         return 40
#     elif value > 40 and value <= 50:
#         return 50
#     elif value > 50 and value <= 60:
#         return 60
#     elif value > 60 and value <= 70:
#         return 70
#     elif value > 70 and value <= 80:
#         return 80
#     elif value > 80:
#         return 100
#     else:
#         return value
#
#
# # 计算成分股权重
# def components_weight(df_shr, df_indicator):
#
#     df_merge = df_shr.merge(df_indicator, on='S_INFO_WINDCODE')
#     # 计算调整股本'ADJ_SHR_TODAY'
#     df_merge['rate'] = df_merge['FREE_SHARES_TODAY'] / df_merge['TOT_SHR_TODAY'] * 100
#     df_merge['ADJrate'] = df_merge['rate'].apply(process_rate)
#     df_merge['ADJ_SHR_TODAY'] = df_merge['ADJrate'] * df_merge['TOT_SHR_TODAY'] / 100
#     folder_path1 = f"./quote/{date}/detail_information"
#     # 使用 os.path.exists() 检查文件夹是否已经存在
#     if not os.path.exists(folder_path1):
#         # 如果文件夹不存在，则使用 os.makedirs() 创建文件夹
#         os.makedirs(folder_path1)
#     # 定义等权重列，并保存csv
#     df_merge['equal_weight'] = 1
#     df_result = df_merge[['S_INFO_WINDCODE', 'equal_weight',
#                           'S_DQ_CLOSE', 'S_DQ_OPEN', 'S_DQ_PRECLOSE',
#                           'S_DQ_ADJCLOSE', 'S_DQ_ADJOPEN', 'S_DQ_ADJPRECLOSE']]
#     df_merge.to_csv(f'./quote/{date}/detail_information/components.csv')
#
#     return df_result


def pre_quote_information(date):

    folder_path = f"./quote/{date}"
    save_path = f"./quote/{date}_detail"
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            print(f"Processing file: {filename}")  # 打印正在处理的文件名
            file_path = os.path.join(folder_path, filename)
            df1 = pd.read_csv(file_path)
            rsy_sigma = rsy_calculation(conn, df1, date)
            try:
                df_reference = pre_process_quote(df1, rsy_sigma)
            except KeyError as e:
                print(f"Error processing file: {filename}, error: {e}")
                continue  # 如果处理文件出错，跳过剩余的代码并处理下一个文件

            # 原始数据
            new_filename = 'processed_reference' + filename
            new_file_path = os.path.join(save_path, new_filename)
            df_reference.to_csv(new_file_path, index=False)


# 自由流通市值加权
def weight_prc(date, capital_df):
    folder_path = f'./quote/{date}_detail'  # 文件夹1的路径

    # 遍历文件夹中的所有文件
    for filename in os.listdir(folder_path):
        if filename.startswith('processed_reference') and filename.endswith(
                '.csv'):  # 筛选以"processed_reference"开头且以.csv结尾的文件
            file_path = os.path.join(folder_path, filename)  # 文件的完整路径
            # 读取CSV文件
            df1 = pd.read_csv(file_path)
            # 找出'symbol'列的众数
            mode_symbol = df1['symbol'].mode()
            start_index = capital_df[capital_df['S_INFO_WINDCODE'] == mode_symbol[0]].index[0]
            cal_weight_value = capital_df.loc[start_index, 'SHR_CALCULATION']
            df1['SHR_CALCULATION'] = cal_weight_value
            df1['last_prc'] = df1['last_prc'].apply(lambda x: Decimal(x))
            df1['weight_prc'] = df1['last_prc'] * df1['SHR_CALCULATION']
            df1['SHR_CALCULATION'] = df1['SHR_CALCULATION'].apply(lambda x: x.quantize(decimal.Decimal('0.00000000')))
            # 保存修改后的DataFrame到同名文件
            output_path = os.path.join(folder_path, filename)
            df1.to_csv(output_path, index=False)
            print(f"Processed file: {filename}")


# 整合成分股quote数据
def final_to_plt(date):

    folder_path = f'quote/{date}_detail'  # 文件夹路径
    output_filename = f'./{date}_result.csv'  # 输出文件名

    result_data = {}  # 用于存储结果的字典

    for filename in os.listdir(folder_path):
        if filename.startswith('processed_') and filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)  # 文件的完整路径
            print(f"Processed file: {filename}")
            # 读取CSV文件
            df = pd.read_csv(file_path)

            # 遍历每一行
            for index, row in df.iterrows():
                time = row['time']
                weight_prc = row['weight_prc']

                # 将时间和加总结果添加到结果字典
                if time in result_data:
                    result_data[time] += weight_prc
                else:
                    result_data[time] = weight_prc

    # 创建结果 DataFrame
    result_df = pd.DataFrame(result_data.items(), columns=['time', 'prc_sum'])
    result_df['prc_sum'] = result_df['prc_sum'].round(8)  # 保留8位小数
    result_df = result_df.groupby('time')['prc_sum'].sum().reset_index()
    result_df = result_df.sort_values('time')
    print(result_df)
    # 保存结果 DataFrame 到新的 CSV 文件
    output_path = os.path.join(folder_path, output_filename)
    result_df.to_csv(output_path, index=False)
    print(f"Output file: {output_path}")


# 画图
def quote_plt(date):

    df = pd.read_csv(f'./quote/{date}_detail/{date}_result.csv')

    def convert_int_to_time(time_int):
        time_str = str(time_int).zfill(9)
        formatted_time_str = f'{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}.{time_str[6:]}'
        datetime_object = datetime.strptime(formatted_time_str, '%H:%M:%S.%f')
        return datetime_object

    # 定义一个函数将 datetime 对象转换为整数形式的 HHMMSSmmm
    def convert_time_to_int(time_obj):
        time_str = time_obj.strftime('%H%M%S%f')[:-3]  # 去掉最后3个字符，即微秒部分
        return int(time_str)

    # 将时间整数转换为 datetime 对象，3s重采样
    df['time'] = df['time'].apply(convert_int_to_time)
    df_resampled = df.set_index('time').resample('3S').asfreq()
    df_resampled.reset_index(inplace=True)
    df_resampled['time'] = df_resampled['time'].apply(convert_time_to_int)
    df_resampled.reset_index(inplace=True)
    df_resampled.drop(df_resampled[(df_resampled['time'] > 113000000) & (df_resampled['time'] < 130000000)].index,
                      inplace=True)
    df_resampled.reset_index(inplace=True)
    # 保存components_quote 3s重采样结果
    df_resampled.to_csv('./resampled.csv')

    df3 = pd.read_csv('./20231201_result_index.csv')

    # 将时间整数转换为 datetime 对象，3s重采样
    df3['time'] = df3['time'].apply(convert_int_to_time)
    df3_resampled = df3.set_index('time').resample('3S').asfreq()
    df3_resampled.reset_index(inplace=True)
    df3_resampled['time'] = df3_resampled['time'].apply(convert_time_to_int)
    df3_resampled.reset_index(inplace=True)
    df3_resampled.drop(df3_resampled[(df3_resampled['time'] > 113000000) & (df3_resampled['time'] < 130000000)].index,
                       inplace=True)
    df3_resampled.reset_index(inplace=True)
    pd.set_option('display.max_rows', None)
    print(df3_resampled)
    # 保存index_quote 3s重采样结果
    df3_resampled.to_csv('./resampled2.csv')

    df3_part1 = df3_resampled[df3_resampled['time'] <= 113000000]
    df3_part2 = df3_resampled[df3_resampled['time'] >= 130000000]

    # 创建数据
    df_resampled1 = df_resampled[df_resampled['time'] <= 113000000]
    df_resampled2 = df_resampled[df_resampled['time'] >= 130000000]

    # 创建一个新的图形，并设置其大小
    fig = plt.figure(figsize=(35, 15))

    # 创建一个带有断点的轴
    bax = brokenaxes(xlims=((df_resampled1['time'].min(), 113000000), (130000000, df_resampled2['time'].max())),
                     fig=fig, hspace=0.30)

    # 在每个断点的部分上绘制数据
    bax.plot(df_resampled1['time'], df_resampled1['prc_sum'], label='成分股上午时段', color='red')
    bax.plot(df_resampled2['time'], df_resampled2['prc_sum'], label='成分股下午时段', color='red')
    bax.plot(df3_part1['time'], df3_part1['last_prc'], label='指数上午时段', color='black')
    bax.plot(df3_part2['time'], df3_part2['last_prc'], label='指数下午时段', color='black')

    # 设置 x 轴的标签
    xticks1 = np.linspace(df_resampled1['time'].min(), df_resampled1['time'].max(), 3)
    xticks2 = np.linspace(df_resampled2['time'].min(), df_resampled2['time'].max(), 3)

    bax.axs[0].set_xticks(xticks1)
    bax.axs[0].set_xticklabels([f"{x:.0f}" for x in xticks1])

    bax.axs[1].set_xticks(xticks2)
    bax.axs[1].set_xticklabels([f"{x:.0f}" for x in xticks2])

    # 调整 y 轴的范围
    y_min = df3['last_prc'].min() - 0.0001
    y_max = df_resampled['prc_sum'].max() + 0.0001
    bax.axs[0].set_ylim(y_min, y_max)  # 设置第一个断点轴的 y 轴范围
    bax.axs[1].set_ylim(y_min, y_max)  # 设置第二个断点轴的 y 轴范围

    plt.title(f"{date}捏合指数日内行情")
    font_properties = {'weight': 'bold'}
    bax.axs[1].legend(loc="upper right", fontsize=20, title_fontsize=24, prop=font_properties)
    plt.savefig(f'./{date}.png')

    plt.show()


# 比较指数和捏合指数三秒行情收益率差异
def indat_ror_comparison():
    df = pd.read_csv('./resampled.csv')
    start_idx = (df['time'] == 93030000).idxmax()
    df1 = df.loc[start_idx:]
    df2 = pd.DataFrame()
    df2['ror_components'] = (df1['prc_sum'] - df1['prc_sum'].shift(1)) / df1['prc_sum'].shift(1)
    df2 = df2.reset_index()

    df3 = pd.read_csv('./resampled2.csv')
    start_idx1 = (df3['time'] == 93030000).idxmax()

    df4 = df3.loc[start_idx1:]
    df4 = df4[['index', 'last_prc']]
    df5 = pd.DataFrame()
    df5['ror_index'] = (df4['last_prc'] - df4['last_prc'].shift(1)) / df4['last_prc'].shift(1)

    df5 = df5.merge(df2)
    df5.to_csv('./inday_ror.csv')
    print(df5)


if __name__ == "__main__":

    conn = connect_to_database(wind)
    index = input('请输入指数：')
    market_weight = input('请输入创业、科创、深主、沪主的权重配比： ')
    market_weight_list = list(map(int, re.findall(r'\d+', market_weight)))
    start_date = int(input('请输入开始日期：'))
    end_date = int(input("请输入结束日期："))
    trade_dates = calculate_trade_days(start_date, end_date, conn)
    for date in trade_dates:
        name_str, components_list = components_ac(conn, index, date)
        index_industry_distribution = industry_distrubution(conn, components_list)
        components_stremove_list = st_remove(conn, components_list, date)
        components_ndaysremove_list = remove_nomore_n_days(conn, components_stremove_list, 0, date)
        remove_appointed_list = remove_from_list(components_ndaysremove_list)
        final_list = add_elements(remove_appointed_list)
        components_industry_distribution = industry_distrubution(conn, final_list)
        double_index_industry_plt(index_industry_distribution, components_industry_distribution, name_str, '定制指数')
        print(f'基于{name_str}下的定制指数于交易日{date}下的数量的为{len(final_list)}成分为：')
        print(final_list)
        # 获取components_list全部的quote---csv数据
        save_to_csvf(components_list)
        pre_quote_information(date)
        capital_today_df = sh50_components_freeshare(conn, date)
        weight_prc(date, capital_today_df)
        final_to_plt(date)
        quote_plt(date)





    conn.close()

