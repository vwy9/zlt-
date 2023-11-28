# -*- coding = utf-8 -*-
# @Time: 2023/11/21 15:20
# @Author: Wu You
# @File：pre_parquet.py
# @Desc: 说明：预处理parquet数据
# @Software: PyCharm
import os

import pandas as pd
from datetime import datetime, time
from brokenaxes import brokenaxes
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

# 指定中文字体文件的路径
font_path = './reference/Arial Unicode.ttf'
font = FontProperties(fname=font_path)
plt.rcParams['font.family'] = font.get_name()


def pre_process_quote(df):

    # 按照 'time' 列对 DataFrame 进行排序
    df.sort_values(by='time', inplace=True)

    # 检查 'time' 列是否包含关键帧值
    if 92500000 not in df['time'].values:
        # 创建一个新的行，其中 'time' 的值为 93000000
        new_row = pd.DataFrame({
            'symbol': [np.nan],
            'date': [np.nan],
            'time': [93000000],
            'prev_close': [np.nan],
            'open': [np.nan],
            'high': [np.nan],
            'low': [np.nan],
            'last_prc': [np.nan]
        })
        # 添加93000000，并重新设置索引
        df = pd.concat([df, new_row])
        df.sort_values(by='time', inplace=True)
        df.reset_index(drop=True, inplace=True)

    if 150000000 not in df['time'].values:
        # 创建一个新的行，其中 'time' 的值为 150000000
        new_row = pd.DataFrame({
            'symbol': [np.nan],
            'date': [np.nan],
            'time': [150000000],
            'prev_close': [np.nan],
            'open': [np.nan],
            'high': [np.nan],
            'low': [np.nan],
            'last_prc': [np.nan]
        })
        # 添加150000000，并重新设置索引
        df = pd.concat([df, new_row])
        df.sort_values(by='time', inplace=True)
        df.reset_index(drop=True, inplace=True)


    # 裁剪92500000-150000000
    index_location1 = df[df['time'] == 92500000].index.min()
    row_number1 = df.index.get_loc(index_location1)
    index_location2 = df[df['time'] == 150000000].index.min()
    row_number2 = df.index.get_loc(index_location2)
    df = df.loc[row_number1:row_number2].reset_index(drop=True)

    # 我们想保留的列

    columns_to_keep = ['date', 'time', 'recv_time', 'last_prc', 'volume', 'ask_prc1', 'bid_prc1',
                       'ask_vol1', 'bid_vol1', 'high_limited', 'low_limited', 'prev_close', 'open', 'symbol']

    columns_to_drop = [col for col in df.columns if col not in columns_to_keep]

    # 删除那些列
    df.drop(columns=columns_to_drop, inplace=True)
    pd.set_option('display.max_columns', None)

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

    # 将 datetime 对象转换回整数形式的 HHMMSSmmm
    df_resampled['time'] = df_resampled['time'].apply(convert_time_to_int)

    # 将原始 df 的 'time' 列转换回整数形式的 HHMMSSmmm
    df['time'] = df['time'].apply(convert_time_to_int)

    # 在原始的 df 中添加重采样后的数据，reindex
    df = pd.merge(df, df_resampled, how='outer')
    df = df.sort_values('time')
    df.reset_index(drop=True, inplace=True)

    # 包含所有数据：原始数据+填补数据
    print(df)

    # 创建一个新的 'fill' 列，如果行中存在空值则为1，否则为0
    df['fill'] = df.apply(lambda row: 1 if row.isnull().any() else 0, axis=1)
    # 使用上一非空行的值填充空值

    # 创建 "index_Resource" 列，标记所有非空行为 'self'
    df['index_Resource'] = df.apply(lambda x: 'self' if x.notna().all() else np.nan, axis=1)
    last_non_na_index = None

    # 遍历 DataFrame 的每一行
    for idx, row in df.iterrows():
        # 如果当前行是非空行，则更新最近的非空行的索引
        if row['index_Resource'] == 'self':
            last_non_na_index = idx
        else:
            # 如果当前行是空行，则将 "index_Resource" 填充为最近的非空行的索引
            df.at[idx, 'index_Resource'] = last_non_na_index

    last_non_na_rectime = None

    # 遍历 DataFrame 的每一行
    for idx, row in df.iterrows():
        # 如果 "recv_time" 列是非空的，则更新最近的非空行的 "recv_time"
        if pd.notna(row['recv_time']):
            last_non_na_rectime = row['recv_time']
        else:
            # 如果 "recv_time" 是空的，则将 "recv_time" 填充为最近的非空行的 "recv_time"
            df.at[idx, 'recv_time'] = last_non_na_rectime

    # 创建 "Time" 列，标记所有非空行为 'self'
    df['Time'] = df.apply(lambda x: x['time'] if x.notna().all() else np.nan, axis=1)

    last_non_na_time = None

    # 遍历 DataFrame 的每一行
    for idx, row in df.iterrows():
        # 如果 "Time" 列是非空的，则更新最近的非空行的 "Time"
        if pd.notna(row['Time']):
            last_non_na_rectime = row['Time']
        else:
            # 如果 "Time" 是空的，则将 "recv_time" 填充为最近的非空行的 "recv_time"
            df.at[idx, 'Time'] = last_non_na_rectime
    df['Time'] = df['Time'].astype(int)

    # 找到 "time" 列大于等于 93000000 的行的起始索引
    start_index = df[df['time'] >= 93000000].index[0]

    # 初始化平均差值
    avg_diff = 0
    # 初始化非空 "recv_time" 的计数
    count = 0

    # 遍历 "time" 列大于等于 93000000 的每一行
    for idx in range(start_index, len(df)):
        # 如果 "recv_time" 非空
        if not pd.isna(df.loc[idx, 'recv_time']):
            # 更新平均差值和计数
            avg_diff = ((avg_diff * count) + (df.loc[idx, 'recv_time'] - df.loc[idx, 'time'])) / (count + 1)
            count += 1
        else:
            # 如果 "recv_time" 为空，填充平均差值
            df.loc[idx, 'recv_time'] = np.round(df.loc[idx, 'time'] + avg_diff)

    # 寻找 "time" 列为 93000000 的行索引
    start_index = df[df['time'] == 93000000].index[0]

    # 创建新的列 "t_volumn"，其初始值为 0
    df['t_volume'] = 0

    # 对于 "time" 列为 93000000 的行，将 "t_volumn" 列设置为 "volumn" 列的值
    df.loc[start_index, 't_volume'] = df.loc[start_index, 'volume']

    # 在 "time" 列为 93000000 的行之后，计算 "t_volumn" 列的值
    for idx in range(start_index + 1, len(df)):
        df.loc[idx, 't_volume'] = df.loc[idx, 'volume'] - df.loc[idx - 1, 'volume']

    # 找到 "time" 列大于等于 92500000 的行的起始索引
    start_index = df[df['time'] == 93000000].index[0]

    # 创建新的列 "outlier"，初始化为 0
    df['outlier'] = 0

    # 对于每一行
    for idx in range(start_index + 1, len(df)):
        # 只考虑从 start_index 到该行前一行的数据，计算平均值和标准差
        mean = df.loc[start_index:idx - 1, 'last_prc'].mean()
        std = df.loc[start_index:idx - 1, 'last_prc'].std()

        # 如果 "last_price" 列的值在 3σ 之外，则标记为 1，否则为 0
        df.loc[idx, 'outlier'] = np.where((df.loc[idx, 'last_prc'] < mean - 3 * std) |
                                          (df.loc[idx, 'last_prc'] > mean + 3 * std), 1, 0)

    # 从数据框的第93000000行开始
    df['middle'] = None
    start_index1 = df[df['time'] == 93000000].index[0]
    df.loc[:start_index1-1, 'middle'] = 0

    # 逐行应用middle逻辑
    for idx in range(start_index1, len(df)):
        # 提取列的值
        vol = df.loc[idx, 't_volume']
        bid = df.loc[idx, 'bid_prc1']
        ask = df.loc[idx, 'ask_prc1']
        settle_prc = df.loc[idx, 'last_prc']
        pre_settle_prc = df.loc[idx - 1, 'last_prc'] if idx > 0 else None

        if vol:
            if bid and ask:
                df.loc[idx, 'middle'] = settle_prc if bid <= settle_prc <= ask else (bid + ask) / 2
            elif bid:
                df.loc[idx, 'middle'] = max(settle_prc, bid)
            elif ask:
                df.loc[idx, 'middle'] = min(settle_prc, ask)
            else:
                df.loc[idx, 'middle'] = settle_prc
        else:
            if bid and ask:
                df.loc[idx, 'middle'] = (bid + ask) / 2
            elif bid:
                df.loc[idx, 'middle'] = max(pre_settle_prc, bid) if pre_settle_prc is not None else bid
            elif ask:
                df.loc[idx, 'middle'] = min(pre_settle_prc, ask) if pre_settle_prc is not None else ask
            else:
                df.loc[idx, 'middle'] = pre_settle_prc

    df.loc[:start_index1-1, 'acc_weight_price'] = 0
    cumulative_prc_volume = 0
    cumulative_volume = 0

    # 从93000000行开始
    for idx in range(start_index1, len(df)):
        # 计算当前行的last_prc * t_volume和volume
        prc_volume = df.loc[idx, 'last_prc'] * df.loc[idx, 't_volume']
        volume = df.loc[idx, 't_volume']

        # 累计last_prc * t_volume和volume
        cumulative_prc_volume += prc_volume
        cumulative_volume += volume

        # 计算并赋值
        df.loc[idx, 'acc_weight_price'] = cumulative_prc_volume / cumulative_volume if cumulative_volume != 0 else 0

    df['acc_weight_price'] = df['acc_weight_price'].round(2)

    df['stop'] = 0

    for idx, row in df.iterrows():
        # 检查time列的值是否大于等于92500000
        if row['time'] >= 92500000:
            # 如果满足条件，将stop列的值设为1
            if ((row['last_prc'] == row['high_limited'] and row['ask_vol1'] == 0) or
                (row['last_prc'] == row['low_limited'] and row['bid_vol1'] == 0)):
                df.loc[idx, 'stop'] = 1

    df.fillna(method='ffill', inplace=True)

    print(df)

    # 将时间整数转换为 datetime 对象，3s重采样
    df['time'] = df['time'].apply(convert_int_to_time)
    df_resampled1 = df.set_index('time').resample('3S').asfreq()
    df_resampled1.reset_index(inplace=True)

    # 将 datetime 对象转换回整数形式的 HHMMSSmmm
    df_resampled1['time'] = df_resampled1['time'].apply(convert_time_to_int)
    print(df_resampled1)
    df_resampled1.reset_index(inplace=True)

    df_resampled1.drop(df_resampled1[(df_resampled1['time'] > 113000000) & (df_resampled1['time'] < 130000000)].index, inplace=True)
    df_resampled1.reset_index(inplace=True)
    print(df_resampled1)

    df_refer = df

    if volumn_choice == '1':
        v_col = 't_volume'
    elif volumn_choice == '2':
        v_col = 'bid_vol1'
    elif volumn_choice == '3':
        v_col = 'ask_vol1'
    elif volumn_choice == '4':
        v_col = 'volume'
    else:
        print("无效的选择。")
        exit()

    if price_choice == '1':
        p_col = 'last_prc'
    elif price_choice == '2':
        p_col = 'middle'
    elif price_choice == '3':
        p_col = 'bid_prc1'
    elif price_choice == '4':
        p_col = 'ask_prc1'
    elif price_choice == '5':
        p_col = 'acc_weight_price'
    else:
        print("无效的选择。")
        exit()

    df_print = df[['date', 'Time', 'recv_time', p_col, v_col, 'stop', 'fill', 'outlier', 'index_Resource']]

    return df_refer, df_print, df_resampled1


folder_path = './quote/20231127'  # 指定文件夹路径
save_path = './quote/20231127_detail'


# 让用户选择要查看的列
print("1. 最新成交量")
print("2. 买一量")
print("3. 卖一量")
print("4. 过去n个3秒成交量")
volumn_choice = input("请输入你想要查看的数量：")

print("1. 最新成交价")
print("2. 中间价")
print("3. 买一价")
print("4. 卖一价")
print("5. 过去n个3秒加权平均价")
price_choice = input("请输入你想要查看的价格：")

for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        print(f"Processing file: {filename}")  # 打印正在处理的文件名
        file_path = os.path.join(folder_path, filename)
        df1 = pd.read_csv(file_path)

        try:
            df_reference, df_result, df_resample = pre_process_quote(df1)
        except KeyError as e:
            print(f"Error processing file: {filename}, error: {e}")
            continue  # 如果处理文件出错，跳过剩余的代码并处理下一个文件

        # 原始数据
        new_filename = 'processed_reference' + filename
        new_file_path = os.path.join(save_path, new_filename)
        df_reference.to_csv(new_file_path, index=False)

        # 用户指定结果
        new_filename1 = 'processed_result' + filename
        new_file_path = os.path.join(save_path, new_filename1)
        df_result.to_csv(new_file_path, index=False)

        # 画图重采样
        new_filename2 = 'processed_resample' + filename
        new_file_path = os.path.join(save_path, new_filename2)
        df_resample.to_csv(new_file_path, index=False)


