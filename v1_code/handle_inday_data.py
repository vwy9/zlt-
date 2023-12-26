
from datetime import datetime
import os
import glob

import numpy as np
import pandas as pd


def convert_int_to_time(time_int):
    time_str = str(time_int).zfill(9)
    formatted_time_str = f'{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}.{time_str[6:]}'
    datetime_object = datetime.strptime(formatted_time_str, '%H:%M:%S.%f')
    return datetime_object


# 定义一个函数将 datetime 对象转换为整数形式的 HHMMSSmmm
def convert_time_to_int(time_obj):
    time_str = time_obj.strftime('%H%M%S%f')[:-3]  # 去掉最后3个字符，即微秒部分
    return int(time_str)


def resample_3s(date):

    folder_path = f'./quote/{date}_detail'  # 文件夹路径

    for filename in os.listdir(folder_path):
        if filename.startswith('processed_') and filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            output_filename = 'resampled' + filename
            # print(f"Processed file: {filename}")
            # 读取CSV文件
            df = pd.read_csv(file_path)
            df['time'] = df['time'].apply(convert_int_to_time)
            df_resampled = df.set_index('time').resample('3S').asfreq()
            df_resampled.reset_index(inplace=True)
            df_resampled['time'] = df_resampled['time'].apply(convert_time_to_int)
            df_resampled.reset_index(inplace=True)

            df_resampled.drop(df_resampled[(df_resampled['time'] > 113000000) & (df_resampled['time'] < 130000000)].index,
                               inplace=True)

            df_resampled.drop(
                df_resampled[(df_resampled['time'] < 93000000)].index,
                inplace=True)
            df_resampled.reset_index(inplace=True)

            # 保存结果 DataFrame 到新的 CSV 文件
            output_path = os.path.join(folder_path, output_filename)
            df_resampled.to_csv(output_path, index=False)
            # print(f"Output file: {output_path}")


def weight_martrix(date):
    folder_path = f'./quote/{date}_detail'  # 文件夹路径

    # 用于存储每个文件的权重值DataFrame列表
    weights_dfs = []

    # 获取所有匹配的文件并进行排序
    filenames = [filename for filename in os.listdir(folder_path) if
                 filename.startswith('resampled') and filename.endswith('.csv')]
    sorted_filenames = sorted(filenames)

    # 遍历排序后的每个文件
    for filename in sorted_filenames:
        file_path = os.path.join(folder_path, filename)  # 拼接完整的文件路径
        df_temp = pd.read_csv(file_path)

        if 'weight_prc' in df_temp.columns:
            # 提取 'weight_prc' 列并将其重命名为不带.csv的文件名
            df_temp = df_temp[['weight_prc']].rename(columns={'weight_prc': filename[28:-7]})  # 假设文件名的格式正确且以 .csv 结尾
            weights_dfs.append(df_temp)

    # 使用concat横向拼接所有DataFrame
    df_weights_concatenated = pd.concat(weights_dfs, axis=1)

    min_value = df_weights_concatenated.iloc[0]  # 获取第一行的最小值

    # 使用广播运算将每一列除以最小值
    normalized_data = df_weights_concatenated.divide(min_value)

    # print(normalized_data)

    df_weights_concatenated.to_csv(f'./quote/{date}_detail/weight_matrix.csv')

    # 返回横向拼接后的DataFrame
    return df_weights_concatenated



def last_prc_martrix():
    folder_path = '../quote/20231201_detail'  # 文件夹路径

    # 用于存储每个文件的权重值DataFrame列表
    last_prc_dfs = []

    # 获取所有匹配的文件并进行排序
    filenames = [filename for filename in os.listdir(folder_path) if
                 filename.startswith('resampled') and filename.endswith('.csv')]
    sorted_filenames = sorted(filenames)

    # 遍历排序后的每个文件
    for filename in sorted_filenames:
        file_path = os.path.join(folder_path, filename)  # 拼接完整的文件路径
        df_temp = pd.read_csv(file_path)

        if 'last_prc' in df_temp.columns:
            # 提取 'weight_prc' 列并将其重命名为不带.csv的文件名
            df_temp = df_temp[['last_prc']].rename(columns={'last_prc': filename[28:-7]})  # 假设文件名的格式正确且以 .csv 结尾
            last_prc_dfs.append(df_temp)


    # 使用concat横向拼接所有DataFrame
    df_last_prc_concatenated = pd.concat(last_prc_dfs, axis=1)

    df_last_prc_concatenated.to_csv('./quote/20231201_detail/last_prc_matrix.csv')

    # 返回横向拼接后的DataFrame
    return df_last_prc_concatenated
# #
# last_prc_martrix()









