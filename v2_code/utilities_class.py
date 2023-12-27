import decimal
import os
from datetime import datetime
import numpy as np
import pandas as pd
import pymysql
from brokenaxes import brokenaxes
from matplotlib import pyplot as plt, ticker
from matplotlib.font_manager import FontProperties


from v2_code.connect_class import DatabaseConnector, wind
from v2_code.factor_class import Factorcalculation

# 指定中文字体
font_path = './reference/Arial Unicode.ttf'
font = FontProperties(fname=font_path)
plt.rcParams['font.family'] = font.get_name()


# 模糊搜索
class Fuzzysearch:
    def __init__(self):
        self.db_connector = DatabaseConnector(wind)
        self.searched_data = None

    def fuzzy_search(self, table_name, search_string, column='column_name'):
        with self.db_connector.get_connection() as conn:
            with conn.cursor() as cursor:
                query = f"SELECT * FROM {table_name} WHERE {column} LIKE %s  " \
                        f"AND S_CON_OUTDATE IS NULL AND CUR_SIGN = 1"
                # 包含
                search_value = f"%{search_string}%"
                # 以_结尾
                # search_value = f"{search_string}%"
                # 以_开头
                # search_value = f"%{search_string}"
                # 执行查询
                cursor.execute(query, (search_value,))
                result = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                selected_data = pd.DataFrame(result, columns=column_names)
                print('共有', len(selected_data), '条符合条件的数据')
            self.searched_data = selected_data
            return selected_data


# fuzzy search demo
# search = Fuzzysearch()
# search.fuzzy_search('AINDEXMEMBERS', '000016.SH', column='S_INFO_WINDCODE')


# 交易日查询
class Tradedates:
    def __init__(self, conn):
        self.trade_days = None
        self.conn = conn

    def calculate_trade_days(self, start_date, end_date):

        query = f"SELECT DISTINCT TRADE_DAYS FROM ASHARECALENDAR " \
                f"WHERE CAST(TRADE_DAYS AS SIGNED) BETWEEN '{start_date}' AND '{end_date}'"
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            selected_data = pd.DataFrame(results, columns=columns)
            cursor.close()
        except pymysql.Error as e:
            print(f"执行查询日期区间内交易日时发生错误: {e}")

        return selected_data

    def find_nearest_trading_day(self, target_date):

        target_date = int(target_date)
        query = f"SELECT MAX(CAST(TRADE_DAYS AS SIGNED)) FROM ASHARECALENDAR " \
                f"WHERE CAST(TRADE_DAYS AS SIGNED)<={target_date}"
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            nearest_trading_day = cursor.fetchone()[0]
            cursor.close()
        except pymysql.Error as e:
            print(f"执行查询最新交易日时发生错误: {e}")

        if nearest_trading_day is not None:
            if nearest_trading_day == target_date:
                return target_date
            else:
                return nearest_trading_day
        else:
            return None


# 交易日查询demo
# trade_date = Tradedates()
# 当前最近交易日功能
# nearest_trading_day = trade_date.find_nearest_trading_day(20230922)
# print(nearest_trading_day)
# 区间内交易日功能
# trade_dates = trade_date.calculate_trade_days(20230921, 20231021)
# print(trade_dates)


# 静态方法
class Datapreprocess:

    # # 上证50靠档
    # @staticmethod
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

    # 定义一个函数将 int 对象转换为datetime 形式的 HHMMSSmmm
    @staticmethod
    def convert_int_to_time(time_int):
        time_str = str(time_int).zfill(9)
        formatted_time_str = f'{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}.{time_str[6:]}'
        datetime_object = datetime.strptime(formatted_time_str, '%H:%M:%S.%f')
        return datetime_object

    # 定义一个函数将 datetime 对象转换为整数形式的 HHMMSSmmm
    @staticmethod
    def convert_time_to_int(time_obj):
        time_str = time_obj.strftime('%H%M%S%f')[:-3]  # 去掉最后3个字符，即微秒部分
        return int(time_str)

    # 将成分股保存在指定文件夹
    @staticmethod
    def components_save_to_csvf(components_list, date):
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
            df.to_csv(f'./quote/{date}/{stock}_SH.csv', index=False)

        for stock in listsz_numbers_only:
            file_path = f'/Volumes/data/tick/stock/{date}/quote/sz_{stock}_{date}_quote.parquet'  # 替换为实际的文件路径
            df = pd.read_parquet(file_path)
            df.to_csv(f'./quote/{date}/{stock}_SZ.csv', index=False)

    @staticmethod
    def index_save_to_csvf(components_list, date):
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
            df.to_csv(f'./quote/{date}/index_{stock}.SH.csv', index=False)

        for stock in listsz_numbers_only:
            file_path = f'/Volumes/data/tick/stock/{date}/quote/sz_{stock}_{date}_quote.parquet'  # 替换为实际的文件路径
            df = pd.read_parquet(file_path)
            df.to_csv(f'./quote/{date}/index_{stock}.SZ.csv', index=False)

    # 将储存的数据进行预处理
    @staticmethod
    def pre_process_quote(df, rsy_sigma):

        columns_to_keep = ['symbol', 'date', 'time', 'recv_time', 'last_prc', 'volume', 'ask_prc1', 'bid_prc1',
                           'ask_vol1', 'bid_vol1', 'high_limited', 'low_limited', 'prev_close', 'open']
        columns_to_drop = [col for col in df.columns if col not in columns_to_keep]
        df.drop(columns=columns_to_drop, inplace=True)
        df.sort_values(by='time', inplace=True)

        # 检查 'time' 列是否包含关键帧值
        if 92500000 not in df['time'].values:
            new_row = pd.DataFrame({
                'symbol': np.nan,
                'date': np.nan,
                'time': [92500000],
                'recv_time': np.nan,
                'last_prc': np.nan,
                'volume': np.nan,
                'ask_prc1': np.nan,
                'bid_prc1': np.nan,
                'ask_vol1': np.nan,
                'bid_vol1': np.nan,
                'high_limited': np.nan,
                'low_limited': np.nan,
                'prev_close': np.nan,
                'open': np.nan,
            })
            # 添加93000000，并重新设置索引
            df = df.append(new_row, ignore_index=True)
            df.sort_values(by='time', inplace=True)
            df.reset_index(drop=True, inplace=True)

        if 150000000 not in df['time'].values:
            new_row = pd.DataFrame({
                'symbol': np.nan,
                'date': np.nan,
                'time': [150000000],
                'recv_time': np.nan,
                'last_prc': np.nan,
                'volume': np.nan,
                'ask_prc1': np.nan,
                'bid_prc1': np.nan,
                'ask_vol1': np.nan,
                'bid_vol1': np.nan,
                'high_limited': np.nan,
                'low_limited': np.nan,
                'prev_close': np.nan,
                'open': np.nan,
            })
            # 添加150000000，并重新设置索引
            df = df.append(new_row, ignore_index=True)
            df.sort_values(by='time', inplace=True)
            df.reset_index(drop=True, inplace=True)

        # 首先按 'time' 和 'recv_time' 排序， 然后去除 'time' 列中重复的条目，只保留每个组中 'recv_time' 最小的条目
        df.sort_values(by=['time', 'recv_time'], inplace=True)
        df.drop_duplicates(subset='time', keep='first', inplace=True)

        # 裁剪92500000-quote行情结束
        index_location1 = df[df['time'] == 92500000].index.min()
        row_number1 = df.index.get_loc(index_location1)
        df = df.loc[row_number1:].reset_index(drop=True)

        # 将时间整数转换为 datetime 对象，3s重采样
        df['time'] = df['time'].apply(Datapreprocess.convert_int_to_time)
        df_resampled = df.set_index('time').resample('3S').asfreq()
        df_resampled.reset_index(inplace=True)
        df_resampled['time'] = df_resampled['time'].apply(Datapreprocess.convert_time_to_int)
        df['time'] = df['time'].apply(Datapreprocess.convert_time_to_int)
        # 在原始的 df 中添加重采样后的数据，reindex
        df = pd.merge(df, df_resampled, how='outer')
        df = df.sort_values('time')

        # 检查recv_time 是否全天缺失, 如果全部缺失用同行time来填充
        if df['recv_time'].isnull().all():
            df['recv_time'] = df['time']
        elif df['recv_time'].isnull().any():
            df['recv_time'] = df['recv_time'].fillna(df['time'])

        # 统一symbol, date, open, pre_close, high_limited, low_limited
        columns_to_replace = ['symbol', 'date', 'open', 'prev_close', 'high_limited', 'low_limited']
        # 使用 lambda 函数替换每列的值为该列的众数
        for column in columns_to_replace:
            df[column] = (lambda col: df[col].mode()[0])(column)
        df.reset_index(drop=True, inplace=True)

        # 创建一个新的 'fill' 列，如果行中存在空值则为1，否则为0
        df['fill'] = df.apply(lambda x: 1 if x.isnull().any() else 0, axis=1)

        # 创建 "index_Resource" 列，标记所有非空行为 'self'
        df['index_resource'] = df.apply(lambda x: 'self' if x.notna().all() else np.nan, axis=1)

        # 创建 "Time" 列，标记所有非空行为 'self'
        df['Time'] = df.apply(lambda x: x['time'] if x.notna().all() else np.nan, axis=1)

        # 遍历 DataFrame 的每一行,将 "index_Resource" 填充为最近的非空行的索引
        last_non_na_index = None
        for idx, row in df.iterrows():
            if row['index_resource'] == 'self':
                last_non_na_index = idx
            else:
                df.at[idx, 'index_resource'] = last_non_na_index

        # 遍历 DataFrame 的每一行,将 "recv_time" 填充为最近的非空行的recv_time
        last_non_na_rectime = None
        for idx, row in df.iterrows():
            if pd.notna(row['recv_time']):
                last_non_na_rectime = row['recv_time']
            else:
                df.at[idx, 'recv_time'] = last_non_na_rectime

        # 遍历 DataFrame 的每一行, 如果 "Time" 是空的，则将 "Time" 填充为最近的非空行的 "time"--回溯来源
        last_non_na_time = None
        for idx, row in df.iterrows():
            if pd.notna(row['Time']):
                last_non_na_time = row['Time']
            else:
                df.at[idx, 'Time'] = last_non_na_time

        df.fillna(method='ffill', inplace=True)

        # 创建新的列 "t_volumn"，当前3s时刻的交易量
        df['t_volume'] = 0
        df.loc[index_location1, 't_volume'] = df.loc[index_location1, 'volume']
        for idx in range(index_location1 + 1, len(df)):
            df.loc[idx, 't_volume'] = df.loc[idx, 'volume'] - df.loc[idx - 1, 'volume']

        # 创建新的列 "outlier"，初始化为 0
        df['outlier'] = 0
        for idx in range(index_location1 + 1, len(df)):
            df.at[idx, 'price_change_pct'] = ((df.at[idx, 'last_prc'] - df.at[idx, 'prev_close']) / df.at[idx, 'prev_close'])
            std = rsy_sigma

            # 如果 "last_price" 列的值在 3σ 之外，则标记为 1，否则为 0
            df.loc[idx, 'outlier'] = np.where((df.loc[idx, 'price_change_pct'] < 3 * std) |
                                              (df.loc[idx, 'price_change_pct'] > 3 * std), 1, 0)

        # 从数据框的第93000000行开始
        df['middle'] = None
        df.loc[:index_location1 - 1, 'middle'] = 0
        # 逐行应用middle逻辑
        for idx in range(index_location1, len(df)):
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

        # 加权成交价，数量加权价格
        df.loc[:index_location1 - 1, 'acc_weight_price'] = 0
        cumulative_prc_volume = 0
        cumulative_volume = 0
        for idx in range(index_location1, len(df)):
            # 计算当前行的last_prc * t_volume和volume
            prc_volume = df.loc[idx, 'last_prc'] * df.loc[idx, 't_volume']
            volume = df.loc[idx, 't_volume']
            # 累计last_prc * t_volume和volume
            cumulative_prc_volume += prc_volume
            cumulative_volume += volume
            # 计算并赋值
            df.loc[idx, 'acc_weight_price'] = cumulative_prc_volume / cumulative_volume if cumulative_volume != 0 else 0
        df['acc_weight_price'] = df['acc_weight_price'].round(2)

        # 涨停判断
        df['stop'] = 0
        for idx, row in df.iterrows():
            # 检查time列的值是否大于等于92500000
            if row['time'] >= 92500000:
                # 如果满足条件，将stop列的值设为1
                if ((row['last_prc'] == row['high_limited'] and row['ask_vol1'] == 0) or
                        (row['last_prc'] == row['low_limited'] and row['bid_vol1'] == 0)):
                    df.loc[idx, 'stop'] = 1

        # 特殊处理last_prc, volumn 两列，将所有0变为null然后ffill
        df['last_prc'].replace(0, np.nan, inplace=True)
        df['volume'].replace(0, np.nan, inplace=True)
        df.fillna(method='ffill', inplace=True)
        df.reset_index(drop=True, inplace=True)

        # 这里处理index_resource 以价格为优先
        first_non_null_index = df['last_prc'].first_valid_index()
        next_non_last_prc = df.at[first_non_null_index, 'last_prc']
        df.loc[:first_non_null_index - 1, 'last_prc'] = next_non_last_prc
        df.loc[:first_non_null_index - 1, 'index_resource'] = first_non_null_index

        first_non_null_index2 = df['volume'].first_valid_index()
        next_non_volume = df.at[first_non_null_index, 'last_prc']
        df.loc[:first_non_null_index2 - 1, 'volume'] = next_non_volume

        df_result = df

        return df_result

    # 如果自由流通市值计算正确时即可启用
    # @staticmethod
    # def weight_prc(date, capital_df):
    #     folder_path = f'./quote/{date}_detail'  # 文件夹1的路径
    #
    #     # 遍历文件夹中的所有文件
    #     for filename in os.listdir(folder_path):
    #         if filename.startswith('processed_reference') and filename.endswith(
    #                 '.csv'):  # 筛选以"processed_reference"开头且以.csv结尾的文件
    #             file_path = os.path.join(folder_path, filename)  # 文件的完整路径
    #             # 读取CSV文件
    #             df1 = pd.read_csv(file_path)
    #             # 找出'symbol'列的众数
    #             mode_symbol = df1['symbol'].mode()
    #             start_index = capital_df[capital_df['S_INFO_WINDCODE'] == mode_symbol[0]].index[0]
    #             cal_weight_value = capital_df.loc[start_index, 'SHR_CALCULATION']
    #             df1['SHR_CALCULATION'] = cal_weight_value
    #             df1['last_prc'] = df1['last_prc'].apply(lambda x: Decimal(x))
    #             df1['weight_prc'] = df1['last_prc'] * df1['SHR_CALCULATION']
    #             df1['SHR_CALCULATION'] = df1['SHR_CALCULATION'].apply(
    #                 lambda x: x.quantize(decimal.Decimal('0.00000000')))
    #             # 保存修改后的DataFrame到同名文件
    #             output_path = os.path.join(folder_path, filename)
    #             df1.to_csv(output_path, index=False)
    #             print(f"Processed file: {filename}")

    # 批量处理行情数据,处理好的数据
    @staticmethod
    def pre_quote_information(date, conn):
        factor = Factorcalculation(conn)
        pre_quote_folder_path = f"./quote/{date}"
        pre_quote_save_path = f"./quote/{date}_detail"
        if not os.path.exists(pre_quote_save_path):
            os.makedirs(pre_quote_save_path)
        for pre_quote_filename in os.listdir(pre_quote_folder_path):
            if pre_quote_filename.endswith('.csv'):
                print(f"Processing file: {pre_quote_filename}")  # 打印正在处理的文件名
                pre_quote_file_path = os.path.join(pre_quote_folder_path, pre_quote_filename)
                pre_quote_df = pd.read_csv(pre_quote_file_path)
                rsy_sigma = factor.rsy_calculation(pre_quote_df, date)
                try:
                    df_reference = Datapreprocess.pre_process_quote(pre_quote_df, rsy_sigma)
                except KeyError as e:
                    print(f"Error processing file: {pre_quote_filename}, error: {e}")
                    continue  # 如果处理文件出错，跳过剩余的代码并处理下一个文件

                # 原始数据
                new_filename = 'processed_reference_' + pre_quote_filename
                new_file_path = os.path.join(pre_quote_save_path, new_filename)
                df_reference.to_csv(new_file_path, index=False)

    # 将处理好的日内行情数据3s重采样，将以resample+ 文件名储存
    @staticmethod
    def resample_3s(date):
        resample_3s_folder_path = f'./quote/{date}_detail'  # 文件夹路径
        for resample_3s_filename in os.listdir(resample_3s_folder_path):
            if resample_3s_filename.startswith('processed_') and resample_3s_filename.endswith('.csv'):
                resample_3s_file_path = os.path.join(resample_3s_folder_path, resample_3s_filename)
                resample_3s_output_filename = '3s_resampled_' + resample_3s_filename
                print(f"Processed file: {resample_3s_filename}")
                # 读取CSV文件
                df = pd.read_csv(resample_3s_file_path)
                df['time'] = df['time'].apply(Datapreprocess.convert_int_to_time)
                df_resampled = df.set_index('time').resample('3S').asfreq()
                df_resampled.reset_index(inplace=True)
                df_resampled['time'] = df_resampled['time'].apply(Datapreprocess.convert_time_to_int)
                df_resampled.reset_index(inplace=True)

                # 特殊处理92500000
                index_location1 = df_resampled.loc[df_resampled['time'] == 92500000, 'time'].index.min()
                index_location2 = df_resampled.loc[df_resampled['time'] < 93000000, 'time'].idxmax() if (
                            df_resampled['time'] < 93000000).any() else None
                self_rows = df_resampled.loc[index_location1:index_location2][
                    df_resampled.loc[index_location1:index_location2, 'index_resource']
                    == 'self']

                # 保存目标行（'92500000'）的 'time' 和 'recv_time' 原始值。
                original_time1 = df_resampled.loc[index_location1, 'time']
                original_recv_time1 = df_resampled.loc[index_location1, 'recv_time']

                # 如果存在符合条件的行，则使用第一条符合条件的行替换 '92500000' 行的数据，但除了 'time' 和 'recv_time'。
                # 如果不存在符合条件的行，则使用该区间最后一行数据替换 '92500000' 行的数据，但除了 'time' 和 'recv_time'。
                if not self_rows.empty:
                    replacement_data = self_rows.iloc[-1].drop(['time', 'recv_time'])
                    df_resampled.loc[index_location1, replacement_data.index] = replacement_data
                else:
                    replacement_data = df.loc[index_location2].drop(['time', 'recv_time'])
                    df_resampled.loc[index_location1, replacement_data.index] = replacement_data

                # 恢复 '92500000' 行的 'time' 和 'recv_time' 原始值。
                df_resampled.loc[index_location1, 'time'] = original_time1
                df_resampled.loc[index_location1, 'recv_time'] = original_recv_time1

                # 同理特殊处理150000000
                index_location3 = df_resampled.loc[df_resampled['time'] == 150000000, 'time'].index.min()
                index_location4 = df_resampled.loc[df_resampled['time'] > 150000000, 'time'].idxmax() if (
                            df_resampled['time'] > 150000000).any() else None
                self_rows = df_resampled.loc[index_location3:index_location4][
                    df_resampled.loc[index_location3:index_location4, 'index_resource']
                    == 'self']

                original_time2 = df_resampled.loc[index_location3, 'time']
                original_recv_time2 = df_resampled.loc[index_location3, 'recv_time']

                if not self_rows.empty:
                    replacement_data = self_rows.iloc[-1].drop(['time', 'recv_time'])
                    df_resampled.loc[index_location3, replacement_data.index] = replacement_data
                else:
                    replacement_data = df.loc[index_location2].drop(['time', 'recv_time'])
                    df_resampled.loc[index_location3, replacement_data.index] = replacement_data

                df_resampled.loc[index_location3, 'time'] = original_time2
                df_resampled.loc[index_location3, 'recv_time'] = original_recv_time2

                # 裁剪df

                time_ranges = [(93000000, 113000000), (130000000, 145700000)]
                time_points = [92500000, 150000000]

                # 使用布尔索引筛选出符合条件的行
                # 条件是时间在92500000，93000000-113000000，130000000-145700000，150000000之间
                condition = df_resampled['time'].isin(time_points)
                for start, end in time_ranges:
                    condition |= (df_resampled['time'] >= start) & (df_resampled['time'] <= end)
                df_resampled = df_resampled[condition]
                df_resampled.reset_index(inplace=True)

                # 保存结果 DataFrame 到新的 CSV 文件
                resample_3s_output_path = os.path.join(resample_3s_folder_path, resample_3s_output_filename)
                df_resampled.to_csv(resample_3s_output_path, index=False)
                print(f"Output file: {resample_3s_output_path}")

    # 处理3s重采样过后的数据，现在默认等权，如果调整自由流通股本正确，将last_prc 换成weight_prc 即可
    # weight_prc 通过前方注释代码def weight_prc计算
    @ staticmethod
    def weight_martrix(date):
        folder_path = f'./quote/{date}_detail'  # 文件夹路径

        # 用于存储每个文件的权重值DataFrame列表
        weights_dfs = []
        # 获取所有匹配的文件并进行排序
        filenames = [filename for filename in os.listdir(folder_path) if
                     filename.startswith('3s_resampled') and not filename.startswith('3s_resampled_processed_reference_index')
                     and filename.endswith('.csv')]
        sorted_filenames = sorted(filenames)
        # 遍历排序后的每个文件
        for filename in sorted_filenames:
            file_path = os.path.join(folder_path, filename)  # 拼接完整的文件路径
            df_temp = pd.read_csv(file_path)
            if 'last_prc' in df_temp.columns:
                # 提取 'weight_prc' 列并将其重命名为不带.csv的文件名
                df_temp = df_temp[['last_prc']].rename(
                    columns={'last_prc': filename[33:-7]})  # 假设文件名的格式正确且以 .csv 结尾
                weights_dfs.append(df_temp)
        # 使用concat横向拼接所有DataFrame
        df_weights_concatenated = pd.concat(weights_dfs, axis=1)
        # min_value = df_weights_concatenated.iloc[0]
        # normalized_data = df_weights_concatenated.divide(min_value)
        df_weights_concatenated.to_csv(f'./quote/{date}_detail/weight_matrix.csv', index=False)
        prc_sum = pd.DataFrame()
        prc_sum['prc_sum'] = df_weights_concatenated.sum(axis=1)
        prc_sum.to_csv(f'./quote/{date}_detail/components_prc_sum.csv', index=False)
        return df_weights_concatenated


# 针对Datapreprocess class 的功能demo
# list1 = ['000016.SH']
# Datapreprocess.index_save_to_csvf(list1, 20231201)
# Datapreprocess.pre_quote_information(20231201)
# Datapreprocess.resample_3s(20231201)
# Datapreprocess.weight_martrix(20231201)


class Plot:
    @staticmethod
    def code_to_name(df):

        # 使用'name'列中的值来替换'ind_code'列中的对应值，使用replace()函数将df中'WIND_IND_CODE'列中的值替换为mapping_dict中的对应值
        mapping_df = pd.read_csv('./reference/wind_industry_level_1.csv')

        # 创建一个新的 Series，统计每个SW_IND_CODE的数量
        wind_code_counts = df['industry_chinese_name'].value_counts()
        for name, count in wind_code_counts.items():
            print(f"Industry_chinese_name: {name}, Count: {count}")

        # 创建一个新的 Series，索引为所有行业名称，值初始化为 0
        all_industries = mapping_df['name'].tolist()
        industry_counts = pd.Series(0, index=all_industries)
        industry_counts.update(wind_code_counts)

        return industry_counts, all_industries

    # 传入industry df
    @staticmethod
    def single_industry_plt(df, name_result_str):
        industry_counts, all_industries = Plot.code_to_name(df)

        fig, ax = plt.subplots(figsize=(14, 8))
        percentages = [count / sum(industry_counts) * 100 for count in industry_counts]
        bars = ax.bar(range(len(industry_counts.values)), percentages, width=0.8, color='b')

        # 设置 x 轴标签
        ax.set_xticks(range(len(industry_counts.index)))
        ax.set_xticklabels(all_industries, rotation=90, fontsize=14)

        # 设置 y 轴标签为百分号
        formatter = ticker.PercentFormatter(xmax=100)
        ax.yaxis.set_major_formatter(formatter)

        # 设置y轴范围# 设置y轴范围
        max_percentage = max(industry_counts.values) / len(df) * 100
        ax.set_ylim(0, max_percentage * 1.1)

        # 调整y轴刻度标签的字体大小
        ax.set_yticks(ax.get_yticks())
        ax.set_yticklabels(ax.get_yticks(), fontsize=14)

        for i, rect in enumerate(bars):
            height = rect.get_height()
            percentage = percentages[i]
            label = f'{percentage:.2f}%' if i < len(industry_counts) else '0%'
            ax.text(rect.get_x() + rect.get_width() / 2, height, label, ha='center', va='bottom',
                    fontsize=14, rotation=0, weight='bold')

        plt.xlabel('行业', fontsize=12)
        plt.ylabel('占比', fontsize=12)
        plt.title(name_result_str + '一级行业分布情况', fontsize=12)

        # 调整图像布局
        plt.tight_layout()

        # 保存图像文件到当前目录
        filename = f'{name_result_str}_industry_distribution.png'
        filepath = os.path.join('./result/single_plt_result', filename)
        plt.savefig(filepath, dpi=800)

        # 显示图像
        plt.show()

    # 传入两个industry_df， 两个name_str
    @staticmethod
    def double_index_industry_plt(df1, df2, name_str1, name_str2):
        # 获取两个指数的行业分布
        industry_counts1, all1 = Plot.code_to_name(df1)
        industry_counts2, all2 = Plot.code_to_name(df2)

        # 计算两个指数的行业分布的百分比
        percentages1 = [count / sum(industry_counts1) * 100 for count in industry_counts1]
        percentages2 = [count / sum(industry_counts2) * 100 for count in industry_counts2]

        # 创建一个新的DataFrame，索引为全行业标签列表，列为两个指数的行业分布的百分比

        df = pd.DataFrame({
            name_str1: pd.Series(percentages1, index=all1),
            name_str2: pd.Series(percentages2, index=all2)
        })

        # 绘制并排柱状图
        ax = df.plot(kind='bar', figsize=(24, 14), width=0.9)  # 更改图表大小

        # 添加百分比标签，位置调整到柱状图顶部
        for i, (p1, p2) in enumerate(zip(percentages1, percentages2)):
            ax.text(i - 0.2, p1, f'{p1:.1f}%', va='bottom', ha='center', fontsize=20, weight='bold')
            ax.text(i + 0.2, p2, f'{p2:.1f}%', va='bottom', ha='center', fontsize=20, weight='bold')

        # 设置图表的标题、x轴标签和y轴标签
        ax.set_title(f'{name_str1}和{name_str2}的行业分布对比', fontsize=30)
        ax.set_xlabel('行业', fontsize=24)
        ax.set_ylabel('百分比', fontsize=24)

        # 设置x轴标签的旋转角度和间隔
        plt.xticks(rotation=90, ha='right')

        # 获取柱状图最高点的值
        max_value = max(max(percentages1), max(percentages2))
        ax.set_ylim(0, max_value * 1.4)

        # 改变x轴标签的字体大小
        ax.tick_params(axis='x', labelsize=28)  # 设置为14，你可以根据需要调整这个值
        ax.tick_params(axis='y', labelsize=28)

        # 显示右上角的图例
        ax.legend(fontsize='24', labels=[name_str1, name_str2])
        plt.subplots_adjust(bottom=0.25)
        plt.tight_layout()

        # 保存图像文件到当前目录
        filename = f'{name_str1}和{name_str2}_industry_distribution.png'
        filepath = os.path.join('./result/double_plt_result', filename)
        plt.savefig(filepath, dpi=800)

        # 显示图表
        plt.show()

    # 传入market_distribution_df, name_str
    @staticmethod
    def market_plt(df, name1):
        # 计算'market'列中每个值的数量
        market_counts = df['Market'].value_counts()
        fig, ax = plt.subplots()

        # 定义一个函数用于更新 autopct 参数的值
        def func(pct, allvals):
            absolute = int(pct / 100. * np.sum(allvals))
            return "{:.1f}%\n({:d})".format(pct, absolute)

        # 画一个环形图
        ax.pie(market_counts, labels=market_counts.index, autopct=lambda pct: func(pct, market_counts),
               startangle=90, pctdistance=0.85)

        # 在环形图中间画一个白色的圆形，以使其看起来像一个环形图
        centre_circle = plt.Circle((0, 0), 0.70, fc='white')
        fig = plt.gcf()
        ax.set_title(f'{name1}市场分布情况')
        fig.gca().add_artist(centre_circle)
        ax.axis('equal')
        plt.savefig(f'./result/market_plt_result/{name1}市场分布情况.png')
        plt.show()

    # 日内3s 行情画图
    @staticmethod
    def quote_3s_matplt(date, index):

        df = pd.read_csv(f'./quote/{date}_detail/components_prc_sum.csv')

        df2 = pd.read_csv(f'./quote/{date}_detail/3s_resampled_processed_reference_index_{index}.csv')

        divident = df['prc_sum'][0] / df2['last_prc'][0]
        df['prc_sum'] = df['prc_sum'] / divident

        df2_resampled1 = df2[(df2['time'] <= 113000000) & (df2['time'] >= 93000000)]
        df2_resampled2 = df2[(df2['time'] >= 130000000) & (df2['time'] <= 150000000)]

        index_93000000 = df2['time'].searchsorted(93000000)
        index_113000000 = df2['time'].searchsorted(113000000)
        index_130000000 = df2['time'].searchsorted(130000000)
        index_150000000 = df2['time'].searchsorted(150000000)

        df_resampled1 = df.iloc[index_93000000:index_113000000]
        df_resampled2 = df.iloc[index_130000000:index_150000000]
        print(df_resampled1)

        # 创建一个新的图形，并设置其大小
        plt.figure(figsize=(35, 15))

        # 创建一个带有断点的轴
        bax = brokenaxes(xlims=((index_93000000, index_113000000), (index_130000000, index_150000000)))

        # 在每个断点的部分上绘制数据
        bax.plot(df_resampled1['prc_sum'], label='成分股上午时段', color='red')
        bax.plot(df_resampled2['prc_sum'], label='成分股下午时段', color='red')
        bax.plot(df2_resampled1['last_prc'], label='指数上午时段', color='black')
        bax.plot(df2_resampled2['last_prc'], label='指数下午时段', color='black')

        # 设置 x 轴的标签
        xticks1 = np.linspace(df2_resampled1['time'].index.min(), df2_resampled1['time'].index.max(), 5)
        xticks2 = np.linspace(df2_resampled1['time'].index.min(), df2_resampled1['time'].index.max(), 5)

        bax.axs[0].set_xticks(xticks1)
        bax.axs[0].set_xticklabels([f"{x:.0f}" for x in xticks1])

        bax.axs[1].set_xticks(xticks2)
        bax.axs[1].set_xticklabels([f"{x:.0f}" for x in xticks2])

        # 调整 y 轴的范围
        y_max = max(df['prc_sum'].max(), df2['last_prc'].max())
        y_min = min(df['prc_sum'].min(), df2['last_prc'].min())

        bax.axs[0].set_ylim(y_min, y_max)  # 设置第一个断点轴的 y 轴范围
        bax.axs[1].set_ylim(y_min, y_max)  # 设置第二个断点轴的 y 轴范围

        plt.title(f"{date}捏合指数日内行情")
        font_properties = {'weight': 'bold'}
        bax.axs[1].legend(loc="upper right", fontsize=20, title_fontsize=24, prop=font_properties)
        plt.savefig(f'./result/3s_plt/{date}.png')

        plt.show()


# 针对3s行情图的demo
# Plot.quote_3s_matplt(20231201, '000016.SH')
