import pandas as pd
import ray
from matplotlib import pyplot as plt
import numpy as np
import seaborn as sns
from functools import reduce

from v2_code.utilities_class import Tradedates
from v2_code.components_class import Componentsacquisition
from v2_code.connect import connect_to_database, wind


@ray.remote
def range_ror(date):
    conn = connect_to_database(wind)
    components = Componentsacquisition(conn, '000016.SH', date)
    name_str, components_df = components.components_ac()
    components_eodprice, c_day, c_inday, c_overnight = components.components_single_date_ew_ror()
    result.append({'日期': date, '当天收益率': c_day, '日内收益率': c_inday, '隔夜收益率': c_overnight})
    conn.close()
    return result


@ray.remote
def heatmap(date):
    conn = connect_to_database(wind)
    components = Componentsacquisition(conn, '000016.SH', date)
    name_str, components_df = components.components_ac()
    components_eodprice = components.components_single_date_industry_ror()
    selected_df = components_eodprice[['inday', 'S_INFO_WINDCODE']]
    selected_df.columns = [f'{date}_inday', 'Industry_Index']
    selected_df.sort_values(by='Industry_Index', inplace=True)
    conn.close()
    return selected_df


if __name__ == "__main__":

    # 单个日期收益率
    conn = connect_to_database(wind)
    components = Componentsacquisition(conn, '000985.CSI', 20230921)
    name_str, components_df = components.components_ac()
    components_eodprice, c_day, c_inday, c_overnight = components.components_single_date_ew_ror()
    print('等权情况下：')
    print('day:', c_day)
    print('inday:', c_inday)
    print('overnight:', c_overnight)
    conn.close()

    # 区间内收益率
    result = []
    conn = connect_to_database(wind)
    tradedays = Tradedates(conn)
    trade_dates = tradedays.calculate_trade_days(20230921, 20230924)['TRADE_DAYS'].tolist()
    ray.init()
    futures = [range_ror.remote(date) for date in trade_dates]
    results = ray.get(futures)
    flat_data = [item for sublist in results for item in sublist]
    df = pd.DataFrame(flat_data)
    # 查看DataFrame
    print(df)
    ray.shutdown()
    conn.close()


    # 区间内热力图
    final_df = pd.DataFrame()
    conn = connect_to_database(wind)
    tradedays = Tradedates(conn)
    trade_dates = tradedays.calculate_trade_days(20230921, 20230930)['TRADE_DAYS'].tolist()
    ray.init()
    futures = [heatmap.remote(date) for date in trade_dates]
    results = ray.get(futures)
    df_combined = reduce(lambda left, right: pd.merge(left, right, on='Industry_Index', how='inner'), results)
    pd.set_option('display.max_columns', None)
    dictionary = {k: v.values.tolist() for k, v in df_combined.set_index('Industry_Index').iterrows()}
    df1 = pd.DataFrame(dictionary)
    df_float = df1.astype(float)
    correlation_matrix = df_float.corr()
    print(correlation_matrix)

    # 使用Seaborn绘制热图
    plt.figure(figsize=(8, 6))
    sns.heatmap(correlation_matrix, annot=True, fmt=".2f", cmap='coolwarm', square=True)

    # 显示图表
    plt.show()

    ray.shutdown()
    conn.close()





