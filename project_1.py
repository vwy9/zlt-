import pandas as pd
import ray

from v2_code.utilities_class import Plot, Tradedates
from v2_code.components_class import Componentsacquisition
from v2_code.connect import connect_to_database, wind


# 多日期并行执行project1
@ray.remote
def process1(date):
    conn = connect_to_database(wind)
    components = Componentsacquisition(conn, '000016.SH', date)
    name_str, components_df = components.components_ac()
    industry_dis_df = components.industry_distribution()
    wind_code_counts = industry_dis_df['industry_wind_index'].value_counts()
    count_df = pd.DataFrame(wind_code_counts).reset_index()
    count_df.columns = ['S_INFO_WINDCODE', 'time']
    count_df['percentage'] = count_df['time'] / wind_code_counts.sum()
    Plot.single_industry_plt(industry_dis_df, name_str)
    c_industry_ror_df = components.components_single_date_industry_ror()
    pd.set_option('display.max_columns', None)
    result_df = c_industry_ror_df.merge(count_df, on='S_INFO_WINDCODE')
    # 可选择保存df
    result_df.to_csv('./p1_result.csv')
    print(result_df)
    conn.close()

    return None


if __name__ == "__main__":

    conn = connect_to_database(wind)
    tradedays = Tradedates(conn)
    trade_dates = tradedays.calculate_trade_days(20230921, 20230930)['TRADE_DAYS'].tolist()
    conn.close()

    ray.init()

    futures = [process1.remote(date) for date in trade_dates]
    result = ray.get(futures)

    ray.shutdown()


