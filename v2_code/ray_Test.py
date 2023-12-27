import ray
import pandas as pd


from components_class import Componentsacquisition
from connect_class import DatabaseConnector
from utilities_class import Tradedates
from connect import connect_to_database, wind


@ray.remote
def process_date(date):
    try:
        conn = connect_to_database(wind)
        components = Componentsacquisition(conn, '000016.SH', date)
        name_str, components_df = components.components_ac()
        components_eodprice, c_day, c_inday, c_overnight = components.components_single_date_ew_ror()
        # print(name_str, components_df, components_eodprice, c_day, c_inday, c_overnight)
        return c_inday, c_overnight
    finally:
        conn.close()


if __name__ == "__main__":
    # trade_dates = [20230921, 20230922]
    conn = connect_to_database(wind)
    trade = Tradedates(conn)
    trade_dates = trade.calculate_trade_days(20230921, 20230925)
    trade_dates_list = trade_dates['TRADE_DAYS'].tolist()
    conn.close()
    print(trade_dates_list)
    ray.init()
    futures = [process_date.remote(date) for date in trade_dates_list]

    # 收集结果
    results = ray.get(futures)
    print(results)

    # 关闭Ray
    ray.shutdown()