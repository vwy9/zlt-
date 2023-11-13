import pandas as pd
import pymysql

from connect import connect_to_database, wind


def find_nearest_trading_day(target_date, conn):
    # 将目标日期转换为整数类型
    target_date = int(target_date)

    # 创建一个游标对象 cursor
    cursor = conn.cursor()

    # 查询目标日期之前的最近的交易日
    query = f"SELECT MAX(CAST(TRADE_DAYS AS SIGNED)) FROM ASHARECALENDAR WHERE CAST(TRADE_DAYS AS SIGNED)<={target_date}"
    cursor.execute(query)
    nearest_trading_day = cursor.fetchone()[0]

    # 关闭游标
    cursor.close()

    if nearest_trading_day is not None:

        if nearest_trading_day == target_date:
            print('当前日期为交易日：', target_date)
            return target_date
        else:
            print('当前日期为非交易日，返回最近交易日日期：', nearest_trading_day)
            return nearest_trading_day

    else:
        return None


if __name__ == "__main__":
    # 用户输入日期
    input_date = int(input("请输入日期："))

    conn = connect_to_database(wind)

    # 找到最近的交易日
    nearest_trading_day = find_nearest_trading_day(input_date, conn)

    # 关闭数据库连接
    conn.close()