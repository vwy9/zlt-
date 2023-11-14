# -*- coding = utf-8 -*-
# @Time: 2023/11/13 9:15
# @Author: Wu You
# @File：nearest_trading_day.py
# @Desc: 说明：判断并查找最近交易日
#        数据库 - Wind金融数据库
# @Software: PyCharm

from datetime import datetime, timedelta

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
            # print('当前日期为交易日：', target_date)
            return target_date
        else:
            # print('当前日期为非交易日，返回最近交易日日期：', nearest_trading_day)
            return nearest_trading_day

    else:
        return None


def calculate_trade_days(start_date, end_date, conn):
    # 构建SQL查询语句
    query = f"SELECT DISTINCT TRADE_DAYS FROM ASHARECALENDAR " \
            f"WHERE CAST(TRADE_DAYS AS SIGNED) BETWEEN '{start_date}' AND '{end_date}'"

    # 执行SQL查询
    cursor = conn.cursor()
    cursor.execute(query)

    results = cursor.fetchall()

    # 提取交易日期
    trade_dates = [result[0] for result in results]

    cursor.close()

    # 返回交易日数量
    return trade_dates


if __name__ == "__main__":
    # 用户输入日期
    start_date = int(input("请输入开始日期:"))
    end_date = int(input("请输入结束日期:"))

    conn = connect_to_database(wind)

    # 找到最近的交易日
    # nearest_trading_day = find_nearest_trading_day(start_date, conn)
    # print(nearest_trading_day)

    a = calculate_trade_days(start_date, end_date, conn)
    print(a)
    print(len(a))

    # 关闭数据库连接
    conn.close()