# -*- coding = utf-8 -*-
# @Time: 2023/11/12 15:30
# @Author: Wu You
# @File：000985_wind.py
# @Desc: 说明：中证全指的成分股是否是由：
# 市值大小的上证50  000016.SH、沪深300  399300.SZ、中证500  000905.SH、中证1000   000852.SH、国证2000   399303.SZ组成，
# 并且统计比国证2000市值更小的部分的市值中位数和均值，如有遗漏或者差异记录一下
# @Software: PyCharm

import pandas as pd
import pymysql
from v2_code.connect import connect_to_database, wind


csi_constituents = list(pd.read_csv('../reference/old/中证全有效成分股.csv')['S_CON_WINDCODE'])
print(len(csi_constituents))
combine_constituents = list(pd.read_csv('../reference/components_miss_diff/combine_index.csv'))
print(len(combine_constituents))
zhongzheng_constituents = list(pd.read_csv('../reference/components_miss_diff/zhongzheng2000.csv')['Stock'])
print(len(zhongzheng_constituents))


# csi 和 combine 相交的部分
def inter():
    intersection_constituents = []
    name = 'inter'
    for stock in csi_constituents:
        if stock in combine_constituents:
            intersection_constituents.append(stock)
    print(intersection_constituents)
    print(len(intersection_constituents))

    codes_inter = "', '".join(intersection_constituents)

    return codes_inter, name


def differ():
    differ_constituents = []
    name = 'differ'
    for stock in csi_constituents:
        if stock not in combine_constituents:
            differ_constituents.append(stock)
    print(differ_constituents)
    print(len(differ_constituents))

    codes_differ = "', '".join(differ_constituents)

    return codes_differ, name


def mv_search(conn, code_str, name, today):

    query = f"SELECT S_INFO_WINDCODE, S_VAL_MV FROM ASHAREEODDERIVATIVEINDICATOR " \
            f"WHERE S_INFO_WINDCODE IN ('{code_str}') " \
            f"AND CAST(TRADE_DT AS SIGNED) = {today} "
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        # 获取查询结果
        query_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        selected_data = pd.DataFrame(query_result, columns=columns)

        selected_data = selected_data.sort_values('S_VAL_MV', ascending=False)

        print(selected_data)

        selected_data.to_csv(f'./combine_{name}_mv.csv', index=False)

        cursor.close()

    except pymysql.Error as e:
        print(f"执行查询发生错误: {e}")

    return selected_data


def zhongzheng_mv(conn, today):
    code_str_zz = "', '".join(zhongzheng_constituents)
    print(code_str_zz)
    query1 = f"SELECT S_VAL_MV FROM ASHAREEODDERIVATIVEINDICATOR " \
             f"WHERE S_INFO_WINDCODE IN ('{code_str_zz}') " \
             f"AND CAST(TRADE_DT AS SIGNED) = {today} ORDER BY S_VAL_MV ASC LIMIT 1;"
    try:
        cursor = conn.cursor()
        cursor.execute(query1)
        # 获取查询结果
        query1_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        selected_data1 = pd.DataFrame(query1_result, columns=columns)

        selected_data1 = int(selected_data1['S_VAL_MV'].sum())
        print("国证2000中最小市值为：", selected_data1)

    except pymysql.Error as e:
        print(f"执行查询发生错误: {e}")

    cursor.close()
    return selected_data1


def selected_mv(conn, today):
    a = zhongzheng_mv(conn, today)
    csi_code_str = "', '".join(csi_constituents)
    query = f"SELECT S_VAL_MV FROM ASHAREEODDERIVATIVEINDICATOR " \
            f"WHERE S_INFO_WINDCODE IN ('{csi_code_str}') " \
            f"AND CAST(TRADE_DT AS SIGNED) = {today} " \
            f"AND S_VAL_MV < {a} "

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        # 获取查询结果
        query_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        selected_data = pd.DataFrame(query_result, columns=columns)
        select_mean = selected_data['S_VAL_MV'].mean()
        select_median = selected_data['S_VAL_MV'].median()
        print("符合条件的成分股有：", len(selected_data))
        print("分别是：", selected_data)
        print("市值中位数：", select_median)
        print("市值平均值：", select_mean)

    except pymysql.Error as e:
        print(f"执行查询发生错误: {e}")

    return select_mean, select_median


if __name__ == "__main__":
    conn = connect_to_database(wind)

    inter, inter_str = inter()
    differ, differ_str = differ()

    # current_date = datetime.date.today()
    # formatted_date = current_date.strftime("%Y%m%d")
    # print(formatted_date)
    # today = int(formatted_date)
    # print(today)

    today = int(20231116)
    # print("交集部分的组成和市值分别为：")
    mv_search(conn, inter, inter_str, today)
    # print("差异部分的组成和市值分别为：")
    mv_search(conn, differ, differ_str, today)

    # zhongzheng_mv(conn, today)
    # selected_mv(conn, today)

    conn.close()







    # conn.close()
