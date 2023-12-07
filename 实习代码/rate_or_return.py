import pandas as pd
import pymysql


from components_information_acquisition import sh50_components_freeshare
from nearest_trading_day import find_nearest_trading_day


# 判断日期是否为交易日
def check_trade_day(user_date: int, conn):

    """
    :param user_date:
    :param conn:
    :return:
    """

    # 执行 SQL 查询
    query = f"SELECT TRADE_DAYS FROM ASHARECALENDAR WHERE TRADE_DAYS={user_date}"
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        df1 = pd.DataFrame(data, columns=columns)
        cursor.close()
    except pymysql.Error as e:
        print(f"执行查询交易日发生错误: {e}")

    return not df1.empty  # 如果 DataFrame 是空的，则说明输入的日期不是交易日


# 单日收益率计算，如果需要区间内交易日期，可以
# for trade in trade_days : data_list.append({'日期': input_date, '当天收益率': dangri, '日内收益率': rinei, '隔夜收益率': geye})
# 必要时转换pd.dataframe(list)
def sh50_components_single_date_ror(conn, date):

    """
    :param conn:
    :param date:
    :return:
    """

    capital_today = sh50_components_freeshare(conn, date)
    efficient_last_trading_date = find_nearest_trading_day(date-1, conn)
    capital_yesterday = sh50_components_freeshare(conn, efficient_last_trading_date)

    set_today = set(capital_today['S_INFO_WINDCODE'])
    set_yesterday = set(capital_yesterday['S_INFO_WINDCODE'])
    codes_str = "', '".join(list(set_today))

    # 查询复权价格
    step1 = f"SELECT S_INFO_WINDCODE, S_DQ_PRECLOSE, S_DQ_OPEN, S_DQ_CLOSE " \
            f"FROM ASHAREEODPRICES " \
            f"WHERE S_INFO_WINDCODE IN ('{codes_str}') AND CAST(TRADE_DT AS SIGNED) = {date}"

    # 执行查询
    try:
        cursor = conn.cursor()
        cursor.execute(step1)
        step1_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df1 = pd.DataFrame(step1_result, columns=columns)
        cursor.close()
    except pymysql.Error as e:
        print(f"执行查询成分股量价时发生错误: {e}")

    step2 = f"SELECT S_INFO_WINDCODE, BONUS_SHARE_RATIO, RIGHTSISSUE_PRICE, CONVERSED_RATION, SEO_RATIO " \
            f"FROM ASHAREEODPRICES " \
            f"WHERE S_INFO_WINDCODE IN ('{codes_str}') AND CAST(TRADE_DT AS SIGNED) = {date}"

    if set_today == set_yesterday:
        merged_df1 = df1.merge(capital_today, on='S_INFO_WINDCODE')
        merged_df2 = df1.merge(capital_yesterday, on='S_INFO_WINDCODE')

        a = (merged_df2['S_DQ_PRECLOSE'] * merged_df2['SHR_CALCULATION']).sum()
        b = (merged_df1['S_DQ_OPEN'] * merged_df1['SHR_CALCULATION']).sum()
        c = (merged_df1['S_DQ_CLOSE'] * merged_df1['SHR_CALCULATION']).sum()

        overnight = b / a - 1
        inday = c / b - 1
        day = c / a - 1

    if set_today == set_yesterday:


        return overnight, inday, day


# 成分股对应行业的收益率
def components_single_date_industry_ror(conn, date, df):

    unique_values = df['industry_wind_index'].unique()
    unique_values = unique_values.astype(str)
    result_str = ','.join(unique_values)

    query_step = f"SELECT S_INFO_WINDCODE, S_DQ_PRECLOSE, S_DQ_CLOSE, S_DQ_OPEN " \
                 f"FROM AINDEXWINDINDUSTRIESEOD " \
                 f"WHERE S_INFO_WINDCODE IN ('{result_str}') " \
                 f"AND CAST(TRADE_DT AS SIGNED) = {date}"
    try:
        # 重新开启游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(query_step)
        query_step_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        industry_ror_df = pd.DataFrame(query_step_result, columns=columns)
        # 计算收益率
        industry_ror_df['overnight'] = industry_ror_df['S_DQ_OPEN'] / industry_ror_df['S_DQ_PRECLOSE'] - 1
        industry_ror_df['inday'] = industry_ror_df['S_DQ_CLOSE'] / industry_ror_df['S_DQ_OPEN'] - 1
        industry_ror_df['day'] = industry_ror_df['S_DQ_CLOSE'] / industry_ror_df['S_DQ_PRECLOSE'] - 1
        conn.close()

    except pymysql.Error as e:
        print(f"执行查询发生错误: {e}")

        return industry_ror_df


def index_single_date_ror(conn, index, date):

    query_step = f"SELECT S_INFO_WINDCODE, S_DQ_PRECLOSE, S_DQ_CLOSE, S_DQ_OPEN " \
                 f"FROM AINDEXEODPRICES " \
                 f"WHERE S_INFO_WINDCODE = '{index}' " \
                 f"AND CAST(TRADE_DT AS SIGNED) = {date}"
    try:
        # 重新开启游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(query_step)
        index_ror_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        index_ror_df = pd.DataFrame(index_ror_result, columns=columns)

        a = index_ror_df['S_DQ_PRECLOSE'].sum()
        b = index_ror_df['S_DQ_OPEN'].sum()
        c = index_ror_df['S_DQ_CLOSE'].sum()

        overnight = b / a - 1
        inday = c / b - 1
        day = c / a - 1
        cursor.close()

    except pymysql.Error as e:
        print(f"执行查询发生错误: {e}")

    return overnight, inday, day




# 如果wind数据库更新维护成功可以执行如下代码来更新自由流通股本
# def components_capital_information(conn, list, date):
#
#     """
#     :param conn:
#     :param list:
#     :param date:
#     :return:
#     """
#
#     codes_str = "', '".join(list)
#
#     # 在成分股名单中筛选出原始FREE_SHARES_TODAY
#     step = f"SELECT S_INFO_WINDCODE, FREE_SHARES_TODAY, TOT_SHR_TODAY FROM ASHAREEODDERIVATIVEINDICATOR " \
#            f"WHERE S_INFO_WINDCODE IN ('{codes_str}') AND CAST(TRADE_DT AS SIGNED) = {date-1}"
#     # 执行查询
#     try:
#         cursor = conn.cursor()
#         cursor.execute(step)
#         step_result = cursor.fetchall()
#         columns = [desc[0] for desc in cursor.description]
#         selected_data = pd.DataFrame(step_result, columns=columns)
#         cursor.close()
#     except pymysql.Error as e:
#         print(f"执行查询原始FREE_SHARES_TODAY时发生错误: {e}")
#
#     print(selected_data)
#
#     # 查询因除权更改自由流通成分股记录
#     step_2 = f"SELECT S_INFO_WINDCODE, S_SHARE_FREESHARES FROM ASHAREFREEFLOAT " \
#              f"WHERE S_INFO_WINDCODE IN ('{codes_str}') AND CAST(CHANGE_DT AS SIGNED) = {date}"
#     # 执行查询
#     try:
#         cursor = conn.cursor()
#         cursor.execute(step_2)
#         step_result2 = cursor.fetchall()
#         columns2 = [desc[0] for desc in cursor.description]
#         selected_data2 = pd.DataFrame(step_result2, columns=columns2)
#         if selected_data2.empty:
#             print("查询因除权更改自由流通成分股记录的结果为空，没有找到任何符合条件的数据。")
#         cursor.close()
#     except pymysql.Error as e:
#         print(f"执行查询查询因除权更改自由流通成分股记录时发生错误: {e}")
#
#     if not selected_data2.empty:
#         # 创建一个新的DataFrame，它的索引是df2中的'infor'列
#         # 创建一个映射字典，其中键是'infor'列的值，值是'freeshare'列的值
#         mapping = selected_data2.set_index('S_INFO_WINDCODE')['S_SHARE_FREESHARES'].to_dict()
#
#         # 使用映射字典来更新df1中的'free'列
#         selected_data['FREE_SHARES_TODAY'] = selected_data['S_INFO_WINDCODE'].map(mapping).fillna(selected_data['FREE_SHARES_TODAY'])
#
#     return selected_data


# 同上如果wind表格更新成功可以采用如下方法
# def change(conn, date, list):
#
#     codes_str = ', '.join(list)
#
#     step_2 = f"SELECT S_INFO_WINDCODE, S_SHARE_FREESHARES FROM ASHAREFREEFLOAT " \
#              f"WHERE S_INFO_WINDCODE IN ('{codes_str}') AND CAST(CHANGE_DT AS SIGNED) = {date}"
#     # 执行查询
#     try:
#         cursor = conn.cursor()
#         cursor.execute(step_2)
#         step_result = cursor.fetchall()
#         columns = [desc[0] for desc in cursor.description]
#         selected_data = pd.DataFrame(step_result, columns=columns)
#         if selected_data.empty:
#             print("查询因除权更改自由流通成分股记录的结果为空，没有找到任何符合条件的数据。")
#         cursor.close()
#     except pymysql.Error as e:
#         print(f"执行查询查询因除权更改自由流通成分股记录时发生错误: {e}")
#
#     return selected_data






