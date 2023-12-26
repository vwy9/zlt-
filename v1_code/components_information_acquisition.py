# -*- coding = utf-8 -*-
# @Time: 2023/12/2
# @Author: Wu You
# @File：components_information_acquisition.py
# @Desc: 说明：获得指数内成分股(A+WIND)的详情信息
# @Software: PyCharm

from datetime import datetime, timedelta
import pandas as pd
import pymysql

from v2_code import connect


# 在有指数指标代码存在时：S_INFO_WINDCODE -> 指数代码    S_CON_WINDCODE -> 成分股代码
# 在无指数指标存在时： S_INFO_WINDCODE -> 成分股代码
# 在取出时请注意


# 指定日期当天最新成分股
def components_ac(conn, index, date):

    """
        :param conn: 链接数据库connect
        :param index: 指数代码
        :param date: 日期
        :return: 指数中文名称，成分股list
    """

    # 步骤pre：查找指数对应中文名称
    query_pre = f"SELECT S_INFO_COMPNAME FROM AINDEXDESCRIPTION " \
                f"WHERE S_INFO_WINDCODE = '{index}'"
    try:
        # 建立游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(query_pre)
        # 获取查询结果
        name_result = cursor.fetchall()
        name_result_str = name_result[0][0]
        cursor.close()
    except pymysql.Error as e:
        print(f"执行查询指数对应中文名称发生错误: {e}")

    query_step = f"SELECT S_CON_WINDCODE FROM AINDEXMEMBERS " \
                 f"WHERE S_INFO_WINDCODE = '{index}' " \
                 f"AND S_CON_INDATE <= {date} " \
                 f"AND ( S_CON_OUTDATE IS NULL OR CAST(S_CON_OUTDATE AS SIGNED) >= {date} )" \
                 f"UNION " \
                 f"SELECT S_CON_WINDCODE FROM AINDEXMEMBERSWIND " \
                 f"WHERE F_INFO_WINDCODE = '{index}' " \
                 f"AND S_CON_INDATE <= {date} " \
                 f"AND ( S_CON_OUTDATE IS NULL OR CAST(S_CON_OUTDATE AS SIGNED) >= {date} )"
    try:
        # 建立游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(query_step)
        components_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        selected_data = pd.DataFrame(components_result, columns=columns)
        components_list = selected_data['S_CON_WINDCODE'].tolist()
        cursor.close()
    except pymysql.Error as e:
        print(f"执行查询成分股时发生错误: {e}")

    # print(f'{date}：{name_result_str}的成分股数量为', len(components_list))

    return name_result_str, components_list


# 获得成分股的WIND一级行业分布信息
def industry_distrubution(conn, components_list):

    """
    :param conn: 链接数据库conn
    :param components_list: 需要找到一级行业分布的list
    :return: S_INFOWINDCODE : WIND_Level1_Industry --df
    """

    # 成分股list转换为str, ', '分隔
    list_str = "', '".join(components_list)

    query_step = f"SELECT S_INFO_WINDCODE, WIND_IND_CODE " \
                 f"FROM ASHAREINDUSTRIESCLASS " \
                 f"WHERE S_INFO_WINDCODE IN ('{list_str}') " \
                 f"AND REMOVE_DT IS NULL"
    try:
        # 重新开启游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(query_step)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        industry_distribution_df = pd.DataFrame(result, columns=columns)
        industry_distribution_df['WIND_IND_CODE'] = industry_distribution_df['WIND_IND_CODE'] \
            .apply(lambda x: x[:-6] + '000000' if isinstance(x, str) and x[-6:].isdigit() else x)

        # 使用'name'列中的值来替换'ind_code'列中的对应值，
        # 使用replace()函数将df中'WIND_IND_CODE'列中的值替换为mapping_dict中的对应值
        mapping_df = pd.read_csv('../reference/wind_industry_level_1.csv')
        mapping_df['ind_code'] = mapping_df['ind_code'].astype(str)
        mapping_dict = dict(zip(mapping_df['ind_code'], mapping_df['name']))
        industry_distribution_df['industry_chinese_name'] = industry_distribution_df['WIND_IND_CODE'].replace(
                                                                                                        mapping_dict)
        mapping_dict2 = dict(zip(mapping_df['ind_code'], mapping_df['wind_code']))
        industry_distribution_df['industry_wind_index'] = industry_distribution_df['WIND_IND_CODE'].replace(
                                                                                                        mapping_dict2)
        cursor.close()
    except pymysql.Error as e:
        print(f"执行查询发生错误: {e}")

    return industry_distribution_df


# 判断板块分布功能
# 在 DataFrame 的 S_INFO_WINDCODE 列上应用函数  使用方法：df['Market_Name'] = df.apply(market_distribution, axis=1)
def market_distribution(S_INFO_WINDCODE_df):

    """
    :param S_INFO_WINDCODE_df: 成分股名单df
    :return: 在S_INFO_WINDCODE_df返回板块名称列['Market_Name']
    """

    market_name_list = ['深板创业', '沪板科创', '深主', '沪主']
    if S_INFO_WINDCODE_df['S_INFO_WINDCODE'].startswith(('300', '301')):
        return market_name_list[0]
    elif S_INFO_WINDCODE_df['S_INFO_WINDCODE'].startswith(('688', '689')):
        return market_name_list[1]
    elif S_INFO_WINDCODE_df['S_INFO_WINDCODE'].startswith(("000", "001", "002", "003", "004")):
        return market_name_list[2]
    elif S_INFO_WINDCODE_df['S_INFO_WINDCODE'].startswith(("600", "601", "603", "605")):
        return market_name_list[3]


# ST, *ST 成分股剔除 --- 返回去除ST, *ST 的成分股list
def st_remove(conn, list, date: int):

    """
    :param conn: 链接数据库conn
    :param list: 需要剔除ST, *ST 的成分股名单->list
    :param date: 日期
    :return: 去除ST, *ST 的成分股list
    """

    # 查找ST, *ST 成分股
    query_step = f"SELECT S_INFO_WINDCODE FROM ASHAREST " \
                 f"WHERE CAST(ENTRY_DT AS SIGNED) < {date} " \
                 f"AND (REMOVE_DT IS NULL OR CAST(REMOVE_DT AS SIGNED) >= {date} )"
    try:
        # 重新开启游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(query_step)
        st_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        selected_data = pd.DataFrame(st_result, columns=columns)
        st_list = selected_data['S_INFO_WINDCODE'].tolist()
        cursor.close()
    except pymysql.Error as e:
        print(f"执行查询st名单时发生错误: {e}")

    # 遍历计算去除ST, *ST 的成分股list
    com_remove_st = []
    for stock in list:
        if stock not in st_list:
            com_remove_st.append(stock)

    # print(f'去除ST, *ST的成分股数量为', len(com_remove_st))

    return com_remove_st


# 去除不满上市不满n天的成分股
def remove_nomore_n_days(conn, list, n_days: int, date: int):

    """
    :param conn: 链接数据库conn
    :param n_days: 定义不满n天
    :param date: 日期
    :return: 去除上市不满n天后的成分股list
    """

    # 将list转换为str，用', '分割
    remove_list_str = "', '".join(list)

    # 计算n天前的日期
    given_date_str = str(date)
    given_date = datetime.strptime(given_date_str, "%Y%m%d").date()
    delta = timedelta(days=n_days)
    past_date_int = int((given_date - delta).strftime("%Y%m%d"))
    print(past_date_int)

    # 在com_remove_st 中筛选上市日期大于90天的股票
    query_step = f"SELECT S_CON_WINDCODE FROM AINDEXMEMBERS " \
                 f"WHERE S_CON_WINDCODE IN ('{remove_list_str}' )" \
                 f"AND CAST(S_CON_INDATE AS SIGNED) <= {past_date_int} " \
                 f"UNION " \
                 f"SELECT S_CON_WINDCODE FROM AINDEXMEMBERSWIND " \
                 f"WHERE S_CON_WINDCODE IN ('{remove_list_str}') " \
                 f"AND CAST(S_CON_OUTDATE AS SIGNED) <= {past_date_int} "

    try:
        # 重新开启游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(query_step)
        rm_ndays_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        rm90_data = pd.DataFrame(rm_ndays_result, columns=columns)
        # 以list储存去处st，*st，上市不足90天的成分股
        rm90_list = rm90_data['S_CON_WINDCODE'].tolist()
        cursor.close()

    except pymysql.Error as e:
        print(f"执行查询距今为止上市时间超过{n_days}的有效成分股发生错误: {e}")

    # print(f'去除上市不满{n_days}天的成分股数量为', len(rm90_list))

    return rm90_list


# 去除指定成分股
def remove_add_elemets(list1):

    """
    :param lilist1st: 原始成分股名单list
    :return: 删去指定股票的成分股list
    """

    delete_input = input("请输入你想从列表中删除的字符串，多个字符串请用逗号分隔：")
    delete_input_list = delete_input.split(',')
    filtered_list = [item for item in list1 if item not in delete_input_list]

    add_input = input("请输入你想从列表中加入的字符串，多个字符串请用逗号分隔：")
    if add_input:
        add_input_list = add_input.split(',')
        added_list = filtered_list + add_input_list
        result_list = list(set(added_list))
    else:
        result_list = filtered_list

    return result_list


def sh50_components_freeshare(conn, date):

    """
    :param conn:
    :param date:
    :return:
    """

    query1 = f"SELECT S_CON_WINDCODE, SHR_CALCULATION, CLOSEVALUE, WEIGHT FROM AINDEXSSE50WEIGHT " \
             f"WHERE CAST(TRADE_DT AS SIGNED) = {date} "

    try:
        # 重新开启游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(query1)
        sh50_components_weight = cursor.fetchall()
        sh50_weight_data = pd.DataFrame(sh50_components_weight,
                                        columns=['S_INFO_WINDCODE', 'SHR_CALCULATION', 'CLOSEVALUE', 'WEIGHT'])
        cursor.close()
    except pymysql.Error as e:
        print(f"执行查询{date}上证50成分股的权重发生错误: {e}")

    return sh50_weight_data


# date = int(20231130)
# conn = connect.connect_to_database(connect.wind)
# df = sh50_components_freeshare(conn, date)
# df.to_csv('./20231130.csv')
if __name__ == "__main__":
    conn = connect.connect_to_database(connect.wind)
    index = '000016.SH'
    date = int(20230921)
    name_result_str, components_list = components_ac(conn, index, date)
    industry_distrubution_df = industry_distrubution(conn, components_list)
    print(industry_distrubution_df)
    industry_distrubution_df['Market_Name'] = industry_distrubution_df.apply(market_distribution, axis=1)
    print(industry_distrubution_df)
    rm90_list = remove_nomore_n_days(conn, components_list, 90, date)
    print(rm90_list)
    final_list = remove_add_elemets(components_list)
    print(len(final_list))

    sh_50_df = sh50_components_freeshare(conn, date)
    print(sh_50_df)



# df = pd.DataFrame(my_list)
# 在list转换为df时，首先会看最外层[] 假设叫n层，n-1层[]的数量即为行数，n-2层[]的数量即为列数，同时n-2 纬度下每个元素，即为该行的元素
# df = pd.DataFrame(my_list, columns=['Column1', 'Column2', 'Column3'])
# 名称将会默认0，1，2，3，4 ……
# df = pd.DataFrame(my_list, columns=['Column1', 'Column2', 'Column3'])
# 行名称可以设置为index，所以如下命名方式
# df.index = ['Row1', 'Row2', 'Row3']







