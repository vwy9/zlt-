# -*- coding = utf-8 -*-
# @Time: 2023/11/2 15:20
# @Author: Wu You
# @File：index_ROR.py
# @Desc: 说明：计算等权的指数日行情的涨跌，当天涨跌、隔夜涨跌、日内涨跌
# @Software: PyCharm


# 股票的复权价格是为了消除除权除息因素对股价带来的影响，使得投资者可以更客观地分析股票的价格走势。通过复权价格，投资者可以更准确地观察股票的实际涨跌情况，更好地进行技术分析和基本面分析。
# 常见的复权价格类型包括：
# 前复权价格（前向复权价格）：以股票除权日为基准，将除权日之前的价格按照除权因子进行调整，使得除权日之前的价格与除权日当天的价格相一致。
# 后复权价格（后向复权价格）：以股票除权日为基准，将除权日之后的价格按照除权因子进行调整，使得除权日之后的价格与除权日当天的价格相一致。


import pandas as pd
import pymysql
import datetime

from connect import connect_to_database, wind


# 判断日期是否为交易日
def check_trade_day(user_date: int, conn):
    """
    检查用户输入的日期是否为交易日
    参数:
        - user_date: 用户输入的日期（int 类型）
        - conn: 数据库连接
    返回:
        - 如果输入的日期是交易日，则返回 True
        - 如果输入的日期不是交易日，则返回 False
    """
    # 创建一个游标对象 cursor
    cursor = conn.cursor()

    # 执行 SQL 查询
    query = f"SELECT TRADE_DAYS FROM ASHARECALENDAR WHERE TRADE_DAYS={user_date}"
    cursor.execute(query)

    # 获取所有记录，并将它们转换为 pandas DataFrame
    data = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    df1 = pd.DataFrame(data, columns=columns)

    # 关闭游标和连接
    cursor.close()

    return not df1.empty  # 如果 DataFrame 是空的，则说明输入的日期不是交易日


def select_data_by_date_range(start_date: int, end_date: int, index_type):
    """
    根据输入的日期范围从数据库中选取满足条件的数据
    参数:
        - start_date: 输入的开始日期（int 类型）
        - end_date: 输入的结束日期（int 类型）
    """

    # 计算日期范围
    date_range = pd.date_range(start=datetime.datetime.strptime(str(start_date), '%Y%m%d'),
                               end=datetime.datetime.strptime(str(end_date), '%Y%m%d')).strftime('%Y%m%d').tolist()

    # 初始化df
    data_list = []  # 初始化一个空列表来存储每一个日期的数据
    for input_date in date_range:
        if not check_trade_day(input_date, conn):
            print(f"{input_date} 为非交易日")
            continue
        else:
            try:
                input_date = int(input_date, round(0))
                # 执行之前的查询操作
                dangri, rinei, geye = select_data_by_date_index(input_date, index_type)
                # Append the new data to the DataFrame
                # 将新的数据添加到 data_list 中
                data_list.append({'日期': input_date, '当天收益率': dangri, '日内收益率': rinei, '隔夜收益率': geye})
            except pymysql.Error as e:
                print(f"执行查询发生错误: {e}")
                continue

        # 使用 pandas.concat 创建 DataFrame
    df = pd.concat([pd.DataFrame([i]) for i in data_list], ignore_index=True)

    df.to_csv('./result/日期内收益率.csv', index=False)


def select_data_by_date_index(input_date: int, index_type):
    """
    根据输入的日期从数据库中选取满足条件的数据
    参数:
        - input_date: 输入的日期（int 类型）
    """
    conn = connect_to_database(wind)
    global selected_data
    '''步骤一：首先让用户输入一个日期（int 类型），在表格INDEXMEMBERS中选出对应的S_CON_WINDCODE 首先S_CON_INDATE需要< 日期
    并且 S_CON_OUTDATA （用代码将其转换为int）为 NULL 或者 S_CON_OUTDATA > 输入的日期
    '''

    step_1 = f"SELECT S_CON_WINDCODE " \
             f"FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '{index_type}' " \
             f"AND CAST(S_CON_INDATE AS SIGNED) < {input_date} " \
             f"AND (CAST(S_CON_OUTDATE AS SIGNED) > {input_date} OR S_CON_OUTDATE IS NULL)" \
             f"UNION " \
             f"SELECT S_CON_WINDCODE " \
             f"FROM AINDEXMEMBERSWIND WHERE F_INFO_WINDCODE = '{index_type}'" \
             f"AND CAST(S_CON_INDATE AS SIGNED) <= {input_date} " \
             f"AND (CAST(S_CON_OUTDATE AS SIGNED) >= {input_date} OR S_CON_OUTDATE IS NULL)"
    try:
        # 执行查询
        cursor = conn.cursor()
        cursor.execute(step_1)

        # 获取查询结果
        step1_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        selected_data = pd.DataFrame(step1_result, columns=columns)

        # 输出结果
        # print("满足条件的数据：")
        # print(selected_data)
    except pymysql.Error as e:
        print(f"执行查询发生错误: {e}")

    '''步骤二：在ASHAREEODPRICES表格中搜索与步骤一中selected_data['S_CON_WINDCODE'] 
    与之对应的的S_INFO_WINDCODE 对应的S_DQ_ADJPRECLOSE，S_DQ_ADJOPEN，S_DQ_ADJCLOSE， 
    并将每个绘制成单独的list 用名字区分，且TRADE_DT（转换为int）= 步骤一中的date
    '''
    codes = selected_data['S_CON_WINDCODE'].tolist()
    codes_str = "', '".join(codes)

    step_2 = f"SELECT S_DQ_ADJPRECLOSE, S_DQ_ADJOPEN, S_DQ_ADJCLOSE FROM ASHAREEODPRICES " \
             f"WHERE S_INFO_WINDCODE IN ('{codes_str}') AND CAST(TRADE_DT AS SIGNED) = {input_date}"

    # 执行查询
    try:
        cursor = conn.cursor()
        cursor.execute(step_2)
        step2_result = cursor.fetchall()

        # 将查询结果转换为数据框
        df = pd.DataFrame(step2_result, columns=['S_DQ_ADJPRECLOSE', 'S_DQ_ADJOPEN', 'S_DQ_ADJCLOSE'])

        # 创建A、B和C三个列表
        pre_close = df['S_DQ_ADJPRECLOSE'].tolist()
        t_open = df['S_DQ_ADJOPEN'].tolist()
        t_close = df['S_DQ_ADJCLOSE'].tolist()

        '''
        显示A、B和C列表的元素数量
        print("pre_close列表元素数量:", len(pre_close))
        print("t_open列表元素数量:", len(t_open))
        print("t_close列表元素数量:", len(t_close))
        '''

        # 当天该指数内发生交易的股票数量
        d_trans = len(pre_close)

        dangri_avg = sum((t_close[i] / pre_close[i] - 1) for i in range(d_trans)) / d_trans
        geye_avg = sum((t_open[i] / pre_close[i] - 1) for i in range(d_trans)) / d_trans
        rinei_avg = sum((t_close[i] / t_open[i] - 1) for i in range(d_trans)) / d_trans

        print("当天涨跌平均值为：", round(dangri_avg, 5))
        print("日内涨跌平均值为：", round(rinei_avg, 5))
        print("隔夜涨跌平均值为：", round(geye_avg, 5))

    except pymysql.Error as e:
        print(f"执行查询时发生错误: {e}")

    # 关闭数据库连接
    conn.close()

    return dangri_avg, rinei_avg, geye_avg


if __name__ == "__main__":
    # 连接到数据库
    conn = connect_to_database(wind)
    # 执行步骤1
    start_date = int(input("请输入查询的开始日期: "))
    end_date = int(input("请输入查询的结束日期: "))
    index_type = input("请输入要查询的指数： ")
    select_data_by_date_range(start_date, end_date, index_type)

    conn.close()