# -*- coding = utf-8 -*-
# @Time: 2023/11/2 15:20
# @Author: Wu You
# @File：project_2.py
# @Desc: 说明：计算等权的指数日行情的涨跌，当天涨跌、隔夜涨跌、日内涨跌
# @Software: PyCharm


# 股票的复权价格是为了消除除权除息因素对股价带来的影响，使得投资者可以更客观地分析股票的价格走势。通过复权价格，投资者可以更准确地观察股票的实际涨跌情况，更好地进行技术分析和基本面分析。
# 常见的复权价格类型包括：
# 前复权价格（前向复权价格）：以股票除权日为基准，将除权日之前的价格按照除权因子进行调整，使得除权日之前的价格与除权日当天的价格相一致。
# 后复权价格（后向复权价格）：以股票除权日为基准，将除权日之后的价格按照除权因子进行调整，使得除权日之后的价格与除权日当天的价格相一致。


import pandas as pd
import pymysql


# 数据库连接信息
wind = {
    'host': '192.168.7.93',
    'port': 3306,
    'username': 'quantchina',
    'password': 'zMxq7VNYJljTFIQ8',
    'database': 'wind'
}


# 链接数据库
def connect_to_database(db_info):
    """
    连接到数据库
    :param db_info: 数据库连接信息
    :return: 数据库连接对象
    """
    host = db_info['host']
    port = db_info['port']
    username = db_info['username']
    password = db_info['password']
    database = db_info['database']

    try:
        # 连接到MySQL数据库
        conn = pymysql.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")


def select_data_by_date_index(input_date: int, index_type, conn):
    """
    根据输入的日期从数据库中选取满足条件的数据
    参数:
        - input_date: 输入的日期（int 类型）
    """
    '''步骤一：首先让用户输入一个日期（int 类型），在表格INDEXMEMBERS中选出对应的S_CON_WINDCODE 
    并且 S_CON_OUTDATA （用代码将其转换为int）为 NULL 或者 S_CON_OUTDATA > 输入的日期
    '''

    step_1 = f"SELECT S_INFO_WINDCODE, S_CON_WINDCODE, CAST(S_CON_OUTDATE AS SIGNED) " \
             f"FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '{index_type}' " \
             f"AND (S_CON_OUTDATE IS NULL OR CAST(S_CON_OUTDATE AS SIGNED) > {input_date})"
    try:
        # 执行查询
        cursor = conn.cursor()
        cursor.execute(step_1)

        # 获取查询结果
        step1_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        selected_data = pd.DataFrame(step1_result, columns=columns)

        # 输出结果
        print("满足条件的数据：")
        print(selected_data)
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

        # 计算三个列表各自的和
        sum_preclose = sum(pre_close)
        sum_topen = sum(t_open)
        sum_tclose = sum(t_close)

        # 计算三个收益
        a = (sum_topen - sum_tclose)/len(pre_close)
        print("当天涨跌平均值为：", a)
        b = (sum_topen - sum_preclose) / len(pre_close)
        print("隔夜涨跌平均值为：", b)
        c = (sum_tclose - sum_preclose) / len(pre_close)
        print("日内涨跌平均值为：", c)

    except pymysql.Error as e:
        print(f"执行查询时发生错误: {e}")

    # 关闭数据库连接
    conn.close()


if __name__ == "__main__":
    # 连接到数据库
    conn = connect_to_database(wind)

    # 执行步骤1
    s_date = int(input("请输入查询的日期: "))
    index_type = input("请输入要查询的指数： ")
    select_data_by_date_index(s_date, index_type, conn)
