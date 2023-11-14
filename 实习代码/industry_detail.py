# -*- coding = utf-8 -*-
# @Time: 2023/11/1 9:15
# @Author: Wu You
# @File：industry_detail.py
# @Desc: 说明：输入日期、指数代码 输出指数内部行业分布信息，以及行业的行情信息
#              一个csv/excel 类型的文件，分为五列：
#              行业代码，行业中文名称、标的数量、标的占比、行业对应指数当天涨跌幅
# @Software: PyCharm


# 定义函数，接受数据库连接信息和数据库名称作为参数
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import pymysql

from connect import connect_to_database, wind
from index_ROR import select_data_by_date_index
from nearest_trading_day import find_nearest_trading_day


# 指定中文字体文件的路径
font_path = './reference/Arial Unicode.ttf'
# 加载中文字体
font = FontProperties(fname=font_path)
# 设置中文字体
plt.rcParams['font.family'] = font.get_name()


# 行业分布画图函数
def industry_distribution(conn, date, category):
    """
    绘制行业分布图
    :param conn: 数据库连接对象
    :param date: 需要查询的日期
    :param category: 指数类别
    """

    # 数据库表名
    a_index_members_table = 'AINDEXMEMBERS'
    a_share_industries_class_table = 'ASHAREINDUSTRIESCLASS'
    aindex_wind_member = 'AINDEXMEMBERSWIND'  # WIND 指数成分股

    # 查找指数对应中文名称
    result = f"SELECT S_INFO_COMPNAME FROM AINDEXDESCRIPTION " \
             f"WHERE S_INFO_WINDCODE = '{category}'"
    try:
        # 建立游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(result)

        # 获取查询结果
        name_result = cursor.fetchall()
        name_result_str = name_result[0][0]
    except pymysql.Error as e:
        print(f"执行查询发生错误: {e}")

    # 步骤1：选取 AINDEXMEMBERS 和 AINDEXMEMBERSWIND 表中指定类别的所有S_CON_WINDCODE，且 日期为限定
    query_step1 = f"SELECT S_CON_WINDCODE FROM {a_index_members_table} " \
                  f"WHERE S_INFO_WINDCODE = '{category}' " \
                  f"AND CAST(S_CON_INDATE AS SIGNED) <= {date} " \
                  f"AND (CAST(S_CON_OUTDATE AS SIGNED) >= {date} OR S_CON_OUTDATE IS NULL)" \
                  f"UNION " \
                  f"SELECT S_CON_WINDCODE FROM {aindex_wind_member} " \
                  f"WHERE F_INFO_WINDCODE = '{category}'" \
                  f"AND CAST(S_CON_INDATE AS SIGNED) <= {date} " \
                  f"AND (CAST(S_CON_OUTDATE AS SIGNED) >= {date} OR S_CON_OUTDATE IS NULL)"
    # 构建查询语句
    try:
        # 建立游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(query_step1)

        # 获取查询结果
        step1_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        selected_data = pd.DataFrame(step1_result, columns=columns)

        # 输出结果
        print("满足条件的数据：")
        print(selected_data)

        # selected_data.to_csv('./result/appointed_list.csv', index=False)
    except pymysql.Error as e:
        print(f"执行查询发生错误: {e}")

    codes = selected_data['S_CON_WINDCODE'].tolist()
    codes_str = "', '".join(codes)

    # 步骤2：选取AShareIndustriesClass表中与AIndexMembers表格中S_CON_WINDCODE数值相同的S_INFO_INDCODE，
    # 且REMOVE_DT为null，统计每个对应的SW_IND_CODE的数量，并绘制成柱状图
    # 构建查询语句
    query_step2 = f"SELECT WIND_IND_CODE " \
                  f"FROM {a_share_industries_class_table} " \
                  f"WHERE S_INFO_WINDCODE IN ('{codes_str}') " \
                  f"AND CAST(ENTRY_DT AS SIGNED) <= {date} " \
                  f"AND (CAST(REMOVE_DT AS SIGNED) >= {date} OR REMOVE_DT IS NULL)"

    try:
        # 重新开启游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(query_step2)
        result_step2 = cursor.fetchall()
    except pymysql.Error as e:
        print(f"执行查询发生错误: {e}")

    # 将查询结果转换为DataFrame
    df = pd.DataFrame(result_step2, columns=['WIND_CODE'])
    df['WIND_CODE'] = df['WIND_CODE'].astype(int)
    df['WIND_CODE'] = df['WIND_CODE'] // 1000000 * 1000000

    # 种类及类别
    category_counts = df['WIND_CODE'].value_counts().reset_index()
    category_counts.columns = ['category', 'count']

    # 读取对照关系文件
    mapping_df = pd.read_csv('./reference/wind_industry_level_1.csv')

    # 创建一个字典，将 'ind_code' 映射为中文名称
    mapping_dict1 = dict(zip(mapping_df['ind_code'], mapping_df['name']))

    # 将 'SW_IND_CODE' 替换为对应的中文名称
    category_counts['category_chinese'] = category_counts['category'].replace(mapping_dict1)

    # 计算百分比并添加到新的"percentages"列
    total_count = category_counts['count'].sum()
    category_counts['percentages'] = (category_counts['count'] / total_count) * 100

    # 创建一个字典，将 'ind_code' 映射为指数
    mapping_dict2 = dict(zip(mapping_df['name'], mapping_df['wind_code']))

    # 将 'SW_IND_CODE' 替换为对应的中文名称
    category_counts['index_code'] = category_counts['category_chinese'].replace(mapping_dict2)

    print(category_counts)

    df2 = category_counts

    for index, row in df2.iterrows():
        index_code = row['index_code']
        print(index_code, ":")
        a, _, _ = select_data_by_date_index(date, index_code)
        df2.at[index, f'{date}_DangRi'] = round(a, 5)

        # 定义新的列顺序
    new_order = ['category', 'category_chinese', 'count', 'percentages', f'{date}_DangRi', 'index_code ']
    # 使用reindex方法对列进行重新排序
    df2 = df2.reindex(columns=new_order)

    selected_columns = ['category', 'category_chinese', 'count', 'percentages', f'{date}_DangRi']
    selected_df2 = df2[selected_columns]

    # 关闭游标和数据库连接
    cursor.close()

    return selected_df2, name_result_str


if __name__ == "__main__":
    # 用户输入日期
    input_date = int(input("请输入日期："))
    category = input("请输入指数：")

    conn = connect_to_database(wind)

    data_use = find_nearest_trading_day(input_date, conn)

    df, name = industry_distribution(conn, data_use, category)

    df.to_csv(f'./result/date_index_industry_detail/{data_use}_{name}行业详情信息.csv', index=False)

    conn.close()




