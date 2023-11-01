# -*- coding = utf-8 -*-
# @Time: 2023/11/1 9:15
# @Author: Wu You
# @File：project_1.py
# @Desc: 说明：统计指定指数内部成分股的行业分布，绘制柱状图（图显示比例和数值）
#        行业 - 申万一级行业
#        数据库 - Wind金融数据库
# @Software: PyCharm

# 定义函数，接受数据库连接信息和数据库名称作为参数
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import pymysql


# 数据库连接信息
wind = {
    'host': '192.168.7.93',
    'port': 3306,
    'username': 'quantchina',
    'password': 'zMxq7VNYJljTFIQ8',
    'database': 'wind'
}


# 定义函数，接受数据库连接信息和数据库名称作为参数
def plot_sw_ind_code_distribution(db_info, db_name):
    # 获取数据库连接信息
    host = db_info['host']
    port = db_info['port']
    username = db_info['username']
    password = db_info['password']
    database = db_name

    # 数据库表名
    a_index_members_table = 'AINDEXMEMBERS'
    a_share_sw_industries_class_table = 'ASHARESWINDUSTRIESCLASS'

    # 连接到MySQL数据库
    conn = pymysql.connect(
        host=host,
        port=port,
        user=username,
        password=password,
        database=database
    )

    # 步骤1：选取AIndexMembers表中指定类别的所有S_CON_WINDCODE，且REMOVE_DT为null

    # 用户指定的S_INFO_WINDCODE类别
    category = input("请输入指数类别: ")

    # 构建查询语句
    query_step1 = f"SELECT S_CON_WINDCODE FROM {a_index_members_table} WHERE S_INFO_WINDCODE = '{category}' AND S_CON_OUTDATE IS NULL"

    # 执行查询
    cursor = conn.cursor()
    cursor.execute(query_step1)
    result_step1 = cursor.fetchall()

    # 提取查询结果中的S_CON_WINDCODE值
    s_con_windcode_list = [row[0] for row in result_step1]

    # 关闭游标
    cursor.close()

    # 步骤2：选取AShareSWIndustriesClass表中与AIndexMembers表格中S_CON_WINDCODE数值相同的S_INFO_INDCODE，且REMOVE_DT为null，统计每个对应的SW_IND_CODE的数量，并绘制成柱状图

    # 构建查询语句
    query_step2 = f"SELECT A.S_INFO_WINDCODE, B.SW_IND_CODE FROM {a_index_members_table} A JOIN {a_share_sw_industries_class_table} B ON A.S_CON_WINDCODE = B.S_INFO_WINDCODE WHERE A.S_CON_WINDCODE IN ({','.join(['%s'] * len(s_con_windcode_list))}) AND B.REMOVE_DT IS NULL"

    # 执行查询
    cursor = conn.cursor()
    cursor.execute(query_step2, s_con_windcode_list)
    result_step2 = cursor.fetchall()

    # 将查询结果转换为DataFrame
    df = pd.DataFrame(result_step2, columns=['S_INFO_WINDCODE', 'SW_IND_CODE'])

    # 统计每个SW_IND_CODE的数量
    sw_ind_code_counts = df['SW_IND_CODE'].value_counts()
    # 打印种类和数量
    for category, count in sw_ind_code_counts.items():
        print(f"SW_IND_CODE: {category}, Count: {count}")

    # 读取对照关系文件
    mapping_df = pd.read_csv('./SW_industry_level_1.csv')

    # 创建一个字典，将'SW_IND_CODE'映射为中文名称
    mapping_dict = dict(zip(mapping_df['ind_code'], mapping_df['name']))

    # 将'SW_IND_CODE'替换为对应的中文名称
    df['SW_IND_CODE'] = df['SW_IND_CODE'].replace(mapping_dict)

    # 绘制柱状图
    bars = plt.bar(sw_ind_code_counts.index, sw_ind_code_counts.values, color=['red', 'green', 'blue', 'yellow'])
    plt.xlabel('SW_IND_CODE')
    plt.ylabel('Count')
    plt.title('Industry Distribution')
    plt.xticks(range(len(sw_ind_code_counts)), sw_ind_code_counts.index, rotation=90)
    plt.show()
    plt.tight_layout()
    # 关闭游标和数据库连接
    cursor.close()
    conn.close()


if __name__ == "__main__":
    # 输入数据库名称
    database_name = input("请输入数据库名称：")

    # 检查数据库名称是否与预定义的数据库名称匹配
    if database_name == wind['database']:
        plot_sw_ind_code_distribution(wind, database_name)
    else:
        print("数据库名称不匹配，请检查输入的数据库名称。")