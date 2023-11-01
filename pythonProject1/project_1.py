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

# 指定中文字体文件的路径
font_path = 'Arial Unicode.ttf'

# 加载中文字体
font = FontProperties(fname=font_path)

# 设置中文字体
plt.rcParams['font.family'] = font.get_name()

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
    df['SW_IND_CODE'] = df['SW_IND_CODE'].astype(int)
    df['SW_IND_CODE'] = df['SW_IND_CODE'] // 1000000 * 1000000

    # 读取对照关系文件
    mapping_df = pd.read_csv('./SW_industry_level_1.csv')

    # 创建一个字典，将 'ind_code' 映射为中文名称
    mapping_dict = dict(zip(mapping_df['ind_code'], mapping_df['name']))

    # 将 'SW_IND_CODE' 替换为对应的中文名称
    df['SW_IND_CODE'] = df['SW_IND_CODE'].replace(mapping_dict)

    # 统计每个SW_IND_CODE的数量
    sw_ind_code_counts = df['SW_IND_CODE'].value_counts()
    # 打印种类和数量
    for category, count in sw_ind_code_counts.items():
        print(f"SW_IND_CODE: {category}, Count: {count}")

    df['SW_IND_CODE'] = df['SW_IND_CODE'].astype(str)

    # 绘制柱状图
    fig, ax = plt.subplots(figsize=(20, 12))  # 调整图形的大小

    # 调整 x 轴的元素间隔
    bar_width = 0.8  # 设置柱形的宽度
    x = range(len(sw_ind_code_counts))
    bars = ax.bar(x, sw_ind_code_counts.values, width=bar_width, color= 'b')

    plt.xlabel('SW_IND_CODE')
    plt.ylabel('Count')
    plt.title('Industry Distribution')

    # 调整 x 轴刻度标签的显示
    plt.xticks(x, sw_ind_code_counts.index, rotation=90, fontsize=8)  # 调整刻度标签的旋转角度和字体大小

    # 调整 y 轴的高度
    ax.set_ylim(0, sw_ind_code_counts.values.max() * 1.4)  # 设置 y 轴的上限为最大值的 1.1 倍

    # 添加数值标签和占比标签
    for i, rect in enumerate(bars):
        height = rect.get_height()
        count = sw_ind_code_counts.values[i]
        percentage = count / len(df) * 100
        ax.text(rect.get_x() + rect.get_width() / 2, height, f'{height}\n{percentage:.2f}%', ha='center', va='bottom')

    plt.tight_layout()
    plt.show()

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
