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
import os

from connect import connect_to_database


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


# 行业分布画图函数
def plot_industry_distribution(conn, category):
    """
    绘制行业分布图
    :param conn: 数据库连接对象
    :param category: 指数类别
    """

    # 数据库表名
    a_index_members_table = 'AINDEXMEMBERS'
    a_share_sw_industries_class_table = 'ASHARESWINDUSTRIESCLASS'

    # 步骤1：选取AIndexMembers表中指定类别的所有S_CON_WINDCODE，且REMOVE_DT为null
    query_step1 = f"SELECT S_CON_WINDCODE FROM {a_index_members_table} " \
                  f"WHERE S_INFO_WINDCODE = '{category}' AND S_CON_OUTDATE IS NULL"
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
    except pymysql.Error as e:
        print(f"执行查询发生错误: {e}")

    codes = selected_data['S_CON_WINDCODE'].tolist()
    codes_str = "', '".join(codes)

    # 步骤2：选取AShareSWIndustriesClass表中与AIndexMembers表格中S_CON_WINDCODE数值相同的S_INFO_INDCODE，
    # 且REMOVE_DT为null，统计每个对应的SW_IND_CODE的数量，并绘制成柱状图
    # 构建查询语句
    query_step2 = f"SELECT SW_IND_CODE " \
                  f"FROM {a_share_sw_industries_class_table} " \
                  f"WHERE S_INFO_WINDCODE IN ('{codes_str}') " \
                  f"AND REMOVE_DT IS NULL"

    try:
        # 重新开启游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(query_step2)
        result_step2 = cursor.fetchall()
    except pymysql.Error as e:
        print(f"执行查询发生错误: {e}")

    # 将查询结果转换为DataFrame
    df = pd.DataFrame(result_step2, columns=['SW_IND_CODE'])
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
    for name, count in sw_ind_code_counts.items():
        print(f"SW_IND_CODE: {name}, Count: {count}")

    df['SW_IND_CODE'] = df['SW_IND_CODE'].astype(str)

    # 绘制柱状图
    fig, ax = plt.subplots(figsize=(30, 20))  # 调整图形的大小

    # 调整 x 轴的元素间隔
    bar_width = 0.8  # 设置柱形的宽度
    x = range(len(sw_ind_code_counts))
    bars = ax.bar(x, sw_ind_code_counts.values, width=bar_width, color='b')

    plt.xlabel('SW_IND_CODE')
    plt.ylabel('Count')
    plt.title('SW_IND_CODE')

    # 调整 x 轴刻度标签的显示
    plt.xticks(x, sw_ind_code_counts.index, rotation=90, fontsize=8)  # 调整刻度标签的旋转角度和字体大小

    # 调整 y 轴的高度
    ax.set_ylim(0, sw_ind_code_counts.values.max() * 1.4)  # 设置 y 轴的上限为最大值的 1.4 倍

    # 添加数值标签和占比标签
    for i, rect in enumerate(bars):
        height = rect.get_height()
        count = sw_ind_code_counts.values[i]
        percentage = count / len(df) * 100
        ax.text(rect.get_x() + rect.get_width() / 2, height, f'{height}\n{percentage:.2f}%', ha='center', va='bottom')

    # 保存图像文件到当前目录
    filename = f'{category}_industry_distribution.png'
    filepath = os.path.join('./plt_result', filename)
    plt.savefig(filepath, dpi=800)
    plt.tight_layout()
    plt.show()

    # 关闭游标和数据库连接
    cursor.close()
    conn.close()


if __name__ == "__main__":
    # 连接到数据库
    conn = connect_to_database(wind)

    # 提示用户输入指数类别
    category1 = input("请输入要绘制行业分布图的指数类别: ")

    # 绘制行业分布图
    plot_industry_distribution(conn, category1)




