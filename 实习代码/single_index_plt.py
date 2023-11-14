# -*- coding = utf-8 -*-
# @Time: 2023/11/1 9:15
# @Author: Wu You
# @File：single_index_plt.py
# @Desc: 说明：统计指定指数内部成分股的行业分布，绘制柱状图（图显示比例）
#        行业 - WIND一级行业
#        数据库 - Wind金融数据库
# @Software: PyCharm


# 定义函数，接受数据库连接信息和数据库名称作为参数
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import pymysql
import matplotlib.ticker as ticker
import os

from connect import connect_to_database, wind


# 指定中文字体文件的路径
font_path = './reference/Arial Unicode.ttf'
# 加载中文字体
font = FontProperties(fname=font_path)
# 设置中文字体
plt.rcParams['font.family'] = font.get_name()

# 定义查询字典--统一使用WIND 行业分类
aindex_description = "AINDEXDESCRIPTION"    # 指数代码对应行业名称
aindex_member = "AINDEXMEMBERS"    # 指数成分股
aindex_wind_member = 'AINDEXMEMBERSWIND'     # WIND 指数成分股


# 行业分布画图函数
def plot_industry_distribution(conn, category):
    """
    绘制行业分布图
    :param conn: 数据库连接对象
    :param category: 指数类别
    """

    # 步骤pre：查找指数对应中文名称
    query_pre = f"SELECT S_INFO_COMPNAME FROM {aindex_description} " \
                f"WHERE S_INFO_WINDCODE = '{category}'"
    try:
        # 建立游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(query_pre)
        # 获取查询结果
        name_result = cursor.fetchall()
        name_result_str = name_result[0][0]
    except pymysql.Error as e:
        print(f"执行查询发生错误: {e}")

    # 步骤1：选取 AINDEXMEMBERS 和 AINDEXMEMBERSWIND 表中指定类别的所有S_CON_WINDCODE，且 S_CON_OUTDATE 为null
    query_step1 = f"SELECT S_CON_WINDCODE FROM {aindex_member} " \
                  f"WHERE S_INFO_WINDCODE = '{category}' AND S_CON_OUTDATE IS NULL " \
                  f"UNION " \
                  f"SELECT S_CON_WINDCODE FROM {aindex_wind_member} WHERE F_INFO_WINDCODE = '{category}'" \
                  f"AND S_CON_OUTDATE IS NULL"
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

    # 将选出的成分股代码放入str，用’，‘隔开， 以下警报原因因为可能出现except情况，无法查询到selected data，所以提示有可能referenc before assignment
    codes = selected_data['S_CON_WINDCODE'].tolist()
    codes_str = "', '".join(codes)

    # 步骤2：选取ASHAREINDUSTRIESCLASS表中与AIndexMembers和 AINDEXMEMBERSWIND表格中S_CON_WINDCODE数值相同的S_INFO_INDCODE，
    # 且REMOVE_DT为null，统计每个对应的SW_IND_CODE的数量，并绘制成柱状图
    # 构建查询语句
    query_step2 = f"SELECT WIND_IND_CODE " \
                  f"FROM ASHAREINDUSTRIESCLASS " \
                  f"WHERE S_INFO_WINDCODE IN ('{codes_str}') " \
                  f"AND REMOVE_DT IS NULL"

    try:
        # 重新开启游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(query_step2)
        result_step2 = cursor.fetchall()
    except pymysql.Error as e:
        print(f"执行查询发生错误: {e}")

    # 将查询结果转换为DataFrame，同理提示reference before assignment
    df = pd.DataFrame(result_step2, columns=['WIND_CODE'])
    df['WIND_CODE'] = df['WIND_CODE'].astype(int)
    df['WIND_CODE'] = df['WIND_CODE'] // 1000000 * 1000000

    # 种类及类别
    category_counts = df['WIND_CODE'].value_counts().reset_index()
    category_counts.columns = ['category', 'count']

    # 打印每个种类的数量，并保存csv文件
    # print(category_counts)
    # filename = f"{category}.csv"
    # category_counts.to_csv(filename, index=False)

    # 读取对照关系文件
    mapping_df = pd.read_csv('./reference/wind_industry_level_1.csv')

    # 创建一个字典，将 'ind_code' 映射为中文名称
    mapping_dict = dict(zip(mapping_df['ind_code'], mapping_df['name']))

    # 将 'SW_IND_CODE' 替换为对应的中文名称
    df['WIND_CODE'] = df['WIND_CODE'].replace(mapping_dict)

    # 统计每个SW_IND_CODE的数量
    wind_code_counts = df['WIND_CODE'].value_counts()
    # 打印种类和数量
    for name, count in wind_code_counts.items():
        print(f"WIND_CODE: {name}, Count: {count}")

    # 获取 CSV 文件中的所有行业名称
    all_industries = mapping_df['name'].tolist()

    # 创建一个新的 Series，索引为所有行业名称，值初始化为 0
    industry_counts = pd.Series(0, index=all_industries)

    # 使用 sw_ind_code_counts 更新 industry_counts，如果行业存在则赋值相应的数量
    industry_counts.update(wind_code_counts)
    print(industry_counts)

    # 设置图像尺寸
    fig, ax = plt.subplots(figsize=(14, 8))

    # 计算占比
    total_count = len(df)
    print(total_count)
    percentages = [count / total_count * 100 for count in industry_counts.values]

    # 绘制柱状图
    bars = ax.bar(range(len(industry_counts.values)), percentages, width=0.8, color='b')

    # 设置 x 轴标签
    ax.set_xticks(range(len(industry_counts.index)))
    ax.set_xticklabels(all_industries, rotation=90, fontsize=14)

    # 设置 y 轴标签为百分号
    formatter = ticker.PercentFormatter(xmax=100)
    ax.yaxis.set_major_formatter(formatter)

    # 设置y轴范围# 设置y轴范围
    max_percentage = max(industry_counts.values) / len(df) * 100
    ax.set_ylim(0, max_percentage * 1.1)  # 设置y轴范围为0到最大百分比的1.1倍

    # 调整y轴刻度标签的字体大小
    ax.set_yticks(ax.get_yticks())
    ax.set_yticklabels(ax.get_yticks(), fontsize=14)

    for i, rect in enumerate(bars):
        height = rect.get_height()
        percentage = percentages[i]
        label = f'{percentage:.2f}%' if i < len(industry_counts) else '0%'
        ax.text(rect.get_x() + rect.get_width() / 2, height, label, ha='center', va='bottom',
                fontsize=14, rotation=0, weight='bold')

    plt.xlabel('行业', fontsize=12)
    plt.ylabel('占比', fontsize=12)
    plt.title(name_result_str + '一级行业分布情况', fontsize=12)

    # 调整图像布局
    plt.tight_layout()

    # 保存图像文件到当前目录
    filename = f'{name_result_str}_industry_distribution.png'
    filepath = os.path.join('./result/single_plt_result', filename)
    plt.savefig(filepath, dpi=800)

    # 显示图像
    plt.show()

    # 关闭游标和数据库连接
    cursor.close()

    return


if __name__ == "__main__":
    # 连接到数据库
    conn = connect_to_database(wind)

    # 提示用户输入指数类别
    category = input("请输入要绘制行业分布图的指数类别: ")

    # 绘制行业分布图
    plot_industry_distribution(conn, category)

    conn.close()





