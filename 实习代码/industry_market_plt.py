import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import numpy as np
import matplotlib.ticker as ticker
import os


# 指定中文字体
font_path = './reference/Arial Unicode.ttf'
font = FontProperties(fname=font_path)
plt.rcParams['font.family'] = font.get_name()


# 行业映射code--name
def code_to_name(df):

    """
    :param df:
    :return:
    """

    # 使用'name'列中的值来替换'ind_code'列中的对应值，使用replace()函数将df中'WIND_IND_CODE'列中的值替换为mapping_dict中的对应值
    mapping_df = pd.read_csv('./reference/wind_industry_level_1.csv')

    # 创建一个新的 Series，统计每个SW_IND_CODE的数量
    wind_code_counts = df['industry_chinese_name'].value_counts()
    for name, count in wind_code_counts.items():
        print(f"Industry_chinese_name: {name}, Count: {count}")

    # 创建一个新的 Series，索引为所有行业名称，值初始化为 0
    all_industries = mapping_df['name'].tolist()
    industry_counts = pd.Series(0, index=all_industries)

    return industry_counts, all_industries


# 单宽基指数、定制指数wind以及行业分布画图
def single_index_industry_plt(df, name_result_str):
    """
    :param df: 包含 S_INFO_WINDCODE, WIND_IND_CODE 的df
    :return: 画图
    """

    industry_counts, all_industries = code_to_name(df)
    print(industry_counts)

    # 设置图像尺寸
    fig, ax = plt.subplots(figsize=(14, 8))

    # 计算占比
    percentages = [count / sum(industry_counts) * 100 for count in industry_counts]

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


# 双指数wind一级行业对比，可定制指数，也可以为宽基指数，取决于df的输入
def double_index_industry_plt(df1, df2, name_str1, name_str2):

    """
    :param df1: index1:包含 S_INFO_WINDCODE, WIND_IND_CODE 的df
    :param df2: index2:包含 S_INFO_WINDCODE, WIND_IND_CODE 的df
    :param name_str1: index1 的中文名称
    :param name_str2: index2 的中文名称
    :return:
    """

    # 获取两个指数的行业分布
    industry_counts1, all1 = code_to_name(df1)
    industry_counts2, all2 = code_to_name(df2)

    # 计算两个指数的行业分布的百分比
    percentages1 = [count / sum(industry_counts1) * 100 for count in industry_counts1]
    percentages2 = [count / sum(industry_counts2) * 100 for count in industry_counts2]

    # 创建一个新的DataFrame，索引为全行业标签列表，列为两个指数的行业分布的百分比

    df = pd.DataFrame({
        name_str1: pd.Series(percentages1, index=all1),
        name_str2: pd.Series(percentages2, index=all2)
    })

    # 绘制并排柱状图
    ax = df.plot(kind='bar', figsize=(24, 14), width=0.9)  # 更改图表大小

    # 添加百分比标签，位置调整到柱状图顶部
    for i, (p1, p2) in enumerate(zip(percentages1, percentages2)):
        ax.text(i - 0.2, p1, f'{p1:.1f}%', va='bottom', ha='center', fontsize=20, weight='bold')
        ax.text(i + 0.2, p2, f'{p2:.1f}%', va='bottom', ha='center', fontsize=20, weight='bold')

    # 设置图表的标题、x轴标签和y轴标签
    ax.set_title(f'{name_str1}和{name_str2}的行业分布对比', fontsize=30)
    ax.set_xlabel('行业', fontsize=24)
    ax.set_ylabel('百分比', fontsize=24)

    # 设置x轴标签的旋转角度和间隔
    plt.xticks(rotation=90, ha='right')

    # 获取柱状图最高点的值
    max_value = max(max(percentages1), max(percentages2))
    ax.set_ylim(0, max_value * 1.4)

    # 改变x轴标签的字体大小
    ax.tick_params(axis='x', labelsize=28)  # 设置为14，你可以根据需要调整这个值
    ax.tick_params(axis='y', labelsize=28)

    # 显示右上角的图例
    ax.legend(fontsize='24', labels=[name_str1, name_str2])

    # 调整子图边距
    plt.subplots_adjust(bottom=0.25)

    # 调整图像布局
    plt.tight_layout()

    # 保存图像文件到当前目录
    filename = f'{name_str1}和{name_str2}_industry_distribution.png'
    filepath = os.path.join('./result/double_plt_result', filename)
    plt.savefig(filepath, dpi=800)

    # 显示图表
    plt.show()


def market_plt(df, name1):

    """
    :param df:
    :param name1:
    :return:
    """

    # 计算'market'列中每个值的数量
    market_counts = df['Market_Name'].value_counts()
    fig, ax = plt.subplots()

    # 定义一个函数用于更新 autopct 参数的值
    def func(pct, allvals):
        absolute = int(pct / 100. * np.sum(allvals))
        return "{:.1f}%\n({:d})".format(pct, absolute)

    # 画一个环形图
    ax.pie(market_counts, labels=market_counts.index, autopct=lambda pct: func(pct, market_counts),
           startangle=90, pctdistance=0.85)

    # 在环形图中间画一个白色的圆形，以使其看起来像一个环形图
    centre_circle = plt.Circle((0, 0), 0.70, fc='white')
    fig = plt.gcf()
    ax.set_title(f'{name1}市场分布情况')
    fig.gca().add_artist(centre_circle)
    ax.axis('equal')
    plt.savefig(f'./result/market_plt_result/{name1}市场分布情况.png')
    plt.show()





