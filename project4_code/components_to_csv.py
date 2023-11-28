# -*- coding = utf-8 -*-
# @Time: 2023/11/22 9:15
# @Author: Wu You
# @File：components_to_csv.py
# @Desc: 说明：预处理，定义功能函数
# @Software: PyCharm

# 市值加权指数 = ∑(股票价格 × 股票的发行股本) / 指数基期总市值 × 指数基期点位
# 等权重指数 = ∑(股票价格 × 相同权重) / 指数基期总市值 × 指数基期点位
# 流通市值加权指数 = ∑(股票价格 × 股票的流通股本) / 指数基期总市值 × 指数基期点位

from datetime import date, datetime, timedelta
import re
import pandas as pd
import os
import pymysql
from matplotlib import pyplot as plt
import ray


from connect import connect_to_database, wind
from nearest_trading_day import calculate_trade_days
import appoint_double_index_plt

# pre_data = pd.read_csv('')

ray.init()

# 指定日期当天最新成分股
def components_ac(conn, index, date):

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
        print(f"执行查询发生错误: {e}")

    query_step1 = f"SELECT S_CON_WINDCODE FROM AINDEXMEMBERS " \
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
        cursor.execute(query_step1)
        # 获取查询结果
        components_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        selected_data = pd.DataFrame(components_result, columns=columns)
        # 将成分股名单储存成list
        components_list = selected_data['S_CON_WINDCODE'].tolist()
        num_components_list = len(components_list)
        cursor.close()

    except pymysql.Error as e:
        print(f"执行查询成分股时发生错误: {e}")

    query_step2 = f"SELECT S_INFO_WINDCODE FROM ASHAREST " \
                  f"WHERE CAST(ENTRY_DT AS SIGNED) < {date} " \
                  f"AND (REMOVE_DT IS NULL OR CAST(REMOVE_DT AS SIGNED) >= {date} )"

    try:
        # 重新开启游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(query_step2)
        st_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        selected_data = pd.DataFrame(st_result, columns=columns)
        st_list = selected_data['S_INFO_WINDCODE'].tolist()
        cursor.close()

    except pymysql.Error as e:
        print(f"执行查询st名单时发生错误: {e}")

    com_remove_st = []
    for stock in components_list:
        if stock not in st_list:
            com_remove_st.append(stock)
    com_remove_st = list(set(com_remove_st))

    # 将整数类型的日期转换为日期对象
    given_date_str = str(date)
    given_date = datetime.strptime(given_date_str, "%Y%m%d").date()
    # 计算90天前的日期
    delta = timedelta(days=90)
    past_date = given_date - delta
    # 将90天前的日期转换为整数类型
    past_date_int = int(past_date.strftime("%Y%m%d"))
    remove_list_str = "', '".join(com_remove_st)

    # 在com_remove_st 中筛选上市日期大于90天的股票
    query_step3 = f"SELECT S_CON_WINDCODE FROM AINDEXMEMBERS " \
                  f"WHERE S_CON_WINDCODE IN ('{remove_list_str}' )" \
                  f"AND CAST(S_CON_INDATE AS SIGNED) <= {past_date_int} " \
                  f"UNION " \
                  f"SELECT S_CON_WINDCODE FROM AINDEXMEMBERSWIND " \
                  f"WHERE S_CON_WINDCODE IN ('{remove_list_str}') " \
                  f"AND CAST(S_CON_OUTDATE AS SIGNED) <= {past_date_int} "

    try:
        # 重新开启游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(query_step3)
        rm90_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        rm90_data = pd.DataFrame(rm90_result, columns=columns)
        # 以list储存去处st，*st，上市不足90天的成分股
        ac_final_list = rm90_data['S_CON_WINDCODE'].tolist()
        num_ac_final_list = len(ac_final_list)
        cursor.close()

    except pymysql.Error as e:
        print(f"执行查询发生错误: {e}")

    return ac_final_list, name_result_str, num_components_list, num_ac_final_list, components_list


def weight_method_val(conn, date, list):

    list_str = "', '".join(list)
    query_step3 = f"SELECT S_INFO_WINDCODE, S_VAL_MV FROM ASHAREEODDERIVATIVEINDICATOR " \
                  f"WHERE S_INFO_WINDCODE IN ('{list_str}') " \
                  f"AND CAST(TRADE_DT AS SIGNED) = {date} "

    try:
        # 重新开启游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(query_step3)
        val_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        selected_data = pd.DataFrame(val_result, columns=columns)
        total = selected_data['S_VAL_MV'].sum()  # 计算列的总和
        selected_data['val_weight'] = selected_data['S_VAL_MV'].div(total)
        selected_data['val_weight'] = selected_data['val_weight'].apply(float)
        selected_data['val_weight'] = selected_data['val_weight'].round(10)
        result_df = selected_data[['S_INFO_WINDCODE', 'val_weight']]

        cursor.close()

        return result_df

    except pymysql.Error as e:
        print(f"执行查询发生错误: {e}")


def weight_method_dq(conn, date, list):

    list_str = "', '".join(list)
    query_step4 = f"SELECT S_INFO_WINDCODE, S_DQ_MV FROM ASHAREEODDERIVATIVEINDICATOR " \
                  f"WHERE S_INFO_WINDCODE IN ('{list_str}') " \
                  f"AND TRADE_DT = {date} "

    try:
        # 重新开启游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(query_step4)
        dq_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        selected_data = pd.DataFrame(dq_result, columns=columns)
        total = selected_data['S_DQ_MV'].sum()  # 计算列的总和
        selected_data['dq_weight'] = selected_data['S_DQ_MV'].div(total)
        selected_data['dq_weight'] = selected_data['dq_weight'].apply(float)
        selected_data['dq_weight'] = selected_data['dq_weight'].round(10)
        result_df = selected_data[['S_INFO_WINDCODE', 'dq_weight']]
        cursor.close()

        return result_df

    except pymysql.Error as e:
        print(f"执行查询发生错误: {e}")


def weight_method_equal(conn, date, list):

    list_str = "', '".join(list)
    query_step5 = f"SELECT S_INFO_WINDCODE FROM ASHAREEODDERIVATIVEINDICATOR " \
                  f"WHERE S_INFO_WINDCODE IN ('{list_str}') " \
                  f"AND TRADE_DT = {date} "

    try:
        # 重新开启游标对象，执行查询
        cursor = conn.cursor()
        cursor.execute(query_step5)
        dq_result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        selected_data = pd.DataFrame(dq_result, columns=columns)
        n = len(selected_data['S_INFO_WINDCODE'])  # 获取列中元素的数量
        selected_data['equal_weight'] = 1 / n
        selected_data['equal_weight'] = selected_data['equal_weight'].apply(float)
        selected_data['equal_weight'] = selected_data['equal_weight'].round(10)
        result_df = selected_data[['S_INFO_WINDCODE', 'equal_weight']]

        cursor.close()

        return result_df

    except pymysql.Error as e:
        print(f"执行查询发生错误: {e}")


def assign_market_weight(components_df, weight_list):
    if components_df['S_INFO_WINDCODE'].startswith(('300', '301')):
        return weight_list[0]
    elif components_df['S_INFO_WINDCODE'].startswith(('688', '689')):
        return weight_list[1]
    elif components_df['S_INFO_WINDCODE'].startswith(("000", "001", "002", "003", "004")):
        return weight_list[2]
    elif components_df['S_INFO_WINDCODE'].startswith(("600", "601", "603", "605")):
        return weight_list[3]


def remove_elements(lst, *args):
    for arg in args:
        while arg in lst:
            lst.remove(arg)
    return lst


def save_to_csvf(components_list):
    # 筛选出以 "sh" 结尾的元素
    listsh = [component for component in components_list if component.endswith("SH")]
    listsh_numbers_only = [item.split('.')[0] for item in listsh]
    # 筛选出以 "sz" 结尾的元素
    listsz = [component for component in components_list if component.endswith("SZ")]
    listsz_numbers_only = [item.split('.')[0] for item in listsz]

    # 定义你想要创建的文件夹的路径
    folder_path = f"./quote/{date}"

    # 使用 os.path.exists() 检查文件夹是否已经存在
    if not os.path.exists(folder_path):
        # 如果文件夹不存在，则使用 os.makedirs() 创建文件夹
        os.makedirs(folder_path)

    for stock in listsh_numbers_only:
        file_path = f'/Volumes/data/tick/stock/{date}/quote/sh_{stock}_{date}_quote.parquet'  # 替换为实际的文件路径
        df = pd.read_parquet(file_path)
        df.to_csv(f'./quote/{date}/{stock}.SH.csv')

    for stock in listsz_numbers_only:
        file_path = f'/Volumes/data/tick/stock/{date}/quote/sz_{stock}_{date}_quote.parquet'  # 替换为实际的文件路径
        df = pd.read_parquet(file_path)
        df.to_csv(f'./quote/{date}/{stock}.SZ.csv')


def single_ror(conn, components_ac_list, date):

    codes_str = "', '".join(components_ac_list)
    step = f"SELECT S_DQ_ADJPRECLOSE, S_DQ_ADJOPEN, S_DQ_ADJCLOSE FROM ASHAREEODPRICES " \
           f"WHERE S_INFO_WINDCODE IN ('{codes_str}') AND CAST(TRADE_DT AS SIGNED) = {date}"

    # 执行查询
    try:
        cursor = conn.cursor()
        cursor.execute(step)
        step_result = cursor.fetchall()

        # 将查询结果转换为数据框
        df = pd.DataFrame(step_result, columns=['S_DQ_ADJPRECLOSE', 'S_DQ_ADJOPEN', 'S_DQ_ADJCLOSE'])

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

        dangri_avg = round(dangri_avg, 5)
        geye_avg = round(geye_avg, 5)
        rinei_avg = round(rinei_avg, 5)

        print("当天涨跌平均值为：", round(dangri_avg, 5))
        print("日内涨跌平均值为：", round(rinei_avg, 5))
        print("隔夜涨跌平均值为：", round(geye_avg, 5))

        cursor.close()

    except pymysql.Error as e:
        print(f"执行查询时发生错误: {e}")

    return dangri_avg, geye_avg, rinei_avg


def plot_two_industry_distributions(conn, name, full_list, list_defined):

    list1 = full_list
    name1 = str(name)
    industry_counts1, all1 = appoint_double_index_plt.plot_industry_distribution(conn, list1)

    name2 = 'Appointed'
    industry_counts2, all2 = appoint_double_index_plt.plot_industry_distribution(conn, list_defined)

    # 计算两个指数的行业分布的百分比
    percentages1 = [count / sum(industry_counts1) * 100 for count in industry_counts1]
    percentages2 = [count / sum(industry_counts2) * 100 for count in industry_counts2]

    # 创建一个新的DataFrame，索引为全行业标签列表，列为两个指数的行业分布的百分比

    df = pd.DataFrame({
        name1: pd.Series(percentages1, index=all1),
        name2: pd.Series(percentages2, index=all2)
    })

    # 绘制并排柱状图
    ax = df.plot(kind='bar', figsize=(24, 14), width=0.9)  # 更改图表大小

    # 添加百分比标签，位置调整到柱状图顶部
    for i, (p1, p2) in enumerate(zip(percentages1, percentages2)):
        ax.text(i - 0.2, p1, f'{p1:.1f}%', va='bottom', ha='center', fontsize=20, weight='bold')
        ax.text(i + 0.2, p2, f'{p2:.1f}%', va='bottom', ha='center', fontsize=20, weight='bold')

    # 设置图表的标题、x轴标签和y轴标签
    ax.set_title(f'{name1}和{name2}的行业分布对比', fontsize=30)
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
    ax.legend(fontsize='24', labels=[name1, name2])

    # 调整子图边距
    plt.subplots_adjust(bottom=0.25)

    # 调整图像布局
    plt.tight_layout()

    # 保存图像文件到当前目录
    filename = f'{name1}和{name2}_industry_distribution.png'
    filepath = os.path.join(f'./quote/{date}/detail_information', filename)
    plt.savefig(filepath, dpi=800)

    # 显示图表
    plt.show()


@ray.remote
def process_date(conn, index, date):
    temporary_components_list, index_name, num_components, num_filter, full_components_list = components_ac(conn, index, date)
    print('指数原始成分股数量：', num_components)
    print('去除ST，*ST，上市不足90天后的成分股数量：', num_filter)
    # 获取用户输入,删除用户输入的字符串
    delete_input = input("请输入你想从列表中删除的字符串，多个字符串请用逗号分隔：")
    delete_input_list = delete_input.split(',')
    components_list = remove_elements(temporary_components_list, *delete_input_list)
    print(f'基于{index_name}下的定制指数于交易日{date}下的数量的为{len(components_list)}成分为：')
    print(components_list)

    dangri, geye, rinei = single_ror(conn, components_list, date)
    data_list.append({'日期': date, '当天收益率': dangri, '日内收益率': rinei, '隔夜收益率': geye})

    # 获取全部的csv数据
    save_to_csvf(components_list)

    # 定义你想要创建的文件夹的路径
    folder_path1 = f"./quote/{date}/detail_information"
    # 使用 os.path.exists() 检查文件夹是否已经存在
    if not os.path.exists(folder_path1):
        # 如果文件夹不存在，则使用 os.makedirs() 创建文件夹
        os.makedirs(folder_path1)

    plot_two_industry_distributions(conn, index_name, full_components_list, components_list)

    # 定义每个加权方式
    df = pd.DataFrame(components_list, columns=['S_INFO_WINDCODE'])
    df['market_weight'] = df.apply(lambda row: assign_market_weight(row, market_weight_list), axis=1)
    df_dq = weight_method_dq(conn, date, components_list)
    df_val = weight_method_val(conn, date, components_list)
    df_eq = weight_method_equal(conn, date, components_list)
    merged_df = df.merge(df_dq, on="S_INFO_WINDCODE").merge(df_eq, on="S_INFO_WINDCODE").merge(df_val, on="S_INFO_WINDCODE")
    merged_df.to_csv(f'./quote/{date}/detail_information/components.csv')

    #对选定的components 进行加权处理
    if Weight_method == '1':
        w_col = 'equal_weight'
    elif Weight_method == '2':
        w_col = 'dq_weight'
    elif Weight_method == '3':
        w_col = 'val_weight'
    else:
        print("无效的选择。")
        exit()

    df_print = merged_df[['S_INFO_WINDCODE', w_col, 'market_weight']]
    df_print['cal_weight'] = df_print[w_col] * df_print['market_weight']
    df_print.to_csv(f'./quote/{date}/detail_information/rq_df.csv')
    print(df_print)

    df_ror = pd.concat([pd.DataFrame([i]) for i in data_list], ignore_index=True)

    return df_ror


if __name__ == "__main__":

    conn = connect_to_database(wind)
    data_list = []
    index = input('请输入指数：')
    Weight_method = input("请选择加权方式：1--等权，2--流通市值，3--总市值: ")
    market_weight = input('请输入创业、科创、深主、沪主的权重配比： ')
    market_weight_list = list(map(int, re.findall(r'\d+', market_weight)))
    start_date = int(input('请输入开始日期：'))
    end_date = int(input("请输入结束日期："))

    trade_dates = calculate_trade_days(start_date, end_date, conn)
    df_ror_refs = [process_date.remote(conn, index, date) for date in trade_dates]
    df_ror = ray.get(df_ror_refs)
    # for date in trade_dates:
    #     temporary_components_list, index_name, num_components, num_filter, full_components_list = components_ac(conn, index, date)
    #     print('指数原始成分股数量：', num_components)
    #     print('去除ST，*ST，上市不足90天后的成分股数量：', num_filter)
    #     # 获取用户输入,删除用户输入的字符串
    #     delete_input = input("请输入你想从列表中删除的字符串，多个字符串请用逗号分隔：")
    #     delete_input_list = delete_input.split(',')
    #     components_list = remove_elements(temporary_components_list, *delete_input_list)
    #     print(f'基于{index_name}下的定制指数于交易日{date}下的数量的为{len(components_list)}成分为：')
    #     print(components_list)
    #
    #     dangri, geye, rinei = single_ror(conn, components_list, date)
    #     data_list.append({'日期': date, '当天收益率': dangri, '日内收益率': rinei, '隔夜收益率': geye})
    #
    #     # 获取全部的csv数据
    #     save_to_csvf(components_list)
    #
    #     # 定义你想要创建的文件夹的路径
    #     folder_path1 = f"./quote/{date}/detail_information"
    #     # 使用 os.path.exists() 检查文件夹是否已经存在
    #     if not os.path.exists(folder_path1):
    #         # 如果文件夹不存在，则使用 os.makedirs() 创建文件夹
    #         os.makedirs(folder_path1)
    #
    #     plot_two_industry_distributions(conn, index_name, full_components_list, components_list)
    #
    #     # 定义每个加权方式
    #     df = pd.DataFrame(components_list, columns=['S_INFO_WINDCODE'])
    #     df['market_weight'] = df.apply(lambda row: assign_market_weight(row, market_weight_list), axis=1)
    #     df_dq = weight_method_dq(conn, date, components_list)
    #     df_val = weight_method_val(conn, date, components_list)
    #     df_eq = weight_method_equal(conn, date, components_list)
    #     merged_df = df.merge(df_dq, on="S_INFO_WINDCODE").merge(df_eq, on="S_INFO_WINDCODE").merge(df_val, on="S_INFO_WINDCODE")
    #     merged_df.to_csv(f'./quote/{date}/detail_information/components.csv')
    #
    #     #对选定的components 进行加权处理
    #     if Weight_method == '1':
    #         w_col = 'equal_weight'
    #     elif Weight_method == '2':
    #         w_col = 'dq_weight'
    #     elif Weight_method == '3':
    #         w_col = 'val_weight'
    #     else:
    #         print("无效的选择。")
    #         exit()
    #
    #     df_print = merged_df[['S_INFO_WINDCODE', w_col, 'market_weight']]
    #     df_print['cal_weight'] = df_print[w_col] * df_print['market_weight']
    #     df_print.to_csv(f'./quote/{date}/detail_information/rq_df.csv')
    #     print(df_print)
    #
    #     df_ror = pd.concat([pd.DataFrame([i]) for i in data_list], ignore_index=True)

    df_ror.to_csv(f'./quote/{start_date}_{end_date}内收益率.csv', index=False)

    conn.close()

    ray.shutdown()
