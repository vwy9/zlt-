# -*- coding = utf-8 -*-
# @Time: 2023/11/1 9:15
# @Author: Wu You
# @File：single_index_plt.py
# @Desc: 说明：模糊搜索功能，包含关键词，可返回单列，也可以返回整行
# @Software: PyCharm


from connect import connect_to_database, wind
# 在执行特定讲搜索内容放到df，输出csv是可以启用
import pandas as pd


# 执行模糊搜索
def fuzzy_search(conn, table_name, search_string, column='column_name'):
    """
    在指定表的指定列中执行模糊搜索，并返回匹配的结果
    :param conn: 数据库连接对象
    :param table_name: 表名
    :param search_string: 搜索字符串
    :param column: 要搜索的列名，默认为 'column_name'
    :return: 匹配的结果集
    """

    # 创建游标对象
    cursor = conn.cursor()
    # 构建模糊搜索的查询语句

    # 包含
    # query = f"SELECT * FROM {table_name} WHERE {column} LIKE %s " \
            # f" AND S_INFO_EXCHMARKET = '{SZSE}' " \
            # f"AND (CAST(S_CON_OUTDATE AS SIGNED) > {input_date} OR S_CON_OUTDATE IS NULL)"
    # WHERE ( A = '{category}' ) 可以加入限定条件

    # 以特定开头

    query = f"SELECT * FROM {table_name} WHERE {column} LIKE %s  " \
            f"AND CUR_SIGN = 1 "
            # f"AND USED = 1 AND LEVELNUM = 2" \
            # f"AND LEVELNUM = 2 AND USED = 1" \
            # f"AND S_INFO_EXCHMARKET = '{wind}' AND EXPIRE_DATE IS NULL"

    # 以特定结尾
    # query = f"SELECT * FROM {table_name} WHERE {column} LIKE '%%' || %s"

    # 表达s内容
    search_value = f"%{search_string}%"

    # 执行查询
    cursor.execute(query, (search_value,))
    result = cursor.fetchall()

    # 获取查询结果的列名列表
    column_names = [desc[0] for desc in cursor.description]

    # 保存参数 A 的列表
    parameter_a_list = []
    # parameter_b_list = []
    count2 = 0

    # 打印搜索结果并保存参数 A
    for row1 in result:
        parameter_a = row1[column_names.index('S_CON_WINDCODE')]  # 假设参数 A 的列名为 'A'
        parameter_a_list.append(parameter_a)

        # parameter_b = row1[column_names.index('INDUSTRIESCODE_OLD')]  # 假设参数 A 的列名为 'A'
        # parameter_b_list.append(parameter_b)

        print(row1)
        count2 = count2 + 1

    print(count2)

    # data = {'S_INFO_WINDCODE': parameter_a_list, 'S_INFO_NAME': parameter_b_list}
    # df = pd.DataFrame(data)
    #
    # df.to_csv('wind一级行业.csv', index=False)

    # 关闭游标
    cursor.close()

    # 返回参数 A 的列表
    return parameter_a_list


if __name__ == "__main__":
    # 连接到数据库
    conn = connect_to_database(wind)

    # 执行模糊搜索，表格，搜索内容，指定搜索列
    fuzzy_search(conn, 'AINDEXMEMBERSWIND', '882122.WI', 'F_INFO_WINDCODE')
