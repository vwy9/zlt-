# -*- coding = utf-8 -*-
# @Time: 2023/11/1 9:15
# @Author: Wu You
# @File：project_1.py
# @Desc: 说明：模糊搜索功能，包含关键词，可返回单列，也可以返回整行
# @Software: PyCharm


from project_1 import connect_to_database

# 数据库连接信息
wind = {
    'host': '192.168.7.93',
    'port': 3306,
    'username': 'quantchina',
    'password': 'zMxq7VNYJljTFIQ8',
    'database': 'wind'
}


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
    query = f"SELECT * FROM {table_name} WHERE {column} LIKE %s"
    search_value = f"%{search_string}%"

    # 执行查询
    cursor.execute(query, (search_value,))
    result = cursor.fetchall()

    # 获取查询结果的列名列表
    column_names = [desc[0] for desc in cursor.description]

    # 保存参数 A 的列表
    parameter_a_list = []

    # 打印搜索结果并保存参数 A
    for row1 in result:
        parameter_a = row1[column_names.index('S_INFO_WINDCODE')]  # 假设参数 A 的列名为 'A'
        parameter_a_list.append(parameter_a)
        print(row1)

    # 关闭游标
    cursor.close()

    # 返回参数 A 的列表
    return parameter_a_list


if __name__ == "__main__":
    # 连接到数据库
    conn = connect_to_database(wind)

    # 执行模糊搜索
    fuzzy_search(conn, 'AINDEXDESCRIPTION', '科创板', 'S_INFO_NAME')