# -*- coding = utf-8 -*-
# @Time: 2023/11/12 15:30
# @Author: Wu You
# @File：000985.CSI_index.py
# @Desc: 说明：中证全指的成分股是否是由：上证指数、深证成指、创业板指、科创板（以 688 或 689 开头的标的）组成，
#             如有遗漏或者差异记录一下
# @Software: PyCharm

import pandas as pd
import csv
from v2_code.connect import connect_to_database, wind


csi_constituents = list(pd.read_csv('../reference/old/中证全有效成分股.csv')['S_CON_WINDCODE'])
szse_constituents = list(pd.read_csv('../reference/new/深证成分股.csv')['S_CON_WINDCODE'])
shse_constituents = list(pd.read_csv('../reference/new/上证成分股.csv')['S_CON_WINDCODE'])
chuangye_constituents = list(pd.read_csv('../reference/new/创业成分股.csv')['S_CON_WINDCODE'])
kechuang_constituents = list(pd.read_csv('../reference/new/科创成分股.csv')['S_CON_WINDCODE'])


def combine_index():
    accumulate_constituents = []
    intersection_constituents = []

    for stock in szse_constituents:
        accumulate_constituents.append(stock)
    for stock in shse_constituents:
        accumulate_constituents.append(stock)
    for stock in chuangye_constituents:
        accumulate_constituents.append(stock)
    for stock in kechuang_constituents:
        accumulate_constituents.append(stock)

    accumulate_constituents = list(set(accumulate_constituents))
    # print(accumulate_constituents)
    # print(len(accumulate_constituents))

    for stock in accumulate_constituents:
        if stock in csi_constituents:
            intersection_constituents.append(stock)
    intersection_constituents = list(set(intersection_constituents))
    # print(intersection_constituents)
    # print(len(intersection_constituents))

    return intersection_constituents


def index_detail():
    b = combine_index()

    part_kechuang_constituents = [stock for stock in b if stock.startswith(("688", "689"))]
    num_part_kechuang = len(part_kechuang_constituents)

    part_chuangye_constituents = [stock for stock in b if stock.startswith(("300", "301"))]
    num_part_chuangye = len(part_chuangye_constituents)

    part_shse_constituents = [stock for stock in b if stock.startswith(("600", "601", "603", "605"))]
    num_part_shse = len(part_shse_constituents)

    part_szse_constituents = [stock for stock in b if stock.startswith(("000", "001", "002", "003", "004"))]
    num_part_szse = len(part_szse_constituents)

    total_intersection = num_part_szse + num_part_chuangye + num_part_shse + num_part_kechuang
    per_szse = round(num_part_szse / total_intersection, 5)
    per_shse = round(num_part_shse / total_intersection, 5)
    per_kechuang = round(num_part_kechuang / total_intersection, 5)
    per_chuangye = round(num_part_chuangye / total_intersection, 5)

    print("交集总数为：", total_intersection)
    print("上证：深证：创业：科创的标的数量比例为：", num_part_shse, ":", num_part_szse, ":", num_part_chuangye, ":", num_part_kechuang)
    print("上证：深证：创业：科创的标的占比比例为：", per_shse, ":", per_szse, ":", per_chuangye, ":", per_kechuang)

    # 储存数值方便后续MV对比
    part_kechuang_str = "', '".join(part_kechuang_constituents)
    part_shse_str = "', '".join(part_shse_constituents)
    part_szse_str = "', '".join(part_szse_constituents)
    part_chuangye_str = "', '".join(part_chuangye_constituents)

    return part_kechuang_str, part_shse_str, part_szse_str, part_chuangye_str


def index_detail_nor():
    b = combine_index()

    part_kechuang_constituents = [stock for stock in b if stock.startswith(("688", "689"))]
    num_part_kechuang = len(part_kechuang_constituents)

    part_chuangye_constituents = [stock for stock in b if stock.startswith(("300", "301"))]
    num_part_chuangye = len(part_chuangye_constituents)

    part_shse_constituents = [stock for stock in b if stock.startswith(("600", "601", "603", "605"))]
    num_part_shse = len(part_shse_constituents)

    part_szse_constituents = [stock for stock in b if stock.startswith(("000", "001", "002", "003", "004"))]
    num_part_szse = len(part_szse_constituents)

    max_intersection = max(num_part_szse, num_part_chuangye, num_part_shse, num_part_kechuang)
    per_szse_nor = round(num_part_szse / max_intersection, 5)
    per_shse_nor = round(num_part_shse / max_intersection, 5)
    per_kechuang_nor = round(num_part_kechuang / max_intersection, 5)
    per_chuangye_nor = round(num_part_chuangye / max_intersection, 5)

    print("上证：深证：创业：科创的标的数量比例为：", num_part_shse, ":", num_part_szse, ":", num_part_chuangye, ":", num_part_kechuang)
    print("上证：深证：创业：科创的标的占比比例为：", per_shse_nor, ":", per_szse_nor, ":", per_chuangye_nor, ":", per_kechuang_nor)


def mv_compare(conn, codes_str, today):

    query = f"SELECT S_VAL_MV FROM ASHAREEODDERIVATIVEINDICATOR " \
            f"WHERE S_INFO_WINDCODE IN ('{codes_str}') " \
            f"AND CAST(TRADE_DT AS SIGNED) = {today} "

    cursor = conn.cursor()
    cursor.execute(query)
    # 获取查询结果
    query_result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    selected_data = pd.DataFrame(query_result, columns=columns)

    total_mv = selected_data.sum()

    total_mv = round(float(total_mv), 2)

    return total_mv


def mv_compare_detail():
    kechuang, shse, szse, chuangye = index_detail()
    print("在当前分类条件下：")
    kechuang_mv = mv_compare(conn, kechuang, today)
    shse_mv = mv_compare(conn, shse, today)
    szse_mv = mv_compare(conn, szse, today)
    chuangye_mv = mv_compare(conn, chuangye, today)
    index_detail()
    print("上证：深证：创业：科创的市值分别为：", shse_mv, ":", szse_mv, ":", chuangye_mv, ":", kechuang_mv)

    total_mv = shse_mv + szse_mv + kechuang_mv + chuangye_mv
    per_szse_mv = round(szse_mv / total_mv, 5)
    per_shse_mv = round(shse_mv / total_mv, 5)
    per_kechuang_mv = round(kechuang_mv / total_mv, 5)
    per_chuangye_mv = round(chuangye_mv / total_mv, 5)
    print("上证：深证：创业：科创的市值的比例为：", per_shse_mv, ":", per_szse_mv, ":", per_chuangye_mv, ":", per_kechuang_mv)


def mv_compare_detail_normal():
    kechuang, shse, szse, chuangye = index_detail()
    print("在当前分类条件下：")
    kechuang_mv = mv_compare(conn, kechuang, today)
    shse_mv = mv_compare(conn, shse, today)
    szse_mv = mv_compare(conn, szse, today)
    chuangye_mv = mv_compare(conn, chuangye, today)
    index_detail()
    print("上证：深证：创业：科创的市值分别为：", shse_mv, ":", szse_mv, ":", chuangye_mv, ":", kechuang_mv)

    max_mv = max(szse_mv, shse_mv, kechuang_mv, chuangye_mv)
    per_szse_mv_nor = round(szse_mv / max_mv, 5)
    per_shse_mv_nor = round(shse_mv / max_mv, 5)
    per_kechuang_mv_nor = round(kechuang_mv / max_mv, 5)
    per_chuangye_mv_nor = round(chuangye_mv / max_mv, 5)
    print("上证：深证：创业：科创的市值的比例为：", per_shse_mv_nor, ":", per_szse_mv_nor, ":", per_chuangye_mv_nor, ":", per_kechuang_mv_nor)


def check_csi_composition():

    missing_constituents = []
    differing_constituents = []
    accumulate_constituents = []
    both_constituents = []

    for stock in szse_constituents:
        accumulate_constituents.append(stock)
    for stock in shse_constituents:
        accumulate_constituents.append(stock)
    for stock in chuangye_constituents:
        accumulate_constituents.append(stock)

    accumulate_constituents = list(set(accumulate_constituents))
    print(len(accumulate_constituents))

    # 判断在3指数中但不在csi中：
    for stock in accumulate_constituents:
        if stock not in csi_constituents:
            missing_constituents.append(stock)

    missing_constituents = list(set(missing_constituents))

    print("缺失的成分股数量:", len(missing_constituents))
    print("缺失的成分股:", missing_constituents)

    with open('../reference/components_miss_diff/missing_constituents.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Stock'])  # 写入表头
        writer.writerows([[stock] for stock in missing_constituents])

    filtered_missing_constituents = [stock for stock in missing_constituents if not stock.startswith(("688", "689"))]
    print("不以 688 或 689 开头的缺失成分股数量:", len(filtered_missing_constituents))
    print("不以 688 或 689 开头的缺失成分股:", filtered_missing_constituents)

    with open('../reference/components_miss_diff/filtered_missing_constituents.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Stock'])  # 写入表头
        writer.writerows([[stock] for stock in filtered_missing_constituents])

    for stock in csi_constituents:
        if stock not in accumulate_constituents:
            differing_constituents.append(stock)

    differing_constituents = list(set(differing_constituents))

    print("有差异的成分股数量:", len(differing_constituents))
    print("有差异的成分股:", differing_constituents)

    with open('../reference/components_miss_diff/differing_constituents.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Stock'])  # 写入表头
        writer.writerows([[stock] for stock in differing_constituents])

    filtered_differing_constituents = [stock for stock in differing_constituents if not stock.startswith(("688", "689"))]
    print("不以 688 或 689 开头的差异成分股数量:", len(filtered_differing_constituents))
    print("不以 688 或 689 开头的差异成分股:", filtered_differing_constituents)

    with open('../reference/components_miss_diff/filtered_differing_constituents.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Stock'])  # 写入表头




    for stock in accumulate_constituents:
        if stock in csi_constituents:
            both_constituents = [].append(stock)
    a = (len(both_constituents))

    return a


if __name__ == "__main__":
    # detail_index()
    conn = connect_to_database(wind)
    today = int(input("请输入需要查询的日期："))

    mv_compare_detail_normal()
    index_detail_nor()




    conn.close()


