from datetime import datetime, timedelta
import pymysql
import pandas as pd

from v2_code.connect import connect_to_database, wind

# with self.db_connector.get_connection() as conn:
#     with conn.cursor() as cursor:

class Componentsacquisition:
    def __init__(self, conn, index_name, date):
        self.conn = conn
        self.index = index_name
        self.date = date
        self.components_df = None
        self.index_name_str = None
        self.industry_distribution_df = None
        self.market_distribution_df = None
        self.components_filter_st_df = None
        self.components_rmndays_df = None
        self.components_eodprice_df = None
        self.c_inday = None
        self.c_day = None
        self.c_overnight = None
        self.industry_ror_df = None
        self.sh50_freeshare = None

    # 指定日期当天最新成分股
    def components_ac(self):
        # 步骤pre：查找指数对应中文名称
        query_pre = f"SELECT S_INFO_COMPNAME FROM AINDEXDESCRIPTION " \
                    f"WHERE S_INFO_WINDCODE = '{self.index}'"
        try:
            cursor = self.conn.cursor()
            cursor.execute(query_pre)
            # 获取查询结果
            name_result = cursor.fetchall()
            name_result_str = name_result[0][0]
            cursor.close()
        except pymysql.Error as e:
            print(f"执行查询指数对应中文名称发生错误: {e}")

        query_step = f"SELECT S_CON_WINDCODE, S_INFO_WINDCODE FROM AINDEXMEMBERS " \
                     f"WHERE S_INFO_WINDCODE = '{self.index}' " \
                     f"AND S_CON_INDATE <= {self.date} " \
                     f"AND ( S_CON_OUTDATE IS NULL OR CAST(S_CON_OUTDATE AS SIGNED) >= {self.date} )" \
                     f"UNION " \
                     f"SELECT S_CON_WINDCODE, F_INFO_WINDCODE FROM AINDEXMEMBERSWIND " \
                     f"WHERE F_INFO_WINDCODE = '{self.index}' " \
                     f"AND S_CON_INDATE <= {self.date} " \
                     f"AND ( S_CON_OUTDATE IS NULL OR CAST(S_CON_OUTDATE AS SIGNED) >= {self.date} )"
        try:
            cursor = self.conn.cursor()
            cursor.execute(query_step)
            components_result = cursor.fetchall()
            selected_data = pd.DataFrame(components_result, columns=['S_INFO_WINDCODE', 'INDEX_NUM'])
            cursor.close()
        except pymysql.Error as e:
            print(f"执行查询成分股时发生错误: {e}")

        self.index_name_str = name_result_str
        self.components_df = selected_data
        return name_result_str, selected_data

    # 获得成分股的WIND一级行业分布信息
    def industry_distribution(self):
        if self.components_df is None:
            self.components_ac()
        # 成分股list转换为str, ', '分隔
        list_str = "', '".join(self.components_df['S_INFO_WINDCODE'].astype(str))
        query_step = f"SELECT S_INFO_WINDCODE, WIND_IND_CODE " \
                     f"FROM ASHAREINDUSTRIESCLASS " \
                     f"WHERE S_INFO_WINDCODE IN ('{list_str}') " \
                     f"AND REMOVE_DT IS NULL"
        try:
            cursor = self.conn.cursor()
            cursor.execute(query_step)
            queery_result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            industry_distribution_df = pd.DataFrame(queery_result, columns=columns)
            industry_distribution_df['WIND_IND_CODE'] = industry_distribution_df['WIND_IND_CODE'] \
                .apply(lambda x: x[:-6] + '000000' if isinstance(x, str) and x[-6:].isdigit() else x)

            # 使用'name'列中的值来替换'ind_code'列中的对应值，
            # 使用replace()函数将df中'WIND_IND_CODE'列中的值替换为mapping_dict中的对应值
            mapping_df = pd.read_csv('./reference/wind_industry_level_1.csv')
            mapping_df['ind_code'] = mapping_df['ind_code'].astype(str)
            mapping_dict = dict(zip(mapping_df['ind_code'], mapping_df['name']))
            industry_distribution_df['industry_chinese_name'] = industry_distribution_df['WIND_IND_CODE'].replace(
                mapping_dict)
            mapping_dict2 = dict(zip(mapping_df['ind_code'], mapping_df['wind_code']))
            industry_distribution_df['industry_wind_index'] = industry_distribution_df['WIND_IND_CODE'].replace(
                mapping_dict2)
            cursor.close()
        except pymysql.Error as e:
            print(f"执行查询WIND一级行业发生错误: {e}")

        self.industry_distribution_df = industry_distribution_df
        return industry_distribution_df

    # 判断市场分布
    def market_distribution(self):
        if self.components_df is None:
            self.components_ac()

        market_distribution_df = self.components_df
        market_distribution_df = market_distribution_df.rename(columns={'S_CON_WINDCODE': 'S_INFO_WINDCODE'})

        def determine_market(code):
            market_name_list = ['深板创业', '沪板科创', '深主', '沪主']
            if code.startswith(('300', '301')):
                return market_name_list[0]
            elif code.startswith(('688', '689')):
                return market_name_list[1]
            elif code.startswith(("000", "001", "002", "003", "004")):
                return market_name_list[2]
            elif code.startswith(("600", "601", "603", "605")):
                return market_name_list[3]

        market_distribution_df['Market'] = market_distribution_df['S_INFO_WINDCODE'].apply(determine_market)

        self.market_distribution_df = market_distribution_df
        return market_distribution_df

    # ST, *ST 成分股剔除 --- 返回去除ST, *ST 的成分股list
    def st_remove(self):
        if self.components_df is None:
            self.components_ac()

        # 查找ST, *ST 成分股
        query_step = f"SELECT S_INFO_WINDCODE FROM ASHAREST " \
                     f"WHERE CAST(ENTRY_DT AS SIGNED) < {self.date} " \
                     f"AND (REMOVE_DT IS NULL OR CAST(REMOVE_DT AS SIGNED) >= {self.date} )"
        try:
            cursor = self.conn.cursor()
            cursor.execute(query_step)
            st_result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            selected_data = pd.DataFrame(st_result, columns=columns)
            cursor.close()
        except pymysql.Error as e:
            print(f"执行查询st名单时发生错误: {e}")
        # 遍历计算去除ST, *ST 的成分股list
        components_filter_st_df = \
            self.components_df[~self.components_df['S_INFO_WINDCODE'].isin(selected_data['S_INFO_WINDCODE'])]

        self.components_filter_st_df = components_filter_st_df
        return components_filter_st_df

    # 去除不满上市不满n天的成分股
    def remove_nomore_n_days(self, n_days):
        if self.components_df is None:
            self.components_ac()

        # 将list转换为str，用', '分割
        remove_list_str = "', '".join(self.components_df['S_INFO_WINDCODE'].astype(str))
        # 计算n天前的日期
        given_date_str = str(self.date)
        given_date = datetime.strptime(given_date_str, "%Y%m%d").date()
        delta = timedelta(days=n_days)
        past_date_int = int((given_date - delta).strftime("%Y%m%d"))
        # print(past_date_int)

        # 在com_remove_st 中筛选上市日期大于90天的股票
        query_step = f"SELECT S_CON_WINDCODE FROM AINDEXMEMBERS " \
                     f"WHERE S_CON_WINDCODE IN ('{remove_list_str}' )" \
                     f"AND CAST(S_CON_INDATE AS SIGNED) <= {past_date_int} " \
                     f"UNION " \
                     f"SELECT S_CON_WINDCODE FROM AINDEXMEMBERSWIND " \
                     f"WHERE S_CON_WINDCODE IN ('{remove_list_str}') " \
                     f"AND CAST(S_CON_OUTDATE AS SIGNED) <= {past_date_int} "
        try:
            cursor = self.conn.cursor()
            cursor.execute(query_step)
            rm_ndays_result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            select_data = pd.DataFrame(rm_ndays_result, columns=columns)
            cursor.close()
        except pymysql.Error as e:
            print(f"执行查询距今为止上市时间超过{n_days}的有效成分股发生错误: {e}")

        self.components_rmndays_df = select_data
        return select_data

    def remove_add_components(self):
        if self.components_df is None:
            self.components_ac()
        remove_input_str = input("输入删除的成分股，逗号分割：")
        values_to_remove = [val.strip() for val in remove_input_str.split(',')]
        df_removed = self.components_df[~self.components_df['S_INFO_WINDCODE'].isin(values_to_remove)]
        add_input_str = input("输入加入的成分股，逗号分割： ")
        if add_input_str.strip():
            values_to_add = [val.strip() for val in add_input_str.split(',')]
            new_entries = pd.DataFrame(values_to_add, columns=['S_INFO_WINDCODE'])
            filtered_df = pd.concat([df_removed, new_entries], ignore_index=True)
        else:
            filtered_df = df_removed

        return filtered_df

    def components_single_date_ew_ror(self):
        if self.components_df is None:
            self.components_ac()
        remove_list_str = "', '".join(self.components_df['S_INFO_WINDCODE'].astype(str))
        query_step = f" SELECT S_INFO_WINDCODE, S_DQ_ADJPRECLOSE, S_DQ_ADJCLOSE, S_DQ_ADJOPEN " \
                     f"FROM ASHAREEODPRICES " \
                     f"WHERE S_INFO_WINDCODE IN ('{remove_list_str}') " \
                     f"AND CAST(TRADE_DT AS SIGNED) = {self.date} "
        try:
            cursor = self.conn.cursor()
            cursor.execute(query_step)
            query_step_result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            components_eodprice = pd.DataFrame(query_step_result, columns=columns)
            day = (components_eodprice['S_DQ_ADJCLOSE'] / components_eodprice['S_DQ_ADJPRECLOSE']).mean() - 1
            inday = (components_eodprice['S_DQ_ADJCLOSE'] / components_eodprice['S_DQ_ADJOPEN']).mean() - 1
            overnight = (components_eodprice['S_DQ_ADJOPEN'] / components_eodprice['S_DQ_ADJPRECLOSE']).mean() - 1
            cursor.close()
            self.components_eodprice_df = components_eodprice
            self.c_day = day
            self.c_inday = inday
            self.c_overnight = overnight
            return components_eodprice, day, inday, overnight
        except pymysql.Error as e:
            print(f"执行查询成分股每日交易信息发生错误: {e}")

    def components_single_date_industry_ror(self):
        if self.industry_distribution_df is None:
            self.industry_distribution()
        unique_values = self.industry_distribution_df['industry_wind_index'].unique()
        unique_values = unique_values.astype(str)
        result_str = "', '".join(unique_values)
        query_step = f"SELECT S_INFO_WINDCODE, S_DQ_PRECLOSE, S_DQ_CLOSE, S_DQ_OPEN " \
                     f"FROM AINDEXWINDINDUSTRIESEOD " \
                     f"WHERE S_INFO_WINDCODE IN ('{result_str}') " \
                     f"AND CAST(TRADE_DT AS SIGNED) = {self.date}"
        try:
            cursor = self.conn.cursor()
            cursor.execute(query_step)
            query_step_result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            industry_ror_df = pd.DataFrame(query_step_result, columns=columns)
            industry_ror_df['overnight'] = industry_ror_df['S_DQ_OPEN'] / industry_ror_df['S_DQ_PRECLOSE'] - 1
            industry_ror_df['inday'] = industry_ror_df['S_DQ_CLOSE'] / industry_ror_df['S_DQ_OPEN'] - 1
            industry_ror_df['day'] = industry_ror_df['S_DQ_CLOSE'] / industry_ror_df['S_DQ_PRECLOSE'] - 1
            cursor.close()
        except pymysql.Error as e:
            print(f"执行查询成分股行业收益率发生错误: {e}")

        self.industry_ror_df = industry_ror_df

        return industry_ror_df

    def freeshare(self):
        if self.components_df is None:
            self.components_ac()
        list_str = "', '".join(self.components_df['S_INFO_WINDCODE'].astype(str))

        query = f"SELECT SHR_CALCULATION FROM AINDEXSSE50WEIGHT" \
                f"WHERE S_CON_WINDCODE IN ('{list_str}') " \
                f"AND CAST(TRADE_DT AS SIGNED) = {self.date}"
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            freeshare_result = cursor.fetchall()
            column = [des[0] for des in cursor.description]
            freeshare_df = pd.DataFrame(freeshare_result, columns=column)
            cursor.close()
        except pymysql.err as e:
            print(f"执行查询sh50{self.date}调整股本发生错误: {e}")
        self.sh50_freeshare = freeshare_df


# demo part
if __name__ == "__main__":
    conn = connect_to_database(wind)
    components = Componentsacquisition(conn, '000016.SH', 20230921)
    name_str, components_df = components.components_ac()
    # print(name_str)
    # print(components_df)
    industry_dis_df = components.industry_distribution()
    # print(industry_dis_df)
    market_dis_df = components.market_distribution()
    # print(market_dis_df)
    remove_df = components.st_remove()
    # print(remove_df)
    # 数字可以任意指定
    remove_ndays_df = components.remove_nomore_n_days(90)
    components_add_remove_df = components.remove_add_components()
    # print(components_add_remove_df)
    c_eod_df, c_inday, c_day, c_overnight = components.components_single_date_ew_ror()
    # print(c_eod_df)
    # print(c_overnight)
    c_industry_ror_df = components.components_single_date_industry_ror()
    # print(c_industry_ror_df)