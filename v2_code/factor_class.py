from datetime import datetime, timedelta

import pandas as pd
import pymysql


class Factorcalculation:
    def __init__(self, conn):
        self.conn = conn
        self.rsy_sigma = None

    def rsy_calculation(self, df, date):
        given_date_str = str(date)
        given_date = datetime.strptime(given_date_str, "%Y%m%d").date()
        delta = timedelta(days=10)
        past_date_int = int((given_date - delta).strftime("%Y%m%d"))
        list_str = df['symbol'].mode().to_string(index=False)

        query = f"SELECT S_INFO_WINDCODE, S_DQ_CLOSE, S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW " \
                f"FROM ASHAREEODPRICES " \
                f"WHERE S_INFO_WINDCODE IN ('{list_str}') " \
                f"AND (CAST(TRADE_DT AS SIGNED) < {date} AND CAST(TRADE_DT AS SIGNED) >= {past_date_int}) "
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            query_result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            rsy_df = pd.DataFrame(query_result, columns=columns)

            rsy_df['u'] = rsy_df['S_DQ_HIGH'].apply(lambda x: x.ln()) - \
                rsy_df['S_DQ_OPEN'].apply(lambda x: x.ln())
            rsy_df['d'] = rsy_df['S_DQ_LOW'].apply(lambda x: x.ln()) - \
                rsy_df['S_DQ_OPEN'].apply(lambda x: x.ln())
            rsy_df['c'] = rsy_df['S_DQ_CLOSE'].apply(lambda x: x.ln()) - \
                rsy_df['S_DQ_OPEN'].apply(lambda x: x.ln())
            rsy_df['rsy'] = rsy_df['u'] * (rsy_df['u'] - rsy_df['c']) + \
                rsy_df['d'] * (rsy_df['d'] - rsy_df['c'])
            rsy_sigma = rsy_df['rsy'].mean()

            cursor.close()

        except pymysql.Error as e:
            print(f"执行查询发生错误: {e}")

        self.rsy_sigma = rsy_sigma
        return rsy_sigma
