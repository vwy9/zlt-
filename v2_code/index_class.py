import pandas as pd
import pymysql

from connect_class import wind, DatabaseConnector


class Index:
    def __init__(self, index_name, date):
        self.db_connector = DatabaseConnector(wind)
        self.index = index_name
        self.date = date
        self.i_ror_df = None
        self.i_inday = None
        self.i_day = None
        self.i_overnight = None

    def index_single_date_ror(self):
        with self.db_connector.get_connection() as conn:
            with conn.cursor() as cursor:
                query_step = f"SELECT S_INFO_WINDCODE, S_DQ_PRECLOSE, S_DQ_CLOSE, S_DQ_OPEN " \
                             f"FROM AINDEXEODPRICES " \
                             f"WHERE S_INFO_WINDCODE = '{self.index}' " \
                             f"AND CAST(TRADE_DT AS SIGNED) = {self.date}"
                try:
                    cursor.execute(query_step)
                    index_ror_result = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    index_ror_df = pd.DataFrame(index_ror_result, columns=columns)

                    overnight = index_ror_df['S_DQ_OPEN'] / index_ror_df['S_DQ_PRECLOSE'] - 1
                    inday = index_ror_df['S_DQ_CLOSE'] / index_ror_df['S_DQ_OPEN'] - 1
                    day = index_ror_df['S_DQ_CLOSE'] / index_ror_df['S_DQ_PRECLOSE'] - 1

                except pymysql.Error as e:
                    print(f"执行查询发生错误: {e}")
            self.i_ror_df = index_ror_df
            self.i_day = day
            self.i_overnight = overnight
            self.i_inday = inday
            return index_ror_df, day, inday, overnight,


# demo part
if __name__ == "__main__":
    index = '000016.SH'
    date = 20231201
    index = Index(index, date)
    i_df, i_day, i_inday, i_overnight = index.index_single_date_ror()
    print(i_df, i_day, i_inday, i_overnight)
