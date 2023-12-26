import pandas as pd

from v2_code.utilities_class import Plot
from v2_code.components_class import Componentsacquisition
from v2_code.connect import connect_to_database, wind



if __name__ == "__main__":
    conn = connect_to_database(wind)
    components = Componentsacquisition(conn, '000016.SH', 20230921)
    name_str, components_df = components.components_ac()
    industry_dis_df = components.industry_distrubution()
    wind_code_counts = industry_dis_df['industry_wind_index'].value_counts()
    count_df = pd.DataFrame(wind_code_counts).reset_index()
    count_df.columns = ['S_INFO_WINDCODE', 'time']
    count_df['percentage'] = count_df['time'] / wind_code_counts.sum()
    Plot.single_industry_plt(industry_dis_df, name_str)
    c_industry_ror_df = components.components_single_date_industry_ror()
    pd.set_option('display.max_columns', None)
    result_df = c_industry_ror_df.merge(count_df, on='S_INFO_WINDCODE')
    # 可选择保存df
    result_df.to_csv('./p1_result.csv')
    print(result_df)

    conn.close()

