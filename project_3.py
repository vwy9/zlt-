import pandas as pd


from v2_code.connect import connect_to_database, wind
from v2_code.components_class import Componentsacquisition
from v2_code.utilities_class import Plot


if __name__ == "__main__":
    conn = connect_to_database(wind)
    index1 = '000016.SH'
    index2 = '399300.SZ'
    date = 20230921
    c = Componentsacquisition(conn, index1, date)
    name_str1, components_df1 = c.components_ac()
    industry_df1 = c.industry_distribution()
    c2 = Componentsacquisition(conn, index2, date)
    name_str2, components_df2 = c2.components_ac()
    industry_df2 = c2.industry_distribution()
    Plot.double_index_industry_plt(industry_df1, industry_df2, name_str1, name_str2)
    conn.close()