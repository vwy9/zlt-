import pandas as pd
import numpy as np
from pyecharts import options as opts
from pyecharts.charts import Line, Bar, Grid


from v2_code.utilities_class import Datapreprocess


if __name__ == "__main__":
    index = '000985.SH'
    index_list = [index]
    print(index_list)
    date = 20231020
    Datapreprocess.index_save_to_csvf(index_list, date)