import os
import pandas as pd

folder_path = './quote/20231127_detail'  # 文件夹1的路径

df2 = pd.read_csv('./quote/20231127/detail_information/rq_df.csv')

# 遍历文件夹中的所有文件
for filename in os.listdir(folder_path):
    if filename.startswith('processed_resample') and filename.endswith('.csv'):  # 筛选以"processed_reference"开头且以.csv结尾的文件
        file_path = os.path.join(folder_path, filename)  # 文件的完整路径

        # 读取CSV文件
        df1 = pd.read_csv(file_path)

        # 找出'symbol'列的众数
        mode_symbol = df1['symbol'].mode()

        df2_select = df2[['S_INFO_WINDCODE', 'cal_weight']]

        start_index = df2[df2['S_INFO_WINDCODE'] == mode_symbol[0]].index[0]

        cal_weight_value = df2.loc[start_index, 'cal_weight'].round(8)

        df1['weight'] = cal_weight_value
        df1['weight_prc'] = df1['last_prc'] * df1['weight'].round(8)

        # 保存修改后的DataFrame到同名文件
        output_path = os.path.join(folder_path, filename)
        df1.to_csv(output_path, index=False)

        print(f"Processed file: {filename}")