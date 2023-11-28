import os
import pandas as pd
from datetime import datetime, timedelta

folder_path = 'quote/20231127_detail'  # 文件夹路径
output_filename = './result.csv'  # 输出文件名

result_data = {}  # 用于存储结果的字典

for filename in os.listdir(folder_path):
    if filename.startswith('processed_resample') and filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)  # 文件的完整路径
        print(f"Processed file: {filename}")
        # 读取CSV文件
        df = pd.read_csv(file_path)

        # 遍历每一行
        for index, row in df.iterrows():
            time = row['time']
            weight_prc = row['weight_prc']

            # 将时间和加总结果添加到结果字典
            if time in result_data:
                result_data[time] += weight_prc
            else:
                result_data[time] = weight_prc

# 创建结果 DataFrame
result_df = pd.DataFrame(result_data.items(), columns=['time', 'prc_sum'])
result_df['prc_sum'] = result_df['prc_sum'].round(8)  # 保留8位小数
result_df = result_df.groupby('time')['prc_sum'].sum().reset_index()
result_df = result_df.sort_values('time')

print(result_df)

# 保存结果 DataFrame 到新的 CSV 文件
output_path = os.path.join(folder_path, output_filename)
result_df.to_csv(output_path, index=False)

print(f"Output file: {output_path}")