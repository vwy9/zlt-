import pandas as pd


class Signalscalculate:

    # 线性满仓
    @staticmethod
    def linear_full_signals(initial_investment_weight):
        df = pd.read_csv('../reference/time_stamp.csv')
        columns = ['Start_time', 'Duration', 'Trade_direction']
        df1 = pd.DataFrame(columns=columns)
        init_position_weight = float(initial_investment_weight)
        print("请输入数据，格式为：开始时间,持续时间(min),交易方向")

        while True:
            user_input = input("请输入数据（用逗号分隔）: ")
            if user_input == "":
                break
            try:
                start_time, duration, direction = user_input.split(',')
                new_row = pd.DataFrame([[start_time.strip(), duration.strip(), direction.strip()]], columns=columns)
                df1 = pd.concat([df1, new_row], ignore_index=True)
            except ValueError:
                print("输入格式有误，请按正确格式输入数据：开始时间,持续时间,交易方向")
                continue

        df['signal'] = init_position_weight
        df1['Start_time'] = pd.to_numeric(df1['Start_time'], errors='coerce')
        df1['Duration'] = pd.to_numeric(df1['Duration'], errors='coerce')

        df1 = df1.sort_values(by='Start_time')
        for _, row in df1.iterrows():
            start_time = row['Start_time']
            start_index = df[df['time'] == start_time].index.min()  # 假设这里是行索引
            duration_rows = int(row['Duration']) * 20  # 持续时间转换为行数 (每分钟20行)
            trade_direction = row['Trade_direction']

            if trade_direction == '+':
                weight_increment = (1 - df.at[start_index, 'signal']) / duration_rows

                for i in range(duration_rows + 1):
                    if start_index + i < len(df):
                        df.at[start_index + i, 'signal'] += i * weight_increment

            elif trade_direction == '-':
                weight_increment = (df.at[start_index, 'signal'] - init_position_weight) / duration_rows

                for i in range(duration_rows + 1):
                    if start_index + i < len(df):
                        df.at[start_index + i, 'signal'] -= i * weight_increment
            else:
                print(f"未知的交易方向: {trade_direction}")
                continue

            last_updated_index = min(start_index + duration_rows, len(df) - 1)
            last_value = df.at[last_updated_index, 'signal']
            df.loc[last_updated_index:, 'signal'] = last_value

        self.linear_signals = df['signal']
        return self.linear_signals


if __name__ == "__main__":

    df = Signalscalculate.linear_full_signals(0.4)
    pd.set_option('display.max_rows', None)
    print(df)

