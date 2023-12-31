import time
import pandas as pd
import numpy as np
from pyecharts import options as opts
from pyecharts.charts import Line, Bar, Grid

from v1_code.nearest_trading_day import calculate_trade_days
from v1_code.handle_inday_data import resample_3s, weight_martrix

from v2_code.connect import connect_to_database, wind
from v2_code.utilities_class import Tradedates, Datapreprocess, Plot


class IndexBacktest:
    def __init__(self, weight_data, initial_investment_weight, benchmark, rf_rate=0.04, trade_tax=0.0003,
                 stamp_tax=0.0005, ):
        self.weight_data = weight_data
        self.trade_tax = trade_tax
        self.stamp_tax = stamp_tax
        self.initial_investment_weight = initial_investment_weight
        self.rf_rate = rf_rate
        self.signals = None
        self.results = None
        self.metrics = None
        self.benchmark = benchmark

    def linear_full_signals(self):
        df = pd.read_csv('reference/timestamp_standard.csv')
        columns = ['Start_time', 'Duration', 'Trade_direction']
        df1 = pd.DataFrame(columns=columns)
        init_position_weight = float(self.initial_investment_weight)
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
        self.signals = df['signal']

    def generate_portfolio(self):
        if self.signals is None:
            self.linear_full_signals()

        # rm benchamrk 基准收益
        portfolio_rm = self.weight_data
        # portfolio_rm.columns = self.weight_data.columns
        portfolio_rm.columns = [col[7:] if len(col) > 7 else '' for col in portfolio_rm.columns]
        portfolio_rm = portfolio_rm.add_prefix('rm_')
        portfolio_rm['rm_sum'] = portfolio_rm.sum(axis=1)
        portfolio_rm.fillna(0, inplace=True)
        if self.benchmark is None:
            self.benchmark = portfolio_rm['rm_sum'].iloc[0]
        # print(portfolio_rm)

        # 组合收益计算 ---abs_weight 计算
        signals_temp = pd.concat([self.signals] * 50, axis=1)
        # print(self.signals)
        # print(signals_temp)
        portfolio_r_array = self.weight_data.values * signals_temp.values
        portfolio_r = pd.DataFrame(portfolio_r_array)
        portfolio_r.columns = self.weight_data.columns
        portfolio_r = portfolio_r.add_prefix('abs_')
        # print(portfolio_r)

        # 计算实际交易信号--手续费
        signals_adj = self.signals.where(self.signals == self.signals.shift(), self.signals.shift())
        signals_adj.iloc[0] = self.signals.iloc[0]
        signals_adj_temp = pd.concat([signals_adj] * 50, axis=1)
        adj_abs_array = signals_adj_temp.values * self.weight_data.values
        adj_abs = pd.DataFrame(adj_abs_array)
        adj_abs.columns = self.weight_data.columns
        adj_abs = adj_abs.add_prefix('adj_abs_')
        # print(adj_abs)

        trade_volumn_array = portfolio_r.values - adj_abs.values
        trade_volumn = pd.DataFrame(trade_volumn_array)
        trade_volumn.columns = self.weight_data.columns
        trade_volumn = trade_volumn.add_prefix('trade_volumn_')
        # print(trade_volumn)

        ask = (trade_volumn.abs() - trade_volumn) / 2
        ask.columns = self.weight_data.columns
        ask = ask.add_prefix('ask_')
        ask['stamp_tax'] = ask.sum(axis=1) * self.stamp_tax
        ask['ask_trade_tax'] = ask.sum(axis=1) * self.trade_tax
        ask['ask_revenue'] = ask.sum(axis=1)
        # print(ask)

        bid = (trade_volumn.abs() + trade_volumn) / 2
        bid.columns = self.weight_data.columns
        bid = bid.add_prefix('bid_')
        bid['bid_trade_tax'] = bid.sum(axis=1) * self.trade_tax
        bid['bid_cost'] = bid.sum(axis=1)
        # print(bid)

        portfolio_r['r_sum'] = portfolio_r.sum(axis=1)
        portfolio_return = pd.concat([portfolio_r['r_sum'], portfolio_rm['rm_sum'],
                                      ask['stamp_tax'], ask['ask_trade_tax'], ask['ask_revenue'],
                                      bid['bid_trade_tax'], bid['bid_cost']], axis=1)

        portfolio_return['r_return'] = ((portfolio_return['r_sum']).diff()
                                        - portfolio_return['bid_trade_tax'] - portfolio_return['bid_cost']
                                        - portfolio_return['ask_trade_tax'] + portfolio_return['ask_revenue']
                                        - portfolio_return['stamp_tax']) / self.benchmark
        portfolio_return['r_return'].fillna(0, inplace=True)

        portfolio_return['rm_return'] = portfolio_rm['rm_sum'].diff() / self.benchmark
        portfolio_return['rm_return'].fillna(0, inplace=True)

        data_summary = pd.concat([portfolio_rm, portfolio_r, adj_abs, trade_volumn, ask, bid], axis=1)
        # 日内整合数据
        # print(data_summary)
        # data_summary.to_csv('./datasummary.csv')

        self.results = portfolio_return
        return portfolio_return, data_summary, self.benchmark

    def calculate_performance_metrics(self):

        inday_return_benchmark = self.results['rm_return'].sum()
        inday_return_strategy = self.results['r_return'].sum()

        inday_alpha = self.results['r_return'].sum() - self.results['rm_return'].sum()

        inday_volatility_benchmark = self.results['r_return'].std()
        inday_volatility_strategy = self.results['rm_return'].std()

        self.metrics = {
            'inday_return_benchmark': inday_return_benchmark,
            'inday_return_strategy': inday_return_strategy,
            'inday_alpha': inday_alpha,
            'inday_volatility_benchmark': inday_volatility_benchmark,
            'inday_volatility_strategy': inday_volatility_strategy
        }

        return {
            'inday_return_benchmark': inday_return_benchmark,
            'inday_return_strategy': inday_return_strategy,
            'inday_alpha': inday_alpha,
            'inday_volatility_benchmark': inday_volatility_benchmark,
            'inday_volatility_strategy': inday_volatility_strategy
        }

    def plot_cumulative_returns(self, date):

        # 假设这是两个y轴的数据
        y_data_1 = (1 + self.results['r_return']).cumprod()  # 策略收益的累积回报
        y_data_2 = (1 + self.results['rm_return']).cumprod()  # 基准收益的累积回报
        y_bar_data = self.signals.tolist()

        # 确定Y轴的范围
        y_line_min = round((min(y_data_1.min(), y_data_2.min()) - 0.002), 3)
        y_line_max = round((max(y_data_1.max(), y_data_2.max()) + 0.002), 3)
        y_bar_min = 0
        y_bar_max = 1

        datazoom_opts = [
            opts.DataZoomOpts(is_show=True, xaxis_index=[0, 1]),
            opts.DataZoomOpts(type_="inside", xaxis_index=[0, 1])
        ]

        # 创建折线图实例
        line = Line()
        # 添加x轴和y轴数据
        line.add_xaxis(xaxis_data=self.results.index.tolist())
        line.add_yaxis(
            series_name="策略累计回报",
            y_axis=(self.results['r_return'] + 1).cumprod(),
            is_smooth=True,
            linestyle_opts=opts.LineStyleOpts(width=2),
            label_opts=opts.LabelOpts(is_show=False),
        )
        line.add_yaxis(
            series_name="基准累计回报",
            y_axis=(self.results['rm_return'] + 1).cumprod(),
            is_smooth=True,
            linestyle_opts=opts.LineStyleOpts(width=2),
            label_opts=opts.LabelOpts(is_show=False),
        )

        # 创建柱状图实例
        bar = Bar()
        # 添加x轴和y轴数据
        bar.add_xaxis(xaxis_data=self.results.index.tolist())
        bar.add_yaxis(
            series_name="信号",
            y_axis=y_bar_data,
            label_opts=opts.LabelOpts(is_show=False)  # 设置不显示标签
        )

        # 设置折线图的全局选项
        line.set_global_opts(
            title_opts=opts.TitleOpts(title=f"{date}_累计回报对比"),
            tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
            legend_opts=opts.LegendOpts(pos_left="right"),
            datazoom_opts=datazoom_opts,
            yaxis_opts=opts.AxisOpts(type_="value", min_=y_line_min, max_=y_line_max),
        )

        # 设置柱状图的全局选项，注意不显示x轴，因为将会和折线图共用
        bar.set_global_opts(
            tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
            legend_opts=opts.LegendOpts(pos_left="middle"),
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(is_show=False)),
            yaxis_opts=opts.AxisOpts(type_="value", min_=y_bar_min, max_=y_bar_max),
            datazoom_opts=datazoom_opts
        )

        # 创建 Grid 对象
        grid_chart = Grid()

        grid_chart.add(
            bar,
            grid_opts=opts.GridOpts(
                pos_left="5%",
                pos_right="5%",
                pos_top="75%",
                height="20%"
            ),
        )

        grid_chart.add(
            line,
            grid_opts=opts.GridOpts(
                pos_left="5%",
                pos_right="5%",
                pos_top="10%",
                height="60%"
            ),
        )

        # 渲染图表到文件
        grid_chart.render(f"{date}_combined_chart.html")

        return y_data_1, y_data_2, self.signals

    def run_backtest(self):
        if self.metrics is None:
            self.calculate_performance_metrics()
        if self.metrics:
            print(f"Inday_Return Benchmark: {self.metrics['inday_return_benchmark']}")
            print(f"Inday_Alpha: {self.metrics['inday_alpha']}")
            print(f"Inday_Volatility_benchmark: {self.metrics['inday_volatility_benchmark']}")
            print(f"Inday_Return_strategy: {self.metrics['inday_return_strategy']}")
            print(f"Inday_volatility_strategy: {self.metrics['inday_volatility_strategy']}")


def calculate_metrics(combined_df, trading_date):
    n_days = len(trading_date)

    benchmark_return_mean = combined_df.loc['inday_return_benchmark'].mean()
    annualized_return_benchmark = (1 + benchmark_return_mean) ** (252 / n_days) - 1
    r_return_mean = combined_df.loc['inday_return_strategy'].mean()
    annualized_return_strategy = (1 + r_return_mean) ** (252 / n_days) - 1
    benchmark_volatility_mean = combined_df.loc['inday_volatility_benchmark'].mean()
    annualized_volatility_benchmark = benchmark_volatility_mean * np.sqrt(252 / n_days)
    r_volatility_mean = combined_df.loc['inday_volatility_strategy'].mean()
    annualized_volatility_stragtegy = r_volatility_mean * np.sqrt(252 / n_days)

    sharpe_ratio = (r_return_mean / r_volatility_mean) * np.sqrt(252 / n_days)

    downside_returns = combined_df.loc['inday_alpha'][combined_df.loc['inday_alpha'] < 0]
    downside_deviation = downside_returns.std()

    sortino_ratio = r_return_mean / downside_deviation

    metrics = {
        'annualized_return_benchmark': annualized_return_benchmark,
        'annualized_volatility_benchmark': annualized_volatility_benchmark,
        'sharpe_ratio': sharpe_ratio,
        'annualized_return_strategy': annualized_return_strategy,
        'sortino_ratio': sortino_ratio,
    }

    # 初始化一个新的 Series 来存储计算的差值，第一列设为 None 或 pd.NA
    r_overnight = pd.Series([pd.NA] * len(combined_df.columns), index=combined_df.columns)
    r_inday = pd.Series([pd.NA] * len(combined_df.columns), index=combined_df.columns)
    r_day = pd.Series([pd.NA] * len(combined_df.columns), index=combined_df.columns)

    rm_overnight = pd.Series([pd.NA] * len(combined_df.columns), index=combined_df.columns)
    rm_inday = pd.Series([pd.NA] * len(combined_df.columns), index=combined_df.columns)
    rm_day = pd.Series([pd.NA] * len(combined_df.columns), index=combined_df.columns)

    # 从第二列开始遍历 DataFrame 的列
    for i in range(len(combined_df.columns)):
        initial_col = combined_df.columns[0]
        current_col = combined_df.columns[i]
        previous_col = combined_df.columns[i - 1] if i > 0 else None

        # 如果不是第一列，则执行计算
        if previous_col is not None:
            r_overnight[current_col] = ((combined_df.at['r_start', current_col] - combined_df.at[
                'r_end', previous_col]) / 2) / combined_df.at['rm_start', initial_col]
            r_inday[current_col] = (combined_df.at['r_end', current_col] - combined_df.at['r_start', current_col]) \
                / combined_df.at['rm_start', initial_col]
            r_day[current_col] = (combined_df.at['r_end', current_col] - combined_df.at['r_end', previous_col]) \
                / combined_df.at['rm_start', initial_col]

            rm_overnight[current_col] = ((combined_df.at['rm_start', current_col] - combined_df.at[
                'rm_end', previous_col]) / 2) / combined_df.at['rm_start', initial_col]
            rm_inday[current_col] = (combined_df.at['rm_end', current_col] - combined_df.at['rm_start', current_col]) \
                / combined_df.at['rm_start', initial_col]
            rm_day[current_col] = (combined_df.at['rm_end', current_col] - combined_df.at['rm_end', previous_col]) \
                / combined_df.at['rm_start', initial_col]
        else:
            # 如果是第一列，则赋值为空值
            r_overnight[current_col] = np.nan
            r_inday[current_col] = combined_df.at['r_end', current_col] / combined_df.at['r_start', current_col] - 1
            r_day[current_col] = np.nan

            rm_overnight[current_col] = np.nan
            rm_inday[current_col] = combined_df.at['rm_end', current_col] / combined_df.at['rm_start', current_col] - 1
            rm_day[current_col] = np.nan

        combined_df.loc['r_overnight'] = r_overnight
        combined_df.loc['r_inday'] = r_inday
        combined_df.loc['r_day'] = r_day
        combined_df.loc['rm_overnight'] = rm_overnight
        combined_df.loc['rm_inday'] = rm_inday
        combined_df.loc['rm_day'] = rm_day

    print(combined_df)
    print(metrics)

    return combined_df, metrics


def calculate_max_drawdown_and_duration(net_values):
    # 转换净值为最大回撤计算所需的累计最高净值
    peak = net_values[0]
    max_drawdown = 0
    max_drawdown_duration = 0
    duration_since_peak = 0

    for value in net_values:
        # 如果当前净值高于之前的峰值，则更新峰值
        if value > peak:
            peak = value
            # 由于达到新高，持续时间重置
            duration_since_peak = 0
        else:
            # 计算当前的回撤
            drawdown = (peak - value) / peak
            duration_since_peak += 1
            # 如果当前回撤大于之前记录的最大回撤，则更新最大回撤和持续时间
            if drawdown > max_drawdown:
                max_drawdown = drawdown
                max_drawdown_duration = duration_since_peak

    return max_drawdown, max_drawdown_duration


def mdd_and_alldayplt(df1, df2, df3, df4, metrics):
    overnight_index1 = df2.index.get_loc('r_overnight')
    for i in range(1, len(df1.columns)):
        # 获取 df2 中相应列的 'r_overnight' 行的值
        multiplier1 = df2.iloc[overnight_index1, i] + 1
        # 确保 multiplier 是一个数值，如果不是，可以在这里添加额外的逻辑处理
        if pd.notna(multiplier1) and isinstance(multiplier1, (int, float)):
            # 更新 df1 中的值
            df1.iloc[0, i] = df1.iloc[-1, i - 1] * multiplier1

    overnight_index2 = df2.index.get_loc('rm_overnight')
    for i in range(1, len(df3.columns)):
        # 获取 df2 中相应列的 'r_overnight' 行的值
        multiplier2 = df2.iloc[overnight_index2, i] + 1
        # 确保 multiplier 是一个数值，如果不是，可以在这里添加额外的逻辑处理
        if pd.notna(multiplier2) and isinstance(multiplier2, (int, float)):
            # 更新 df1 中的值
            df3.iloc[0, i] = df3.iloc[-1, i - 1] * multiplier2

    df1_list = df1.T.values.flatten().tolist()
    r_mdd, r_mdd_time = calculate_max_drawdown_and_duration(df1_list)
    print("策略日内最大回撤为", r_mdd)
    print("最大日内回撤周期为", r_mdd_time*3, "s")

    df3_list = df3.T.values.flatten().tolist()
    rm_mdd, rm_mdd_time = calculate_max_drawdown_and_duration(df3_list)
    print("基准策略日内最大回撤为", rm_mdd)
    print("日内最大回撤周期为", rm_mdd_time * 3, "s")

    metrics['Strategy_MDD'] = r_mdd
    metrics['Benchmark_MDD'] = rm_mdd
    metrics['Strategy_MDD_Time'] = str(r_mdd_time*3) + 's'
    metrics['Benchmark_MDD_Time'] = str(rm_mdd_time * 3) + 's'

    signals_list = df4.T.values.flatten().tolist()

    # # 确定Y轴的范围
    y_line_min = round((min(min(df1_list), min(df3_list)) - 0.002), 3)
    y_line_max = round((max(max(df1_list), max(df3_list)) + 0.002), 3)
    y_bar_min = 0
    y_bar_max = 1
    #
    datazoom_opts = [
        opts.DataZoomOpts(is_show=True, xaxis_index=[0, 1]),
        opts.DataZoomOpts(type_="inside", xaxis_index=[0, 1])
    ]
    x_labels = [str(i % 4802) for i in range(len(df1_list))]
    # 创建折线图实例
    line = Line()
    # 添加x轴和y轴数据
    line.add_xaxis(list(range(len(df1_list))))
    line.add_yaxis(
        series_name="策略累计回报",
        y_axis=df1_list,
        is_smooth=True,
        linestyle_opts=opts.LineStyleOpts(width=2),
        label_opts=opts.LabelOpts(is_show=False),
    )
    line.add_yaxis(
        series_name="基准累计回报",
        y_axis=df3_list,
        is_smooth=True,
        linestyle_opts=opts.LineStyleOpts(width=2),
        label_opts=opts.LabelOpts(is_show=False),
    )

    # 创建柱状图实例
    bar = Bar()
    # 添加x轴和y轴数据
    bar.add_xaxis(list(range(len(signals_list))))
    bar.add_yaxis(
        series_name="信号",
        y_axis=signals_list,
        label_opts=opts.LabelOpts(is_show=False)  # 设置不显示标签
    )

    # 设置折线图的全局选项
    line.set_global_opts(
        title_opts=opts.TitleOpts(title="累计回报对比"),
        tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
        legend_opts=opts.LegendOpts(pos_left="right"),
        datazoom_opts=datazoom_opts,
        yaxis_opts=opts.AxisOpts(type_="value", min_=y_line_min, max_=y_line_max),
    )

    # 设置柱状图的全局选项，注意不显示x轴，因为将会和折线图共用
    bar.set_global_opts(
        tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
        legend_opts=opts.LegendOpts(pos_left="middle"),
        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(is_show=False)),
        yaxis_opts=opts.AxisOpts(type_="value", min_=y_bar_min, max_=y_bar_max),
        datazoom_opts=datazoom_opts
    )

    # 创建 Grid 对象
    grid_chart = Grid()

    grid_chart.add(
        bar,
        grid_opts=opts.GridOpts(
            pos_left="5%",
            pos_right="5%",
            pos_top="75%",
            height="20%"
        ),
    )

    grid_chart.add(
        line,
        grid_opts=opts.GridOpts(
            pos_left="5%",
            pos_right="5%",
            pos_top="10%",
            height="60%"
        ),
    )

    # 渲染图表到文件
    grid_chart.render("3s_combined_chart.html")

    # 定义自定义文本的 HTML
    custom_text_html = f"""
        <div style="margin-right: 50px; flex: 1;">
            <p>基准年化回报: {metrics['annualized_return_benchmark']}</p>
            <p>基准年化波动率: {metrics['annualized_volatility_benchmark']}</p>
            <p>夏普比率: {metrics['sharpe_ratio']:}</p>
            <p>策略年化回报: {metrics['annualized_return_strategy']}</p>
            <p>索提诺比率: {metrics['sortino_ratio']}</p>
            <p>策略最大回撤为: {metrics['Strategy_MDD']}</p>
            <p>基准策略最大回撤为: {metrics['Benchmark_MDD']}</p>
            <p>策略最大回撤周期为: {metrics['Strategy_MDD_Time']}</p>
            <p>基准策略最大回撤周期为: {metrics['Benchmark_MDD_Time']}</p>
        </div>
    """

    # 假设您已经通过pyecharts生成了图表，并且已经渲染到 "chart.html" 文件中
    chart_html_filename = "result/html_chart/3s_combined_chart.html"
    # 读取图表的HTML内容
    with open(chart_html_filename, "r", encoding="utf-8") as file:
        chart_html_content = file.read()

    chart_html_content = chart_html_content.replace("</body>", custom_text_html + "</body>")

    # 将修改后的内容写回到文件中
    with open("combined_chart.html", "w", encoding="utf-8") as file:
        file.write(chart_html_content)

    print(df1)
    print(df3)




if __name__ == "__main__":
    conn = connect_to_database(wind)
    # 记录程序开始时间
    p_start_time = time.time()
    start_date = int(input('请输入开始时间：'))
    end_date = int(input('请输入结束时间：'))
    trading_date = calculate_trade_days(start_date, end_date, conn)
    dfs = []
    r_data = []
    rm_data = []
    signals_data = []
    benchmark_value = None
    initial_investment_weight = 0.4
    for date in trading_date:
        print(f'{date}: ')
        resample_3s(date)
        weight_martrix(date)
        weight_data = pd.read_csv(f'./quote/{date}_detail/weight_matrix.csv')
        weight_data.drop(weight_data.columns[0], axis=1, inplace=True)
        weight_data = weight_data.add_prefix('weight_')
        backtest = IndexBacktest(weight_data, initial_investment_weight, benchmark_value)

        backtest.linear_full_signals()
        # _ 定义日内全部交易
        daily_portfolio, _, benchmark = backtest.generate_portfolio()
        print(daily_portfolio)
        if benchmark_value is None:
            benchmark_value = benchmark
        else:
            pass
        daily_metrics = backtest.calculate_performance_metrics()
        backtest.run_backtest()
        # 当日的所有行情数据自动生成html画图
        datar, datarm, signals = backtest.plot_cumulative_returns(date)
        daily_dict = {'r_start': daily_portfolio['r_sum'][0], 'r_end': daily_portfolio['r_sum'].iloc[-1],
                      'r_min': daily_portfolio['r_sum'].min(), 'r_max': daily_portfolio['r_sum'].max(),
                      'rm_start': daily_portfolio['rm_sum'][0], 'rm_end': daily_portfolio['rm_sum'].iloc[-1],
                      'rm_min': daily_portfolio['rm_sum'].min(), 'rm_max': daily_portfolio['rm_sum'].max(),
                      'inday_return_benchmark': daily_metrics['inday_return_benchmark'],
                      'inday_return_strategy': daily_metrics['inday_return_strategy'],
                      'inday_volatility_benchmark': daily_metrics['inday_volatility_benchmark'],
                      'inday_volatility_strategy': daily_metrics['inday_volatility_strategy'],
                      'inday_alpha': daily_metrics['inday_alpha']}
        df = pd.DataFrame.from_dict(daily_dict, orient='index', columns=[f'{date}'])
        dfs.append(df)
        r_data.append(datar)
        rm_data.append(datarm)
        signals_data.append(signals)
        # 将新的DataFrame拼接到已有的DataFrame上
    combined_df = pd.concat(dfs, axis=1)
    r_data_df = pd.concat(r_data, axis=1)
    rm_data_df = pd.concat(rm_data, axis=1)
    signals_df = pd.concat(signals_data, axis=1)

    dfc, metrics = calculate_metrics(combined_df, trading_date)

    df_r, df_rm = mdd_and_alldayplt(r_data_df, dfc, rm_data_df, signals_df, metrics)
    print(df_r)
    print(df_rm)

    conn.close()
    # 记录程序结束时间
    end_time = time.time()
    # 计算运行时间
    run_time = end_time - p_start_time
    print("程序运行时间：", run_time, "秒")
