## zlt-
#### 所有的引用文件在reference中，结果保存在result

### components_information_acquisition.py 包含
1. 指定日期当天最新成分股 def components_ac  
2. 获得成分股的WIND一级行业分布 def industry_distrubution
3. 判断板块分布功能 def market_distribution
4. ST, *ST 成分股剔除 def st_remove
5. 去除不满上市不满n天的成分股 def remove_nomore_n_days
6. 去除指定成分股 def remove_from_list
7. 添加指定股票 def add_elements
8. 获取上证50当天最新自由流通股本 def sh50_components_freeshare

### handle_inday_data.py 包含
处理过的quote数据进行handle_inday_data.py 处理3s采样，加入权重

### backtest.py 包含
回测框架，详见项目五

### rate_or_return.py 包含
1. 判断日期是否为交易日 def check_trade_da
2. 成分股捏合指数单日收益率计算 def sh50_components_single_date_ror (inday解决 overnight，day因为自由流通股本问题尚未解决)
3. 成分股对应行业的收益率 def components_single_date_industry_ror
4. 指数单日收益率计算 def index_single_date_ror
5. 如果wind数据库更新维护成功可以执行如下代码来更新自由流通股本 def components_capital_information （文件中已经注释处理）
6. 同上如果wind表格更新成功可以采用如下方法 def change （文件中已经注释处理）

### industry_market_plt.py 包含
1. 行业中文名称和CODE映射code--name def code_to_name
2. 单宽基指数、定制指数wind以及行业分布画图 def single_index_industry_plt
3. 双指数wind一级行业对比，可定制指数，也可以为宽基指数，取决于df的输入 def double_index_industry_plt
4. 成分股市场分布 def market_plt

### nearest_trading_day.py 包含
1. 找到最近交易日期 def find_nearest_trading_day
2. 找到区间内的交易日 def calculate_trade_days

### quote_information.py 包含
1. 赋予市场权重 def assign_market_weight
2. 保存quote def save_to_csvf
3. 上证50靠档 def process_rate （如果自由流通股表格实时更新可以启用）
4. 计算成分股权重 def components_weight （分级靠档后计算调整股本，同上）
5. 预处理quote文件 def pre_quote_information
6. 自由流通市值加权 def weight_prc
7. 整合成分股quote数据 def final_to_plt
8. 画图 def quote_plt
9. 比较指数和捏合指数三秒行情收益率差异 def indat_ror_comparison


### 项目一：指数以及行业分布柱状图

#### 输出一个csv/excel 类型的文件，分为三列： 行业代码、行业名称、标的数量、标的占比、行业指数当天涨跌
1. components_information_acquisition.components_ac
2. components_information_acquisition.industry_distrubution ->indusry_market_plt.single_index_industry_plt
3. components_information_acquisition.components_ac -> rate_or_return.components_single_date_industry_ror


### 项目二：输入开始日期，截止日期和指数代码：计算等权的指数区间交易日内行情的涨跌，当天涨跌、隔夜涨跌、日内涨跌
1. rate_or_return.index_single_date_ror

股票复权价格是一种对股票价格进行调整的方法，旨在消除除权除息对股价带来的影响，使得投资者可以更客观地分析股票的价格走势。在股票市场中，公司会定期进行除权除息操作，例如派发现金股利、进行股票拆细或合并等。这些操作会对股票的价格产生影响，导致价格出现突变。
为了便于投资者对股票价格进行比较和分析，需要对股票价格进行复权处理，即将股票历史价格调整为考虑除权除息因素后的价格，以保持连续性和可比性。常见的复权价格类型包括：
前复权价格（前向复权价格）：以股票除权日为基准，将除权日之前的价格按照除权因子进行调整，使得除权日之前的价格与除权日当天的价格相一致。前复权价格反映了投资者在除权前的实际买入成本和收益情况。
后复权价格（后向复权价格）：以股票除权日为基准，将除权日之后的价格按照除权因子进行调整，使得除权日之后的价格与除权日当天的价格相一致。后复权价格反映了投资者在除权后的实际买入成本和收益情况。
日内涨跌：今收-今开        
隔天涨跌：今开-昨收       
当天涨跌：今收-昨收


### 项目三. 对比特定的两个指数的一级行业分布区别情况
1. components_information_acquisition.components_ac
2. components_information_acquisition.components_ac
3. indusry_market_plt.double_index_industry_plt


### 项目四：定制化指数与定制化指数的日内行情
1. components_information_acquisition.components_ac
   1.1 components_information_acquisition.industry_distrubution
   1.2 st_remove
   1.3 remove_nomore_n_days
   1.4 remove_from_list
   1.5 add_elements
2. components_information_acquisition.industry_distrubution
3. industry_market_plt.double_index_industry_plt
4. quote.information.save_to_csvf
   4.1 pre_quote_information
   4.2 capital_today_df = sh50_components_freeshare
   4.3 weight_prc
   4.4 final_to_plt
   4.5 quote_plt

### 项目五：日内择时回测框架
收益类 1. Alpha 收益
        a. 单日平均收益率
        b. 年化累积收益率
      2. 绝对收益
        a. 单日平均
        b. 年化累积
风险类
      1. 夏普比例
      2. 索提诺比率
      3. 最大回撤
      4. 最大回撤持续时间
1. handle_inday_data.py
2. backtest.py


## 本次更新：
class类，connect_class, components_class 
修复industry_market_plt.py， components_information_acquisition.py， nearest_trading_day.py内所有发现错误

