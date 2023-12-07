## zlt-

#### components_information_acquisition.py 包含
##### 1. 指定日期当天最新成分股def components_ac  2. 获得成分股的WIND一级行业分布 def industry_distrubution 3. 判断板块分布功能 def market_distribution 4. ST, *ST 成分股剔除 def st_remove 5. 去除不满上市不满n天的成分股 def remove_nomore_n_days 6. 去除指定成分股 def remove_from_list 7. 添加指定股票 def add_elements

### 项目一：指数以及行业分布柱状图

#### . 输出一个csv/excel 类型的文件，分为三列： 行业代码、行业名称、标的数量、标的占比、行业指数当天涨跌
##### components_information_acquisition.py ---def components_ac -> def industry_distrubution ->indusry_market_plt.py def single_index_industry_plt
##### components_information_acquisition.py ---def components_ac -> rate_or_return.py def components_single_date_industry_ror


### 项目二：输入开始日期，截止日期和指数代码：计算等权的指数区间交易日内行情的涨跌，当天涨跌、隔夜涨跌、日内涨跌
##### index_ROR.py

股票复权价格是一种对股票价格进行调整的方法，旨在消除除权除息对股价带来的影响，使得投资者可以更客观地分析股票的价格走势。在股票市场中，公司会定期进行除权除息操作，例如派发现金股利、进行股票拆细或合并等。这些操作会对股票的价格产生影响，导致价格出现突变。

为了便于投资者对股票价格进行比较和分析，需要对股票价格进行复权处理，即将股票历史价格调整为考虑除权除息因素后的价格，以保持连续性和可比性。常见的复权价格类型包括：

前复权价格（前向复权价格）：以股票除权日为基准，将除权日之前的价格按照除权因子进行调整，使得除权日之前的价格与除权日当天的价格相一致。前复权价格反映了投资者在除权前的实际买入成本和收益情况。
后复权价格（后向复权价格）：以股票除权日为基准，将除权日之后的价格按照除权因子进行调整，使得除权日之后的价格与除权日当天的价格相一致。后复权价格反映了投资者在除权后的实际买入成本和收益情况。

##### 日内涨跌：今收-今开        
##### 隔天涨跌：今开-昨收       
##### 当天涨跌：今收-昨收

### 项目三. 对比特定的两个指数的一级行业分布区别情况
##### appoint_double_index_plt.py

### utilities
##### connect.py
##### fuzzy_s.py
##### nearest_trading_day.py
#### 所有的引用文件在reference中，结果保存在result
