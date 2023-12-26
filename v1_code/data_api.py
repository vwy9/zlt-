import pandas as pd
import pymssql
import pymongo
import pymysql
import datetime
from sqlalchemy import create_engine
import os
import numpy as np
import json
import datetime
import traceback

class BaseData():
    def __init__(self):
        self.mongo_username = "zlt01"
        self.mongo_password = "zlt_ujYH"
        self.mongo_host = "mongodb://zlt01:zlt_ujYH@192.168.9.189:15009/data"
        self.local_mongo_host = "mongodb://quoteread:quoteread.800@192.168.9.110:27027/quote_db"

        self.sql_user = 'chuangXin'
        self.sql_pwd = 'Xini.100'
        self.sql_host = '192.168.9.85'
        self.sql_db = 'option_new'

        self.wind_username = 'quantchina'
        self.wind_password = 'zMxq7VNYJljTFIQ8'
        self.wind_host = '192.168.7.93'
        self.wind_port = 3306

        self.data_path =  "//nas92/data/"

        file_name = 'input_data_config.json'
        if os.path.exists(file_name):
            with open(file_name,encoding='utf-8') as f:
                json_data = json.load(f)
            if "mongo_username" in json_data:
                self.mongo_username = json_data["mongo_username"]            
                self.mongo_password = json_data["mongo_password"]
            if "mongo_host" in json_data:
                self.mongo_host = json_data["mongo_host"]
            if "local_mongo_host" in json_data:
                self.local_mongo_host = json_data["local_mongo_host"]

            if "wind_username" in json_data:
                self.wind_username = json_data["wind_username"]            
                self.wind_password = json_data["wind_password"]            
                self.wind_host = json_data["wind_host"]            
                self.wind_port = json_data["wind_port"]

            if "tick_minbar_path" in json_data:
                self.data_path = json_data["tick_minbar_path"]

        file_name = 'output_data_config.json'
        self.mongo_output_addr = None
        self.mongo_output_db = ''
        if os.path.exists(file_name):
            with open(file_name,encoding='utf-8') as f:
                json_data = json.load(f)
            if "sql_host" in json_data:
                self.sql_host = json_data["sql_host"]            
                self.sql_db = json_data["sql_db"]           
                self.sql_user = json_data["sql_user"]           
                self.sql_pwd = json_data["sql_pwd"]
            if "mongo_host" in json_data:
                mongo_host = json_data["mongo_host"]
                mongo_db = json_data["mongo_db"]
                mongo_user = json_data["mongo_user"]
                mongo_pwd = json_data["mongo_pwd"]
                
                self.mongo_output_addr = f"mongodb://{mongo_user}:{mongo_pwd}@{mongo_host}/{mongo_db}"
                self.mongo_output_db = mongo_db

    def mssql_select(self,sql,database = None,host = None,username = None,password= None):
        '''查询mssql类型的数据库,默认使用85数据库'''

        if database == None:
            database = self.sql_db
        if host == None:
            host = self.sql_host
        if username == None:
            username = self.sql_user
        if password == None:
            password = self.sql_pwd        
        conn = pymssql.connect(host, username,password,database)
        # engine = create_engine('mysql+pymysql://%s:%s@%s/%s?charset=utf8'%(username, password, host, database))
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    
    def mssql_execute(self,sql,database = None,host = None,username = None,password= None):
        '''在mssql类型的数据库执行 类似delete 开头的sql 语句,默认使用85数据库'''

        if database == None:
            database = self.sql_db
        if host == None:
            host = self.sql_host
        if username == None:
            username = self.sql_user
        if password == None:
            password = self.sql_pwd        
        conn = pymssql.connect(host, username,password,database)
        with conn.cursor() as cursor:
            cursor.execute(sql)
            conn.commit()
        conn.close()

    
    def mysql_select(self,sql,database = 'wind', charset='gbk',data_type = 'dataframe' ,host = None, username = None, password = None):
        '''查询mysql类型的数据库,默认使用85数据库'''
        data = None
        if host == None:
            host = self.wind_host
        if username == None:
            username = self.wind_username
        if password == None:
            password = self.wind_password
        conn = pymysql.Connect(host = host,user = username,password = password,database = database,charset = charset)
        with conn.cursor() as cursor:
            cursor.execute(sql)
            data = cursor.fetchall()
            if data_type == 'dataframe':
                data = pd.DataFrame(list(data))
            
        conn.close()
        return data

    def get_mssql_engine(self,database = None,host = None,username = None,password= None):
        ''' 用于dataframe 向mssql类型的数据库写入数据，默认写入的数据库是85数据库'''

        if database == None:
            database = self.sql_db
        if host == None:
            host = self.sql_host
        if username == None:
            username = self.sql_user
        if password == None:
            password = self.sql_pwd 
        engine = create_engine("mssql+pymssql://{0}:{1}@{2}/{3}?charset=utf8".format(username, password, host, database))
        return engine

    def get_trading_day(self, start_date,end_date = None,exchange = None): #exchange:'SSE'/'SZSE'
        ''' 从万德数据库获取交易日'''

        if end_date == None:
            end_date = pd.to_datetime(datetime.date.today())
        if exchange == None:
            exchange = 'SSE'         
        sql = "SELECT TRADE_DAYS FROM ASHARECALENDAR where S_INFO_EXCHMARKET = '%s' and TRADE_DAYS >= '%s' and TRADE_DAYS <= '%s' order by TRADE_DAYS"%(exchange,start_date.strftime('%Y%m%d'),end_date.strftime('%Y%m%d'))
        df =self.mysql_select(sql)
        if len(df) > 0:
            df.columns = ['date']
            return df
        else:
            print("Error to get trading calendays")
    
    def get_cache_trading_day(self, start_date, end_date = None, 
                            cache_dir = "cache_data",using_cache = True,
                            cache_start_date = None, exchange = None): #exchange:'SSE'/'SZSE'

        ''' date foramt: %Y%m%d '''
        if cache_start_date is None:
            cache_start_date = '20220101'

        if cache_start_date > start_date:
            cache_start_date = start_date    

        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
            
        cachce_file = cache_dir + "/"+ cache_start_date + "_trade_day.parquet"  
        today_str =  datetime.datetime.now().strftime("%Y%m%d")    
        if end_date is None:
            end_date = today_str

        if exchange is None:
            exchange = 'SSE' 

        if not os.path.exists(cachce_file) or (not using_cache):
            sql = f"SELECT TRADE_DAYS FROM ASHARECALENDAR where S_INFO_EXCHMARKET = '{exchange}'"
            sql = sql + f" and TRADE_DAYS >= '{cache_start_date}' and TRADE_DAYS <= '{end_date}'"
            df = self.mysql_select(sql)
            df.columns = ['date']
            df.sort_values(by=['date'], ascending=True,inplace=True)           
            df.to_parquet(cachce_file)
        else:
            df = pd.read_parquet(cachce_file)
            record_date = df['date'].values[-1]            
            if today_str > record_date:
                sql = f"SELECT TRADE_DAYS FROM ASHARECALENDAR where S_INFO_EXCHMARKET = '{exchange}'"
                sql = sql + f" and TRADE_DAYS > '{record_date}' and TRADE_DAYS <= '{today_str}'"
                new_df = self.mysql_select(sql)
                new_df.columns = ['date']                     
                df = pd.concat([df,new_df])
                df = df.sort_values(by=['date'], ascending=True).reset_index(drop=True)
                df.to_parquet(cachce_file)
                
        if start_date is not None:
            df = df[df['date'] >= start_date]
        
        if end_date is not None:
            df = df[df['date'] <= end_date]
               
        trade_days = []
        if len(df) > 0:
            trade_days = list(df["date"])

        return trade_days
       

    def get_last_price(self,symbol: str):
        ''' 从mongo上获取标的的最新价'''

        myclient = pymongo.MongoClient(self.local_mongo_host)
        mydb = myclient.quote_db
        c = mydb["quote_data"]
        try:      
            price = c.find_one({"code_name": symbol})["last_price"]
            return price
        except:
            print("Input code is not in MangoDB datebase")

    def get_last_minbar(self, symbol:str, dt=None):
        ''' 从mongo上获取标的的最新的分钟bar'''

        myclient = pymongo.MongoClient(self.mongo_host)
        mydb = myclient["xz_signal"]
        c = mydb["sample_data"]
        today_int = int(datetime.datetime.now().strftime("%Y%m%d") + "093000")
        if dt is not None and dt < today_int:
            c = mydb["history_sample_data"]
        result_s = 0
        try:
            if dt:
                sample = c.find({"symbol": symbol, "datetime": dt})
            else:
                sample = c.find({"symbol": symbol})
            for s in sample:
                result_s = s
            return result_s
        except:
            print("Input code is not in MangoDB datebase")

    def get_mongo_daily_minbar(self,date,symbol:str):
        """

        :param date: "%Y%m%d%H%M%S" str格式
        :param symbol:
        :return: df
        """
        
        date_int = int(date)
        myclient = pymongo.MongoClient(self.mongo_host)
        mydb = myclient["xz_signal"]
        c = mydb["sample_data"]

        res = []
        sample = c.find({"symbol": symbol, "datetime": {'$gt':date_int}})
        for s in sample:
            res.append(s)
        result_df = pd.DataFrame(res)   
        return result_df

    def get_signal_mongo_daily_minbar(self,symbol:str = None,date = None):
        """

        :param date: "%Y%m%d%H%M%S" str格式
        :param symbol:
        :return: df
        """
        if date is None:
            cur_str = datetime.datetime.now().strftime('%Y%m%d')
            date_int = int(cur_str+'093000')
        else:
            date_int = int(date)
        myclient = pymongo.MongoClient(self.mongo_host)
        mydb = myclient["xz_signal"]
        c = mydb["signal_data"]

        res = []
        if symbol is None:
            sample = c.find({"datetime": {'$gt':date_int}})
        else:
            sample = c.find({"symbol": symbol, "datetime": {'$gt':date_int}})
        for s in sample:
            res.append(s)
        result_df = pd.DataFrame(res)   
        return result_df

    def get_mongo_daily_minbar_by_list(self,date,symbols:list):
        """

        :param date: "%Y%m%d%H%M%S" str格式
        :param symbol:
        :return: df
        """
        
        date_int = int(date)
        myclient = pymongo.MongoClient(self.mongo_host)
        mydb = myclient["xz_signal"]
        c = mydb["sample_data"]

        res = []
        sample = c.find({"symbol": {'$in':symbols}, "datetime": {'$gt':date_int}})
        for s in sample:
            res.append(s)
        result_df = pd.DataFrame(res)   
        return result_df    

    def get_last_price_by_list(self,symbols: list):

        myclient = pymongo.MongoClient(self.local_mongo_host)
        mydb = myclient.quote_db
        c = mydb["quote_data"]
        try: 
            price_list = {}     
            option_data = c.find({"code_name": {"$in":symbols}})
            for i in option_data:
                price_list[i['code_name']] = i["last_price"]
            return price_list
        except:
            print(traceback.format_exc())
            print("Input code is not in MangoDB datebase")

    def get_minbar_data(self,symbol,date,symbol_type = "future"):
        """
        从datahouse中获取分钟bar，期权的数据在股票类里
        :param symbol: str,format like IF2109.CFE or 600001.SH
        :param date : str,format like '20210907'
        :param symbol_type : str, must be 'future' or 'stock'
        :return: dataframe
        """
        df = pd.DataFrame()      
        try:
            code = (symbol.split('.')[0]).lower()
            exchange = (symbol.split('.')[1]).lower()
        except Exception as e:
            print(e)
            return df
        if symbol_type == "future" or symbol_type == "stock":
            min_bar_dir = self.data_path + "/minbar/"+ symbol_type + "/" + date + "/1min/" 
            file_name = exchange +"_"+ code +"_" + date +"_1min.parquet"            
            if os.path.exists(min_bar_dir + file_name):
                df = pd.read_parquet(min_bar_dir + file_name)
            else:
                print("min bar path error:",min_bar_dir+file_name)
        return df
    
    def tick_or_minbar_data_exist(self,symbol,date,symbol_type = "future"):  
        try:
            code = (symbol.split('.')[0]).lower()
            exchange = (symbol.split('.')[1]).lower()
        except Exception as e:
            print(e)
            return False
        if symbol_type == "future" or symbol_type == "stock":
            min_bar_dir = self.data_path + "/minbar/"+ symbol_type + "/" + date + "/1min/" 
            file_name = exchange +"_"+ code +"_" + date +"_1min.parquet"            
            if os.path.exists(min_bar_dir + file_name):
                return True
        return False

    def get_tick_data(self,symbol,date,symbol_type = "stock",debug=False):
        """
        从datahouse中获取tick，期权的数据在股票类里
        :param symbol: str,format like IF2109.CFE or 600001.SH
        :param date : str,format like '20210907'
         param symbol_type : str, must be 'future' or 'stock'
        :return: dataframe
        """
        df = pd.DataFrame()     
        try:
            code = (symbol.split('.')[0]).lower()
            exchange = (symbol.split('.')[1]).lower()
        except Exception as e:
            if debug:
                print(e)
            return df
        if symbol_type == "future" or symbol_type == "stock":
            tick_dir = self.data_path + "/tick/"+ symbol_type + "/" + date + "/quote/" 
            file_name = exchange +"_"+ code +"_" + date +"_quote.parquet"            
            if os.path.exists(tick_dir + file_name):
                df = pd.read_parquet(tick_dir + file_name)
            else:
                if debug:
                    print("tick data path error:",tick_dir+file_name)
        return df

    def load_json_from_file(self,file_name):
        json_data = {}
        if os.path.exists(file_name):
            with open(file_name,encoding='utf-8') as f:
                json_data = json.load(f)
        return json_data

    def save_json_to_file(self,file_name,json_data):    
        with open(file_name,"w",encoding='utf-8') as f:
            json.dump(json_data,f,ensure_ascii=False,sort_keys=True, indent=4)

    def get_mssql_table_data(self,table_name,using_cache = True, cache_dir="cache_data"):
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        file_name = cache_dir + "/" + table_name +"_cache.parquet"
        if not os.path.exists(file_name) or (not using_cache):
            sql = f"select * from {table_name}"
            df = self.mssql_select(sql) 
            df = df.sort_values(by=['date'], ascending=True).reset_index(drop=True)
            df.to_parquet(file_name)
        else:
            today_str = datetime.datetime.now().strftime("%Y%m%d")
            cache_info_file = cache_dir + "/" + "cache_info_"+today_str+".json" 
            df = pd.read_parquet(file_name)
            record_date = df['date'].values[-1]           
            if not os.path.exists(cache_info_file):
                start_date = datetime.datetime(2022,12,31)
                trading_days = self.get_trading_day(start_date)       
                last_trade_day = list(trading_days['date'])[-2]
                cache_info = { 
                        "last_trade_day":last_trade_day,
                        "record_date": record_date
                }
                self.save_json_to_file(cache_info_file,cache_info)
            else:
                cache_info = self.load_json_from_file(cache_info_file)
            last_trade_day = cache_info["last_trade_day"]
            if record_date < last_trade_day:
                print(f" update {table_name} from {record_date}")
                sql = f"select * from {table_name} where date > '{record_date}' "
                new_df = self.mssql_select(sql) 
                df = pd.concat([df,new_df])
                df = df.sort_values(by=['date'], ascending=True).reset_index(drop=True)
                df.to_parquet(file_name)
        return df
