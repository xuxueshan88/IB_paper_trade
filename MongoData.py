#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/4/2 18:35
# @Author  : xshxu@abcft.com
# @Site    : 
# @File    : MongoData.py
# @Software: PyCharm

from pymongo import MongoClient
import pandas as pd
import datetime
# from pymongo import (ASCENDING, DESCENDING)


class MongoData():
    def __init__(self, host='10.12.0.30', port=27017, db='research', col='optionsonar'):
        self.client = MongoClient(host, port)
        self.db = self.client[db]
        self.col = self.db[col]

    # 获取期权localsymbol
    def get_option_code(item_loc):
        date_str = str(
            (item_loc['Expiration'].year - 2000) * 10000 + item_loc['Expiration'].month * 100 + item_loc['Expiration'].day)
        option_code = item_loc['stock_code'].upper()
        option_code = option_code + (6 - len(option_code)) * ' ' + date_str
        if item_loc['Type'].upper() == 'CALL':
            option_code += 'C'
        else:
            option_code += 'P'
        strike_str = item_loc['Strike'].replace('$', '').replace('.', '') + '0'
        strike_str = (8 - len(strike_str)) * '0' + strike_str
        option_code = option_code + strike_str
        return option_code


    # 获取option_code_map.csv函数
    def get_option_code_map_csv():
        option_list = []
        for index, item in data_pd.iterrows():
            option_code = get_option_code(item)
            option_list.append(option_code)
        option_list = list(set(option_list))
        option_map = pd.DataFrame(option_list, columns=['option_code'])
        option_map.to_csv('option_code_map.csv', index=False)


    # 获得unusual_option.csv文件
    def get_unusual_option_csv():
        my_client = MongoClient('127.0.0.1', 27017)
        my_db = my_client['option_data_us_day']
        global data_pd
        data_pd = data_pd.reindex(columns=data_pd.columns.tolist()+['0_day_C','1_day_C','2_day_C','3_day_C'])
        for index, item in data_pd.iterrows():
            buy_date_flag = False
            buy_date = None
            if 'days' in item['Timestamp']:
                days_diff = item['Timestamp'].split('day')[0]
                buy_date = item['date'].to_pydatetime() - datetime.timedelta(days=int(days_diff))
                # data_pd.loc[index, 'buy_date'] = datetime.datetime.strftime(buy_date, '%Y-%m-%d')
                data_pd.loc[index, 'buy_date'] = buy_date
                buy_date_flag = True
            elif 'hour' in item['Timestamp']:
                buy_date = item['date'].to_pydatetime() - datetime.timedelta(days=1)
                # data_pd.loc[index, 'buy_date'] = datetime.datetime.strftime(buy_date, '%Y-%m-%d')
                data_pd.loc[index, 'buy_date'] = buy_date
                buy_date_flag = True

            if buy_date_flag:
                option_code = get_option_code(item)
                for i in range(4):
                    query = {'option_code':option_code,'date':{'$gt' : buy_date,'$lt' : buy_date+datetime.timedelta(hours=23)}}
                    data_cur = my_db[item['stock_code'].upper()].find(query,{'_id':0,'date':1,'Close':1}).sort('date',1)
                    data = pd.DataFrame(list(data_cur))
                    if len(data)>0 :
                        data_pd.loc[index, str(i)+'_day_C'] = data.loc[data.index[-1],'Close']
                    next_index = trade_date.index(buy_date)+1
                    if next_index >= len(trade_date):
                        break
                    buy_date = trade_date[next_index]

            print(index)
        data_pd.to_csv('unusual_option.csv')

    # 获取数据库数据，转成dataframe格式，全局变量
    data = my_col.find({},{'_id':0})
    data_pd = pd.DataFrame(list(data))