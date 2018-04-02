#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/8 17:11
# @Author  : xshxu@abcft.com
# @Site    : 
# @File    : stock_code_define.py.py
# @Software: PyCharm


stock_code_list = ['SPY', 'DIA', 'QQQ']

stock_code_map = {}
index = 1
for item in stock_code_list:
    stock_code_map[index] = item
    index += 1
stock_code_max_index = index - 1
