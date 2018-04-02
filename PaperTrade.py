#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/14 12:12
# @Author  : xshxu@abcft.com
# @Site    : 
# @File    : PaperTrade.py
# @Software: PyCharm

import time
import random
from threading import Thread
from ContractSamples import ContractSamples
from datetime import timedelta
import pandas as pd
from pymongo import MongoClient
from pymongo import (DESCENDING, ASCENDING)
from ibapi.order import *
from OrderSamples import OrderSamples


class PaperTrade(Thread):
    def __init__(self, client):
        super().__init__()
        self.client = client

    def run(self):
        if self.client.started:
            return

        self.client.started = True

        if self.client.globalCancelOnly:
            print("Executing GlobalCancel only")
            self.client.reqGlobalCancel()
        else:
            print("Executing requests")
            self.paper_trade()
            print("Executing requests ... finished")

    def paper_trade(self):
        order_done = False
        while not self.client.process_done and not order_done:
            if self.client.nextValidOrderId is not None:
                self.client.placeOrder(self.client.nextOrderId(), ContractSamples.OptionWithLocalSymbol('AAPL  180420C00180000'),
                               OrderSamples.LimitOrder("BUY", 3, 0.1))
                order_done = True
            else:
                time.sleep(1)
                print('wait for placing order')
        print('place order done')
