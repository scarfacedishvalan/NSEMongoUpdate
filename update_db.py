#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 27 19:11:37 2021

@author: abhirakshit
"""

import pandas as pd
from yahoofinancials import YahooFinancials
from joblib import Parallel, delayed
from datetime import datetime
import pymongo


def get_summary(ticker):
    yahoo_financials = YahooFinancials(ticker)
    return yahoo_financials.get_summary_data()


class MongoConnect():
    def __init__(self, collection_name, dbname):
        self.client = pymongo.MongoClient("mongodb+srv://abhrksht:Redwing@cluster0.lqb3j.mongodb.net/stocks_key_stats?retryWrites=true&w=majority")
        self.db = self.client[dbname]
        self.collection = self.db[collection_name]
    
    def create_index(self, index):
        res = self.collection.find_one({"index":index})
        if res is None:
            self.collection.insert_one({"index": index,"data":{}})
            return self
        else:
            return self
            

    def add_available_tickers(self, tickerlist):
        df = pd.DataFrame(columns = ["ticker"])
        df["ticker"] = tickerlist
        res = self.collection.find_one({"index":"all_tickers_available"})
        if res is None:
            self.collection.insert_one({"index": "all_tickers_available", 
                                        "data": df.to_dict("records")})
        else:
            self.collection.update_many({"index": "all_tickers_available"}, 
                                        { "$set": { "data": df.to_dict("records")}})
        
    
    def update_ticker_record(self, ticker, details_dict = None):
        try:
            res = self.collection.find_one({"index":ticker})
            updated_data = get_summary(ticker) if details_dict is None else details_dict 
            
            if updated_data[ticker]==None:
                print("No records for {}".format(ticker))
                return -1
            updated_data[ticker].update({"last_update": str(datetime.now())})
            if res is None:
                
                self.collection.insert_one({"index": ticker,"data":updated_data[ticker]})
            else:
                self.collection.update_many({"index": ticker}, { "$set": { "data": updated_data[ticker]}})
        except Exception as e:
            log_details = "Error for ticker: " + ticker + ": " + str(e)
            self.error_log(log_details)
            
    
    def find_details(self, ticker):
        res = self.collection.find_one({"index":ticker})
        if res is None:
            return {}
        return res["data"]
    
    def error_log(self, log_details):
        self = self.create_index("error_log")
        timestr = str(datetime.now())
        res = self.collection.find_one({"index": "error_log"})
        d1 = res["data"]
        d1.update({timestr: log_details})
        self.collection.update_many({"index": "error_log"}, { "$set": { "data": d1}})
        
    
    def get_all_tickers(self):
        d1 = self.find_details("all_tickers_available")
        return pd.DataFrame(d1)["ticker"].tolist()
    
    def get_updated_tickerlist(self):
        tickers = [ticker for ticker in self.collection.distinct(key = "index") if ticker != "all_tickers_available"]
        return tickers
        
    def drop_index(self, index):
        self.collection.drop_index(index)


if __name__ == '__main__':
    mgdb = MongoConnect("stocks_monitoring", "stocks_key_stats")
    all_tickers = mgdb.get_all_tickers()
    for ticker in all_tickers[-27:]:
        mgdb.update_ticker_record(ticker)
        print("Done for "+ ticker)
        
        
