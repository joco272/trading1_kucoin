import pandas as pd
import json
from time import time, sleep
from datetime import datetime
import csv


class Analysis(object):
    def __init__(self, use=''):
        self.use = use

        self.ticker_var = {'sequence': int(), 'time': str(), 'price': float()}
        self.tr_var = {'transaction': str(), 'timeline': int(), 'time': str()}
        self.op_var = {'timeline': float(), 'time': str(), 'o': float(), 'lower2': float(), 'upper2': float(),
                       'lower1': float(), 'upper1': float()}
        self.cl_var = {'timeline': float(), 'time': str(), 'h': float(), 'l': float(), 'c': float(), 'vol': float(),
                       'RSI': float()}
        self.data_var = ['timeline', 'time', 'o', 'h', 'l', 'c', 'lower2', 'upper2', 'lower1', 'upper1', 'vol', 'RSI',
                         'SD_pct']
        self.data2_var = {'timeline': float(), 'time': str(), 'o': float(), 'h': float(), 'l': float(), 'c': float(),
                          'lower2': float(), 'upper2': float(), 'lower1': float(), 'upper1': float(), 'vol': float(),
                          'RSI': float(), 'SD_pct': float()}
        # self.tr_var = {'transaction': str(), 'timeline': int(), 'time': str()}

        # instantiate dataframes needed in the analysis
        self.df_data_1m = pd.DataFrame(columns=self.data_var, index=[])
        self.df_data_5m = pd.DataFrame(columns=self.data_var, index=[])
        self.df_data_15m = pd.DataFrame(columns=self.data_var, index=[])
        self.df_ticker = pd.DataFrame(self.ticker_var, index=[])
        self.df_sell_5m = pd.DataFrame(self.tr_var, index=[])
        self.df_sell_15m = pd.DataFrame(self.tr_var, index=[])
        self.df_buy_5m = pd.DataFrame(self.tr_var, index=[])
        self.df_buy_15m = pd.DataFrame(self.tr_var, index=[])

        # Constants for the maximum rows of each df
        self.MAX_SIZE_df_data_1m = 500
        self.MAX_SIZE_df_data_5m = 500
        self.MAX_SIZE_df_data_15m = 500
        self.MAX_SIZE_df_ticker = 500
        self.MAX_SIZE_df_sell_5m = 500
        self.MAX_SIZE_df_sell_15m = 500
        self.MAX_SIZE_df_buy_5m = 500
        self.MAX_SIZE_df_buy_15m = 500

        # variables needed to save the data in the dataframe as csv
        self.date_time = datetime.now()
        self.date_for_file = self.date_time.strftime("%Y_%m_%d")
        # self.time = self.date_time.strftime("%H-%M-%S")
        self.ticker_path = 'D:/Dropbox/Trading/James/csv/ticker'
        self.df_data_15m_path = 'D:/Dropbox/Trading/James/csv/data_15m'
        self.df_data_5m_path = 'D:/Dropbox/Trading/James/csv/data_5m'
        self.df_data_1m_path = 'D:/Dropbox/Trading/James/csv/data_1m'
        self.df_sell_15m_path = 'D:/Dropbox/Trading/James/csv/sell_15m'
        self.df_sell_5m_path = 'D:/Dropbox/Trading/James/csv/sell_5m'
        self.df_buy_15m_path = 'D:/Dropbox/Trading/James/csv/buy_15m'
        self.df_buy_5m_path = 'D:/Dropbox/Trading/James/csv/buy_5m'


    def change_date_for_file(self, date):
        self.date_for_file = date

    # This code saves the ticker to CSV file. No header
    def merge_ticker(self, df):
        csv_out = self.ticker_path+'/'+self.date_for_file+'_ticker.csv'

        rows = []
        # old = self.df_ticker.shape[0]
        for row in self.df_ticker.index:
            rows.append(row)
        while self.df_ticker.shape[0] >= self.MAX_SIZE_df_ticker:
            remove = rows.pop(0)
            self.df_ticker = self.df_ticker.drop([remove])
        # new = self.df_ticker.shape[0]
        # print(f'resizing DF from {old} to {new}')
        # print('ticker: ', self.df_ticker.shape[0])

        self.df_ticker = pd.merge(self.df_ticker, df, how='outer')
        df.to_csv(csv_out, index=False, mode='a', header=False)

        return

    # open and close webhook timestamp time is that of server-side event. Examples follow:
    # open time will always be the open time of the candle, not the time it was sent by server
    # 5m: a candle that opens at 8:55 will have a ts of 8:55:00 AM
    # The same above candle will have a close ts of 9:00:00 AM
    def merge_open(self, df):
        tl = df.iloc[-1, 0]
        t = df.iloc[-1, 1]
        o = df.iloc[-1, 2]
        l2 = df.iloc[-1, 3]
        u2 = df.iloc[-1, 4]
        l1 = df.iloc[-1, 5]
        u1 = df.iloc[-1, 6]

        append = pd.DataFrame({'timeline': [tl], 'time': [t], 'o': [o], 'h': [None], 'l': [None], 'c': [None],
                               'lower2': [l2], 'upper2': [u2], 'lower1': [l1], 'upper1': [u1],
                               'vol': [None], 'RSI': [None], 'SD_pct': [None]})
        if tl == 15:
            self.resize_df(self.df_data_15m, self.MAX_SIZE_df_data_15m)
            self.df_data_15m = pd.merge(self.df_data_15m, append, how='outer')
        elif tl == 5:
            self.resize_df(self.df_data_5m, self.MAX_SIZE_df_data_5m)
            self.df_data_5m = pd.merge(self.df_data_5m, append, how='outer')
        elif tl == 1:
            self.resize_df(self.df_data_1m, self.MAX_SIZE_df_data_1m)
            self.df_data_1m = pd.merge(self.df_data_1m, append, how='outer')
        else:
            pass

        return

    # 8/24: Working
    def merge_close(self, df):
        pd.set_option('display.max_columns', None)
        tl = df.iloc[-1, 0]
        t = df.iloc[-1, 1]
        h = df.iloc[-1, 2]
        l = df.iloc[-1, 3]
        c = df.iloc[-1, 4]
        vol = df.iloc[-1, 5]
        rsi = df.iloc[-1, 6]

        #FUNSCTION MAY BE OBSOLETE use new function in Merge_Ticker
        def write_csv(df_to_use, csv_path):
            rows = []
            columns = []
            for i in df_to_use.index():
                rows.append(i)
            for column in df_to_use:
                columns.append(column)
            df_csv = pd.DataFrame(df, index=[rows[-1]], columns=columns)
            df_csv.to_csv(csv_path, index=False, mode='a', header=False)

        def update_values(df_to_update):
            try:
                print(df_to_update)
                df_to_update.iloc[-1, 0] = tl
                df_to_update.iloc[-1, 1] = t
                df_to_update.iloc[-1, 3] = h
                df_to_update.iloc[-1, 4] = l
                df_to_update.iloc[-1, 5] = c
                df_to_update.iloc[-1, 10] = vol
                df_to_update.iloc[-1, 11] = rsi
                upper2 = df_to_update.iloc[-1, 7]
                lower2 = df_to_update.iloc[-1, 6]
                close = df_to_update.iloc[-1, 5]
                if (upper2 is not None) and (lower2 is not None) and (close is not None):
                    sd_pct = (upper2-lower2)/close
                    df_to_update.iloc[-1, 12] = sd_pct
            except IndexError:  # if the df is blank with no 'open' data, initialize as empty
                append = pd.DataFrame({'timeline': [tl], 'time': [t], 'o': [None], 'h': [h], 'l': [l],
                                       'c': [c], 'lower2': [None], 'upper2': [None], 'lower1': [None],
                                       'upper1': [None], 'vol': [vol], 'RSI': [rsi]})
                df_to_update = pd.merge(df_to_update, append, how="outer")
                return
        # set the variable to the appropriate timeline
        if tl == 15:
            # Resize dataframe
            rows = []
            for row in self.df_data_15m.index:
                rows.append(row)
            while self.df_data_15m.shape[0] >= self.MAX_SIZE_df_data_15m:
                remove = rows.pop(0)
                self.df_data_15m = self.df_data_15m.drop([remove])
            print('data_15m size: ', self.df_data_15m.shape[0])

            update_values(self.df_data_15m)
            csv_out_15 = self.df_data_15m_path + '/' + self.date_for_file + 'data_15.csv'
            # call function to save row to df
            df_15_csv = self.df_data_15m[rows[-1]:rows[-1]+1]
            df_15_csv.to_csv(csv_out_15, index=False, mode='a', header=False)
            # write_csv(self.df_data_15m, csv_out_15)

        elif tl == 5:
            # Resize dataframe
            rows = []
            for row in self.df_data_5m.index:
                rows.append(row)
            while self.df_data_5m.shape[0] >= self.MAX_SIZE_df_data_5m:
                remove = rows.pop(0)
                self.df_data_5m = self.df_data_5m.drop([remove])
            print('data_5m size: ', self.df_data_5m.shape[0])

            update_values(self.df_data_5m)
            csv_out_5 = self.df_data_5m_path + '/' + self.date_for_file + 'data_5.csv'
            # call function to save row to df
            df_5_csv = self.df_data_5m[rows[-1]:rows[-1]+1]
            df_5_csv.to_csv(csv_out_5, index=False, mode='a', header=False)
            # write_csv(self.df_data_5m, csv_out_5)
        elif tl == 1:
            # Resize dataframe
            rows = []
            for row in self.df_data_1m.index:
                rows.append(row)
            while self.df_data_1m.shape[0] >= self.MAX_SIZE_df_data_1m:
                remove = rows.pop(0)
                self.df_data_1m = self.df_data_1m.drop([remove])
            print('data_5m size: ', self.df_data_1m.shape[0])

            update_values(self.df_data_1m)
            csv_out_1 = self.df_data_1m_path + '/' + self.date_for_file + 'data_1.csv'
            # call function to save row to df
            df_1_csv = self.df_data_1m[rows[-1]:rows[-1]+1]
            df_1_csv.to_csv(csv_out_1, index=False, mode='a', header=False)
            #write_csv(self.df_data_1m, csv_out_1)
        else:
            pass

        print('data15df:\n', self.df_data_15m)
        print('data5df:\n', self.df_data_5m)
        print('data1df:\n', self.df_data_1m)
        # update the df of the tim timeline selected in the above statement

        return

# 8/24: Working
    def merge_buy(self, df):
        csv_out = None
        if df.iloc[-1, 2] == 15:
            self.resize_df(self.df_buy_15m, self.MAX_SIZE_df_buy_15m)
            self.df_buy_15m = pd.merge(self.df_buy_15m, df, how='outer')
            csv_out = self.df_buy_15m_path + '/' + self.date_for_file + 'buy_15.csv'

        elif df.iloc[-1, 2] == 5:
            self.resize_df(self.df_buy_5m, self.MAX_SIZE_df_buy_5m)
            self.df_buy_5m = pd.merge(self.df_buy_5m, df, how='outer')
            csv_out = self.df_buy_5m_path + '/' + self.date_for_file + 'buy_5.csv'
        else:
            pass
        df.to_csv(csv_out, index=False, mode='a', header=False)
        return

    def merge_sell(self, df):
        csv_out = None
        if df.iloc[-1, 2] == 15:
            rows = []
            for row in self.df_sell_15m.index:
                rows.append(row)
            while self.df_sell_15m.shape[0] >= self.MAX_SIZE_df_sell_15m:
                remove = rows.pop(0)
                self.df_sell_15m = self.df_sell_15m.drop([remove])
            print('df_sell_15m size: ', self.df_sell_15m.shape[0])

            self.df_sell_15m = pd.merge(self.df_sell_15m, df, how='outer')
            csv_out = self.df_sell_15m_path + '/' + self.date_for_file + 'sell_15.csv'
        elif df.iloc[-1, 2] == 5:
            rows = []
            for row in self.df_sell_5m.index:
                rows.append(row)
            while self.df_sell_5m.shape[0] >= self.MAX_SIZE_df_sell_5m:
                remove = rows.pop(0)
                self.df_sell_5m = self.df_sell_5m.drop([remove])
            print('df_sell_5m size: ', self.df_sell_5m.shape[0])

            self.df_sell_5m = pd.merge(self.df_sell_5m, df, how='outer')
            csv_out = self.df_sell_5m_path + '/' + self.date_for_file + 'sell_5.csv'
        else:
            pass
        df.to_csv(csv_out, index=False, mode='a', header=False)

    # NOT USED since it is implemented in the different MERGE functions
    def resize_df(self, df, size):
        print('resizing DF')
        rows = []
        old = df.shape[0]
        for row in df.index:
            rows.append(row)

        while df.shape[0] >= size:
            remove = rows.pop(0)
            df = df.drop([remove])
        new = df.shape[0]
        print(f'resizing DF from {old} to {new}')
        print('ticker: ', self.df_ticker.shape[0])

    def analyze(self):
        buy_sell = None
        buy_sell_mode = None







