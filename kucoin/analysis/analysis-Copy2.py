import pandas as pd
import json
from time import time, sleep
from datetime import datetime, timedelta
import csv


class Analysis(object):
    def __init__(self, use=''):
        self.use = use
        self.mins = None

        self.ticker_var = {'sequence': int(), 'time': str(), 'price': float()}
        self.tr_var = {'time': str(), 'timeline': int(), 'transaction': str()}
        self.op_var = {'timeline': float(), 'time': str(), 'o': float(), 'lower2': float(), 'upper2': float(),
                       'lower1': float(), 'upper1': float()}
        self.cl_var = {'timeline': float(), 'time': str(), 'h': float(), 'l': float(), 'c': float(), 'vol': float(),
                       'RSI': float(), 'SD_pct': float()}
        self.data0_var = ['time', 'timeline', 'o', 'h', 'l', 'c', 'lower2', 'upper2', 'lower1', 'upper1', 'vol', 'RSI',
                          'SD_pct']
        self.data_var = {'time': None, 'timeline': float(), 'o': float(), 'h': float(), 'l': float(), 'c': float(),
                         'lower2': float(), 'upper2': float(), 'lower1': float(), 'upper1': float(), 'vol': float(),
                         'RSI': float(), 'SD_pct': float()}
        # self.tr_var = {'transaction': str(), 'timeline': int(), 'time': str()}

        # instantiate dataframes needed in the analysis
        self.df_open = pd.DataFrame(columns=self.op_var, index=[])
        self.df_close = pd.DataFrame(columns=self.cl_var, index=[])
        self.df_data_1m = pd.DataFrame(columns=self.data_var, index=[])
        self.df_data_5m = pd.DataFrame(columns=self.data_var, index=[])
        self.df_data_15m = pd.DataFrame(columns=self.data_var, index=[])
        self.df_ticker = pd.DataFrame(self.ticker_var, index=[])
        # elf.df_sell_5m = pd.DataFrame(self.tr_var, index=[])
        # self.df_sell_15m = pd.DataFrame(self.tr_var, index=[])
        self.df_trade_5m = pd.DataFrame(self.tr_var, index=[])
        self.df_trade_15m = pd.DataFrame(self.tr_var, index=[])


        # set index of some dfs to `time`
        self.date = datetime.strptime("2021-01-01 00:00", "%Y-%m-%d %H:%M")
        self.init = pd.DataFrame({'time': [self.date]}) # dummy datetime data to append as first row
        self.df_data_1m = self.df_data_1m.append(self.init, ignore_index=False) # append dummy data as 1st row
        self.df_data_1m = self.df_data_1m.set_index(['time']) # set the index when df has 1st row w/dummy data. Ready to append rows
        self.df_data_5m = self.df_data_5m.append(self.init, ignore_index=False)
        self.df_data_5m = self.df_data_5m.set_index(['time'])
        self.df_data_15m = self.df_data_15m.append(self.init, ignore_index=False)
        self.df_data_15m = self.df_data_15m.set_index(['time'])
        # trade df's store the possible by & sell signals that will be analyzed
        self.df_trade_5m = self.df_trade_5m.append(self.init, ignore_index=False)
        self.df_trade_5m = self.df_trade_5m.set_index(['time'])
        self.df_trade_15m = self.df_trade_15m.append(self.init, ignore_index=False)
        self.df_trade_15m = self.df_trade_15m.set_index(['time'])
        self.df_open = self.df_open.append(self.init, ignore_index=False)
        self.df_open = self.df_open.set_index(['time'])
        self.df_close = self.df_close.append(self.init, ignore_index=False)
        self.df_close = self.df_close.set_index(['time'])

        # Constants for the maximum rows of each df
        self.MAX_SIZE_df_data_1m = 50
        self.MAX_SIZE_df_data_5m = 50
        self.MAX_SIZE_df_data_15m = 50
        self.MAX_SIZE_df_ticker = 500
        # self.MAX_SIZE_df_sell_5m = 50
        # self.MAX_SIZE_df_sell_15m = 50
        self.MAX_SIZE_df_trade_5m = 50
        self.MAX_SIZE_df_trade_15m = 50
        self.MAX_SIZE_df_close = 50
        self.MAX_SIZE_df_open = 50

        # variables needed to save the data in the dataframe as csv
        self.date_time = datetime.now()
        self.date_for_file = self.date_time.strftime("%Y_%m_%d")
        # self.time = self.date_time.strftime("%H-%M-%S")
        self.ticker_path = 'D:/Dropbox/Trading/James/csv/ticker'
        self.df_data_15m_path = 'D:/Dropbox/Trading/James/csv/data'
        self.df_data_5m_path = 'D:/Dropbox/Trading/James/csv/data'
        self.df_data_1m_path = 'D:/Dropbox/Trading/James/csv/data'
        self.df_data_path = 'D:/Dropbox/Trading/James/csv/data'
        # self.df_sell_15m_path = 'D:/Dropbox/Trading/James/csv/sell_15m'
        # self.df_sell_5m_path = 'D:/Dropbox/Trading/James/csv/sell_5m'
        self.df_trade_path = 'D:/Dropbox/Trading/James/csv/trade'


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
        pd.set_option('display.max_columns', None)
        pd.set_option("expand_frame_repr", False)
        # df.set_index(['time'])
        tl = df.iloc[-1, 0]
        # t = df.iloc[-1, 1]
        # o = df.iloc[-1, 2]
        # l2 = df.iloc[-1, 3]
        # u2 = df.iloc[-1, 4]
        # l1 = df.iloc[-1, 5]
        # u1 = df.iloc[-1, 6]

        #append = pd.DataFrame({'time': [t], 'timeline': [tl], 'o': [o], 'h': [None], 'l': [None], 'c': [None],
                               # 'lower2': [l2], 'upper2': [u2], 'lower1': [l1], 'upper1': [u1],
                               # 'vol': [None], 'RSI': [None], 'SD_pct': [None]})
        # No df resizing needs to be done. Handled in merge_close
        if tl == 15:
            # self.resize_df(self.df_data_15m, self.MAX_SIZE_df_data_15m)
            self.df_data_15m = self.df_data_15m.append(df, ignore_index=False)
        elif tl == 5:
            # self.resize_df(self.df_data_5m, self.MAX_SIZE_df_data_5m)
            self.df_data_5m = self.df_data_5m.append(df, ignore_index=False)
        elif tl == 1:
            # self.resize_df(self.df_data_1m, self.MAX_SIZE_df_data_1m)
            self.df_data_1m = self.df_data_1m.append(df, ignore_index=False)
        else:
            pass
        # print('Open-data1df:\n', self.df_data_1m)
        return

    # 8/24: Working
    def merge_close(self, df):
        pd.set_option('display.max_columns', None)
        pd.set_option("expand_frame_repr", False)
        # df.set_index(['time'])
        t = df.iloc[-1, 0]
        tl = int(df.iloc[-1, 1])
        h = df.iloc[-1, 2]
        l = df.iloc[-1, 3]
        c = df.iloc[-1, 4]
        vol = df.iloc[-1, 5]
        rsi = df.iloc[-1, 6]

        def update_values(df_to_update):
            # print('Update Values')
            # print(df_to_update)
            # if not (df_to_update.loc[t, 'o'] < 0): # If the row has `open` data

            # print('df to update:\n', df_to_update)
            df_to_update.loc[t, 'h'] = h
            df_to_update.loc[t, 'l'] = l
            df_to_update.loc[t, 'c'] = c
            df_to_update.loc[t, 'vol'] = vol
            df_to_update.loc[t, 'RSI'] = rsi
            upper2 = df_to_update.loc[t, 'upper2']
            lower2 = df_to_update.loc[t, 'lower2']
            close = df_to_update.loc[t, 'c']
            if (upper2 is not None) and (lower2 is not None) and (close is not None):
                sd_pct = (upper2-lower2)/close
                df_to_update.loc[t, 'SD_pct'] = sd_pct
            # else:
                # df_to_update = df_to_update.append(df, ignore_index=False)
            return
        # set the variable to the appropriate timeline
        if tl == 15:
            self.mins = timedelta(minutes=15)
            t = t - self.mins
            # Resize dataframe
            rows = []
            for row in self.df_data_15m.index:
                rows.append(row)
            while self.df_data_15m.shape[0] >= self.MAX_SIZE_df_data_15m:
                remove = rows.pop(0)
                self.df_data_15m = self.df_data_15m.drop([remove])
            print('data_15m size: ', self.df_data_15m.shape[0])

            update_values(self.df_data_15m)
            csv_out_15 = self.df_data_path + '/' + self.date_for_file + 'data_15.csv'
            # call function to save row to df
            df_15_csv = self.df_data_15m[t:t]
            df_15_csv.to_csv(csv_out_15, index=True, mode='a', header=False)
            # write_csv(self.df_data_15m, csv_out_15)

        elif tl == 5:
            self.mins = timedelta(minutes=5)
            t = t - self.mins
            # Resize dataframe
            rows = []
            for row in self.df_data_5m.index:
                rows.append(row)
            while self.df_data_5m.shape[0] >= self.MAX_SIZE_df_data_5m:
                remove = rows.pop(0)
                self.df_data_5m = self.df_data_5m.drop([remove])
            print('data_5m size: ', self.df_data_5m.shape[0])

            update_values(self.df_data_5m)
            csv_out_5 = self.df_data_path + '/' + self.date_for_file + 'data_5.csv'
            # call function to save row to df
            df_5_csv = self.df_data_5m[t:t]
            df_5_csv.to_csv(csv_out_5, index=True, mode='a', header=False)
            # write_csv(self.df_data_5m, csv_out_5)
        elif tl == 1:
            self.mins = timedelta(minutes=1)
            t = t - self.mins
            # Resize dataframe
            rows = []
            for row in self.df_data_1m.index:
                rows.append(row)
            while self.df_data_1m.shape[0] >= self.MAX_SIZE_df_data_1m:
                remove = rows.pop(0)
                self.df_data_1m = self.df_data_1m.drop([remove])
            print('data_5m size: ', self.df_data_1m.shape[0])

            update_values(self.df_data_1m)
            csv_out_1 = self.df_data_path + '/' + self.date_for_file + 'data_1.csv'
            # call function to save row to df
            df_1_csv = self.df_data_1m[t:t]
            df_1_csv.to_csv(csv_out_1, index=True, mode='a', header=False)
            #write_csv(self.df_data_1m, csv_out_1)
        else:
            print('No timeline')
            pass

        # print('data15df:\n', self.df_data_15m)
        # print('data5df:\n', self.df_data_5m)
        print('Close-data1df at', datetime.now().strftime("%Y_%m_%d_%I-%M-%S"), ':\n', self.df_data_1m)
        # update the df of the tim timeline selected in the above statement

        return

# 8/24: Working
    def merge_trade(self, df):
        csv_out = None
        t = df[-1, 0]
        tl = df[-1, 1]

        if tl == 15:
            # Resize dataframe
            # ===================================================================
            rows = []
            for row in self.df_trade_15m.index:
                rows.append(row)
            while self.df_trade_15m.shape[0] >= self.MAX_SIZE_df_trade_15m:
                remove = rows.pop(0)
                self.df_trade_15m = self.df_trade_15m.drop([remove])
                #===============================================================
            self.df_trade_15m = self.df_trade_15m.append(df, ignore_index=False)
            csv_out = self.df_trade_path + '/' + self.date_for_file + 'trade_15.csv'

        elif tl == 5:
            # Resize dataframe
            # ===================================================================
            rows = []
            for row in self.df_trade_5m.index:
                rows.append(row)
            while self.df_trade_5m.shape[0] >= self.MAX_SIZE_df_trade_5m:
                remove = rows.pop(0)
                self.df_trade_5m = self.df_trade_5m.drop([remove])
                # ===============================================================
            self.df_trade_5m = self.df_trade_5m.append(df, ignore_index=False)
            csv_out = self.df_trade_path + '/' + self.date_for_file + 'trade_5.csv'
        else:
            pass
        df.to_csv(csv_out, index=False, mode='a', header=False)
        return

    def analyze(self):
        buy_sell = None
        buy_sell_mode = None







