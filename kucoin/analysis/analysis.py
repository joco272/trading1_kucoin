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
        self.tr_var = {'time': str(), 'timeline': int(), 'b': int(), 's': int(), 'n': int(), 'c': int(), 'ts': str(), 'tl': int()}
        self.op_var = {'timeline': float(), 'time': str(), 'o': float(), 'lower2': float(), 'upper2': float(),
                       'lower1': float(), 'upper1': float()}
        self.cl_var = {'timeline': float(), 'time': str(), 'h': float(), 'l': float(), 'c': float(), 'vol': float(),
                       'RSI': float(), 'SD_pct': float()}
        self.data0_var = ['time', 'timeline', 'o', 'h', 'l', 'c', 'lower2', 'upper2', 'lower1', 'upper1', 'vol', 'RSI',
                          'SD_pct']
        self.data_var = {'time': None, 'timeline': float(), 'o': float(), 'h': float(), 'l': float(), 'c': float(),
                         'lower2': float(), 'upper2': float(), 'lower1': float(), 'upper1': float(), 'vol': float(),
                         'RSI': float(), 'SD_pct': float(), 'SD': float()}

        # instantiate dataframes needed in the analysis
        self.df_open = pd.DataFrame(columns=self.op_var, index=[])
        self.df_close = pd.DataFrame(columns=self.cl_var, index=[])
        self.df_data = pd.DataFrame()
        self.df_ticker = pd.DataFrame(self.ticker_var, index=[])

        # set index of some dfs to `time`
        self.date = datetime.strptime("2021-01-01 00:00", "%Y-%m-%d %H:%M")
        self.init = pd.DataFrame({'time': [self.date]}) # dummy datetime data to append as first row
        self.init_data = pd.DataFrame({'time': [self.date], 'timeline': [0], 'o': ['o'], 'h': ['h'], 'l': ['l'], 'c': ['c'], 'lower2': ['u2'], 'upper2': ['l2'], 'lower1': ['l1'], 'upper1': ['u1'], 'vol': ['vol'], 'RSI': ['RSI'], 'SD_pct': ['SD Pct'], 'SD': ['SD']})
        self.df_data = self.df_data.append(self.init_data, ignore_index=False) # append dummy data as 1st row
        self.df_data = self.df_data.set_index(['time', 'timeline']) # set the index when df has 1st row w/dummy data. Ready to append rows
        # trade df's store the possible by & sell signals that will be analyzed
        # self.df_trade_alerts = self.df_trade_alerts.append(self.init, ignore_index=False) # Data from all timelines stored, filtered later

        self.df_trade_alerts = pd.DataFrame({'time': [self.date], 'timeline': [1], 'b': [0], 's': [0], 'n': [1], 'c': [1], 'ts': [self.date], 'tl': [1]})
        self.df_trade_alerts = self.df_trade_alerts.set_index(['time', 'timeline', 'b', 's', 'n', 'c'])

        # Constants for the maximum rows of each df
        self.MAX_SIZE_df_data = 750
        self.MAX_SIZE_df_ticker = 500
        self.MAX_SIZE_df_trade_alerts = 150
        self.MAX_SIZE_df_close = 50
        self.MAX_SIZE_df_open = 50

        # variables needed to save the data in the dataframe as csv
        self.date_time = datetime.now()
        self.time = self.date_time.strftime("%Y_%m_%d-%H-%M-%S")
        self.date_for_file = self.date_time.strftime("%Y_%m_%d")
        self.ticker_path = 'D:/Dropbox/Trading/James/csv/ticker'
        self.df_data_path = 'D:/Dropbox/Trading/James/csv/data'
        self.df_trade_path = 'D:/Dropbox/Trading/James/csv/trade'
        self.df_open_path = 'D:/Dropbox/Trading/James/csv/open'
        self.df_close_path = 'D:/Dropbox/Trading/James/csv/close'

        # trade signal variables:
        self.RSI_OVERBOUGHT = 64
        self.RSI_OVERSOLD = 33

        # time resolution that the algorithm is working with
        self.RESOLUTIONS = [15,5,1] # time resolutions available for teh algo to use
        self.PRIMARY_RESOLUTION = 15 # default working resolution
        self.SECONDARY_RESOLUTION = None
        self.use_smaller_resolution = False

        # THE MOST RECENT SIGNAL, in the process of being evaluated
        # the index of the df row that changed the state to Buy or Sell
        self.signal_index = None
        # Signal type
        self.signal = None # Buy, Sell
        self.signal_time = None
        self.signal_resolution = None
        # state of the analysis object: Idle (waiting for signal), Buy (waiting for), Sell (Waiting for)
        self.signal_validity = None # Valid, Invalid, Wait

        # Last executed transaction
        self.last_valid_signal = None # Buy, Sell. The last signal that prompted an execution
        self.last_valid_signal_time = None
        self.last_valid_signal_resolution = None
        self.last_transaction = None # Buy, Sell. The last transaction, whether from valid signal or correction
        self.last_transaction_timestamp = None # of the candle that triggered the last_transaction
        self.last_transaction_resolution = None # of the candle that triggered the last_transaction
        self.last_transaction_close = None  # of the candle that triggered the last_transaction
        self.last_sd = None # standard dev (5 min resolution) of last transaction, whether from valid signal or correction
        self.stoploss_limit = None

        # instance variables for paper trading
        self.init_transactions = pd.DataFrame(
            {'time_ex': [self.date], 'timeline_ex': ['timeline_ex'], 'time_s': ['time_s'], 'timeline_s': ['timeline_s'],
             'signal': ['signal'], 'c': ['c'], 'limit': ['limit'], 'stoploss': ['stoploss'],
             'transaction': ['transaction'], 'balance': ['balance'], 'currency': ['currency']})
        self.df_transactions = self.df_data.append(self.init_data, ignore_index=False)  # append dummy data as 1st row
        self.df_transactions_path = 'D:/Dropbox/Trading/James/paper_trading'
        self.paper_balance_amount = 1
        self.paper_balance_currency = "ETH"


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
        self.date_time = datetime.now()  # update the date for the file name when open data is received
        self.time = self.date_time.strftime("%Y_%m_%d-%H-%M-%S")
        self.date_for_file = self.date_time.strftime("%Y_%m_%d")

        pd.set_option('display.max_columns', None)
        # pd.set_option("expand_frame_repr", False)

        self.df_open = self.df_open.append(df, ignore_index=True)

        print(f'{self.time} Open-df size is:', self.df_open.shape[0], '\n', self.df_open)
        csv_open = self.df_open_path + '/' + self.date_for_file + 'open.csv'
        df_open_csv = self.df_open[self.df_open.shape[0]-1:self.df_open.shape[0]]
        df_open_csv.to_csv(csv_open, index=True, mode='a', header=False)

        # resize df to be of MAX size

        if self.df_open.shape[0] > self.MAX_SIZE_df_open:
            self.df_open = self.df_open[self.df_open.shape[0] - self.MAX_SIZE_df_open:]

        # print('Open===================================================================\n', self.df_open)
        return

    # append_df = merge(inner) the open_df with a new df that has the last row of the close_df
    def merge_close(self, df):
        pd.set_option('display.max_columns', None)
        # pd.set_option("expand_frame_repr", False)
        print(f'{self.time} merge_close function called with', df)
        # df = df.set_index(['time'])
        self.df_close = self.df_close.append(df, ignore_index=False)
        print(f'{self.time} Close-df appended-size is:', self.df_close.shape[0], '\n', self.df_close)
        csv_close = self.df_close_path + '/' + self.date_for_file + 'close.csv'
        self.df_data.to_csv(csv_close, index=True, mode='a', header=False)
        #===================================Start new Merge method======================================

        df_to_append = pd.merge(left=self.df_open, right=self.df_close, how='inner', on=['time', 'timeline'])
        df_to_append = df_to_append.set_index(['time'])

        for index, row in df_to_append.iterrows():
            upper1 = df_to_append.loc[index, 'upper1']
            lower1 = df_to_append.loc[index, 'lower1']
            close = df_to_append.loc[index, 'c']
            sd = (upper1 - lower1) / 2
            sd_pct = sd / close
            df_to_append.loc[index, 'SD_pct'] = sd_pct
            df_to_append.loc[index, 'SD'] = sd

        self.df_data = self.df_data.append(df_to_append)
        csv_data = self.df_data_path + '/' + self.date_for_file + '_data.csv'
        self.df_data.to_csv(csv_data, index=True, mode='a', header=False)
        self.df_close = self.df_close[:0]

        if self.df_data.shape[0] > self.MAX_SIZE_df_data:
            self.df_data = self.df_data[self.df_data.shape[0] - self.MAX_SIZE_df_data:]
        print(f'{self.time} Close merge finished-DATA size is:', self.df_data.shape[0], '\n', self.df_data)

        index_list = df_to_append.index
        for idx in index_list:
            self.analyze_signal_and_data(idx[0], idx[1])
        return

    # Merges trade signals into one df
    def merge_action(self, df=None): #change name to "process signals"?

        def process_incoming_signals(df):
            # Resize dataframe
            rows = self.df_trade_alerts.index
            # remove the extra rows, starting with the oldest
            if self.df_trade_alerts.shape[0] >= self.MAX_SIZE_df_trade_alerts:
                num_rows_to_remove = self.df_trade_alerts.shape[0] - self.MAX_SIZE_df_trade_alerts
                for row_to_remove in range(num_rows_to_remove):
                    remove = rows.pop(row_to_remove)
                    self.df_trade_alerts = self.df_trade_alerts.drop([remove])

            # set index, append DF, save to CSV
            df = df.set_index(['time', 'timeline', 'b', 's', 'n', 'c'])
            self.df_trade_alerts = self.df_trade_alerts.append(df, ignore_index=False)
            csv_out = self.df_trade_path + '/' + self.date_for_file + 'trade_alerts.csv'
            df_csv = self.df_trade_alerts[self.df_trade_alerts.shape[0] - 1:self.df_trade_alerts.shape[0]]
            df_csv.to_csv(csv_out, index=True, mode='a', header=False)
            return

        process_incoming_signals(df)

        index_list = df.index
        for idx in index_list:
            timestamp = idx[0]
            resolution = idx[1]
            if resolution == self.SECONDARY_RESOLUTION or resolution == self.PRIMARY_RESOLUTION:
                self.analyze_signal_and_data(timestamp, resolution)

        return

    # this function is called by merge_action and merge_close. Needs to come before
    def analyze_signal_and_data(self, timestamp, resolution):
        # every candle and every action signal at any resolution should trigger this function

        def check_signals_complete(df_index):  # Check that sub-signals are complete. Inner function in merge_action
            # takes the index as an argument to check if all subsignals have been received
            sub_signals_complete = False
            sub_signal_indexes = [] # delete

            start_sub = df_index[0]
            check_complete_tl = int(df_index[1])
            end_sub = start_sub + timedelta(minutes=check_complete_tl - 1)

            # not being used?
            sub_signal_res = self.RESOLUTIONS[self.RESOLUTIONS.index(check_complete_tl) + 1]
            resolutions_to_use = self.RESOLUTIONS[self.RESOLUTIONS.index(check_complete_tl):]

            try:
                sub_signals_df = self.df_trade_alerts.loc[
                                 (slice(start_sub, end_sub), resolutions_to_use, (0, 1), (0, 1), (0, 1)), :]
            except KeyError:
                print(f'no sub-signals found for {df_index}')
                # need to update code. Probably pass? Log to CSV?
            else:
                sub_signals_df_shape = 0
                for resolution_to_use in resolutions_to_use:
                    sub_signals_df_shape += resolutions_to_use[0] / resolution_to_use
                if sub_signals_df.shape[0] == sub_signals_df_shape:  # sub-signals are complete
                    print('sub-signals are complete:', sub_signals_df.shape[0])
                    # mark signal and sub-signals (except smaller resolution) as complete
                    sub_signals_df = self.df_trade_alerts.loc[
                                     (slice(start_sub, end_sub),
                                      self.RESOLUTIONS[self.RESOLUTIONS.index(check_complete_tl):-1],
                                      (0, 1), (0, 1), (0, 1)), :]  # exclude 1m
                    sub_signals_index_names = sub_signals_df.index.names
                    sub_signal_indexes = sub_signals_df.index.values
                    sub_signals_df = sub_signals_df.reset_index()

                    for ind in range(sub_signals_df.shape[0]):  # indexes were reset, so indexes start at 0
                        sub_signals_df.at[ind, 'c'] = 1
                    sub_signals_df = sub_signals_df.set_index(sub_signals_index_names)
                    self.df_trade_alerts.drop(sub_signal_indexes, inplace=True)
                    self.df_trade_alerts = self.df_trade_alerts.append(sub_signals_df)
                    self.df_trade_alerts.sort_index(inplace=True)
                    sub_signals_complete = True # Added 1/30/22. Make sure I didn't mess up
                else:
                    print('sub-signals are NOT complete:', sub_signals_df.shape[0])
                    # need to update code. Probably pass? Log to CSV?
            return sub_signals_complete  # end of inner function 'check_complete'

        def detect_signal(resolution, start = None):
            # Helper function for use resolution functions. Helps not repeat code
            # Finished 12/13 18:23
            # Untested
            """
            What return values should be used??????
            """
            signal_time = None
            signal_resolution = None
            signal = None

            if start is None: # When principal resolution is in use
                start_df = self.df_trade_alerts.iloc[0, 2]
                end_df = self.df_trade_alerts.iloc[-1, 2]
            else: # When secondary resolution is in use
                start_df = start
                end_df = start_df + timedelta(minutes=resolution-1)

            try:  # Is there a SELL signal?
                check_sell_1 = self.df_trade_alerts.loc[(slice(start_df, end_df), (resolution), (0, 1), (1), (0)), :]
                check_sell = check_sell_1.iloc[-1]
            except KeyError:
                # No sell signal
                try:  # IS there a BUY signal?
                    check_buy_1 = self.df_trade_alerts.loc[(slice(start_df, end_df), (resolution), (1), (0, 1), (0)), :]
                    check_buy = check_buy_1.iloc[-1]
                    signal_index = check_buy.name
                    signal_time = signal_index[0]
                    signal_resolution = signal_index[1]
                except KeyError:
                    # No sell Signal
                    signal = None
                    return signal, None, None
                else:  # buy signal detected
                    print('Buy signal was detected at', check_buy.name)
                    signal = 'Buy'
                    return signal, signal_time, signal_resolution
                    # analyze(start, end, tl, buy, sell) #Does this need to be replaced with the return values????
            else:  # SELL signal detected
                print('Sell signal was detected at', check_sell.name)
                signal_index = check_sell.name
                signal_time = signal_index[0]
                signal_resolution = signal_index[1]
                signal = 'Sell'
                return signal, signal_time, signal_resolution
            return

        def check_signal_validity(signal_time, resolution):
            # Diagram: Check transaction execution
            # Finished 12/13 18:22
            # Untested
            """
            After a signal has been received, decides whether the transaction should be executed
            Analyzes that (close outside BB2) -> (close inside BB1)
            start_time: Required. timestamp of the signal received
            resolution: Required. Resolution being analyzed
            end_time: needed only when analyzing sub-signals
            """
            change_counter = 0
            changes_list = []
            end_time = self.df_data.iloc[-1].name[0] # item[0] of the series' name (index) is tiemstamp
            check_transaction_df = self.df_data.loc[(slice(signal_time, end_time), resolution),:]
            index_list = check_transaction_df.index
            last_row = index_list[-1]
            signal_status = None
                # Has the color of the candles (direction of close) changed only once?
                # (1/2) Assign color to the candles
            for idx in index_list:
                if self.df_data.loc[idx, 'c'] > self.df_data.loc[idx, 'o']:
                    changes_list.append('green')
                else:
                    changes_list.append('red')

            # (2/2) Count the times the color of the candles changes
            for i in range(1, len(changes_list)):
                # Change < 2? (2)
                if changes_list[i - 1] != changes_list[i]:
                    change_counter += 1
            # Diagram decision 1: Did te direction of the closing price change more than once?
            if change_counter < 2:
                # Diagram decision 2: Did the candle close inside upper1 and lower1
                if self.df_data.loc[last_row, 'upper1'] > self.df_data.loc[last_row, 'c'] > self.df_data.loc[
                    last_row, 'lower1']:
                    # Diagram outcome 3
                    signal_status = 'Valid'
                    self.signal_validity = signal_status
                    return signal_status
                else:
                    # Diagram outcome 2
                    signal_status = 'Wait'
                    self.signal_validity = signal_status
                    return signal_status
            else:
                # Diagram outcome 1
                self.buy_sell = 'None'
                signal_status = 'Invalid'
                self.signal_validity = signal_status
                return signal_status

        # The use primary and use secondary functions analyze signals and data and send the execution signal
        def use_primary_resolution(timestamp, resolution):
            self.df_trade_alerts.sort_index(inplace=True)
            index_list = self.df_trade_alerts.index
            start_sub = timestamp
            end_sub = index_list[-1][0]
            transaction_execution = False
            signal = None
            sub_signal = None
            secondary_resolution = self.RESOLUTIONS[self.RESOLUTIONS.index(resolution)+1]
            try: # Graph decision A2: Signal @ main resolution?
                df_slice = self.df_trade_alerts.loc[(slice(start_sub, end_sub), resolution, (0, 1), (0, 1), (0, 1), (0, 1)), :]
            except IndexError: # A2 No
                #no signal @ primary resolution found
                pass
            else: # A2 Yes, signal @ main resolution
                # Graph decision A3: Sub-signal complete?
                df_index_1 = df_slice.iloc[-1] # this results in a series, not a df
                df_index = df_index_1.name # so we use .name instead of .index
                sub_signals_complete = check_signals_complete(df_index)
                if sub_signals_complete: # Graph decision A4
                    # Graph decision A4 Yes: There is a BUY or SELL @ principal resolution; sub-signals are complete
                    signal, signal_time, signal_resolution = detect_signal(resolution) #return values for signal should be: Buy, Sell, None
                    # Above line returns the last (current or not) signal @ principal resolution
                    if signal == 'Buy' or signal == 'Sell': #A4: What kind of signal is it??
                        self.signal = signal
                        self.signal_time = signal_time
                        self.signal_resolution = resolution
                        self.use_smaller_resolution = False
                        self.SECONDARY_RESOLUTION = None
                        # Graph decision A5: if matching sub-signal
                        sub_signal, sub_signal_time, sub_signal_resolution = detect_signal(secondary_resolution, signal_time)
                        if sub_signal == 'Buy' or sub_signal == 'Sell':
                            # Graph decision A6: Is the sub-signal valid?
                            sub_signal_validity = check_signal_validity(sub_signal_time, sub_signal_resolution)
                            if sub_signal_validity == 'Valid':
                                # Execute transaction
                                transaction_execution = True
                                self.signal = sub_signal
                                self.signal_time = sub_signal_time
                                self.signal_resolution = sub_signal_resolution
                                self.use_smaller_resolution = True
                                self.SECONDARY_RESOLUTION = sub_signal_resolution
                                pass
                            elif sub_signal_validity == 'Wait':
                                # Wait @ secondary resolution
                                self.signal = sub_signal
                                self.signal_time = sub_signal_time
                                self.signal_resolution = sub_signal_resolution
                                self.use_smaller_resolution = True
                                self.SECONDARY_RESOLUTION = sub_signal_resolution
                            elif sub_signal_validity == 'Invalid':
                                # Wait @ Primary resolution
                                self.SECONDARY_RESOLUTION = None
                                self.use_smaller_resolution = False
                        else: # A5 NO (No sub-signal @ secondary)
                            # Check for the validity of last signal @ principal resolution
                            signal_validity = check_signal_validity(signal_time, signal_resolution)
                            if signal_validity == 'Valid':
                                # Execute transaction
                                transaction_execution = True
                                pass
                            elif signal_validity == 'Wait':
                                # Wait @ principal resolution
                                self.SECONDARY_RESOLUTION = None
                                self.use_smaller_resolution = False
                            elif signal_validity == 'Invalid':
                                # invalidate signal
                                self.SECONDARY_RESOLUTION = None
                                self.use_smaller_resolution = False
                                self.signal = None
                                self.signal_validity = None
                                self.signal_time = None
                                self.signal_resolution = None
                                self.signal_index = None
                    else: # A4 NO (No Signal in current candle @ Primary)
                        # End
                        pass
                else: # A3 NO (Sub-signals not complete)
                    # End
                    pass
            return transaction_execution

        def use_secondary_resolution(timestamp, resolution):
            self.df_trade_alerts.sort_index(inplace=True)
            index_list = self.df_trade_alerts.index
            start_sub = timestamp
            end_sub = index_list[-1][0]
            transaction_execution = False

            try: # to find signal data of the secondary resolution
                df_slice = self.df_trade_alerts.loc[(slice(start_sub, end_sub), resolution, (0, 1), (0, 1), (0, 1), (0, 1)), :]
            except IndexError:
                #no data of the correct resolution found
                pass
            else:
                # Graph decision B4: There is a BUY or SELL @ secondary resolution
                signal, signal_time, signal_resolution = detect_signal(resolution)  # return values for signal should be: Buy, Sell, None
                if signal == 'Buy' or signal == 'Sell':
                    self.signal = signal
                    self.signal_time = signal_time
                    self.signal_resolution = signal_resolution
                    self.use_smaller_resolution = True
                    self.SECONDARY_RESOLUTION = resolution
                    # Graph B6: Conduct the validity check of the signal
                    self.df_trade_alerts.sort_index(inplace=True)
                    signal_validity = check_signal_validity(signal_time, resolution)
                    if signal_validity == 'Valid':
                        # Execute transaction
                        transaction_execution = True
                    elif signal_validity == 'Wait':
                        # No need to do anything
                        pass
                    elif signal_validity == 'Invalid':
                        # Wait @ principal resolution; cancel signal @ secondary resolution
                        #1. Reset signal to "none"
                        #2. Reset resolution to main
                        #3. @ next main resolution analysis the previous signal will be detected
                        #4. The sub-signal that was detected made the change to SECONDARY_RESOLUTION will be detected and invalidated
                        self.use_smaller_resolution = False
                        self.SECONDARY_RESOLUTION = None
                        self.signal = None
                        self.signal_time = None
                        self.signal_resolution = None
                        self.signal_validity = None
            return transaction_execution

        # Access to inner functions
        resolution_to_use = self.PRIMARY_RESOLUTION
        execution_time = timestamp
        execution_resolution = resolution
        execution = None

        # Set the variable to False so a stoploss check will execute
        # it is before the Try Except so in case the @1m resolution is being used and
        # a transaction execution results, then the var will change  from False to True and the stoploss will not execute
        if timestamp[0].minute % 5 != 0: #not @ 15 or 5 minute resolution
            execution = False

        try:
            signal_df_slice = self.df_trade_alerts.loc[(slice(timestamp, timestamp)), resolution, (0, 1), (0, 1), (0, 1), (0, 1)]
        except IndexError:
            # no signal for this time/resolution combination
            pass
        else:
            # There is a signal
            try:
                # Is there data?
                data_df_slice = self.df_data.loc[(slice(timestamp, timestamp)), resolution]
            except IndexError:
                # Signal but no data
                pass
            else:
                # There is signal and data: Decide what resolution to use
                if self.SECONDARY_RESOLUTION is not None:
                    resolution_to_use = self.SECONDARY_RESOLUTION
                    if resolution == resolution_to_use:
                        execution = use_secondary_resolution(timestamp, resolution_to_use)
                else:
                    if resolution == resolution_to_use:
                        execution = use_primary_resolution(timestamp, resolution_to_use)

        if execution is not None:
            self.check_transaction_execution(execution_time, execution_resolution, execution)
        return

    # Incomplete. Work in progress 2/5/22
    # Called by analyze_signal_and_data
    def check_transaction_execution(self, execution_time, execution_resolution, transaction_execution):
        timestamp = execution_time
        resolution = execution_resolution
        # executes a regular trade or a correction transaction
        # Pending 2/5/22: code to send transaction
        def execute_transaction(transaction_execution, stoploss):
            # (1/2) Data recording and changes to instance variables
            # returns the values of SD and close to use in either stoploss or transaction
            execution_candle = self.df_data.loc[(slice(timestamp, timestamp)), resolution]
            close = execution_candle[3]
            mins = execution_candle.name[0].minutes
            extra_mins = mins % 15
            sd = 0.0
            if extra_mins % 5 != 0:
                new_ts = execution_candle.name[0] - timedelta(minutes=extra_mins)
                series_for_sd = self.df_data.loc[(slice(new_ts, new_ts)), 5]
                sd = series_for_sd[11]
            elif extra_mins % 5 == 0:
                sd = execution_candle[11]
            elif extra_mins == 0:
                new_ts = execution_candle.name[0] + timedelta(minutes=extra_mins)
                series_for_sd = self.df_data.loc[(slice(new_ts, new_ts)), 5]
                sd = series_for_sd[11]
            # Record data for all transactions
            # Instance Variables to update with any transaction; regular or stoploss
            self.last_sd = sd
            self.last_transaction_close = close  # last transaction price, whether from valid signal or correction
            self.last_transaction_resolution = execution_resolution
            self.last_transaction_timestamp = execution_time
            # Sets the value for a stoploss; if regular transaction, value will be changed in "if transaction_execution:"
            if self.last_transaction is not None:
                if self.last_transaction == "BUY":
                    self.last_transaction = "SELL"  # Buy, Sell. The last transaction, whether from valid signal or correction
                elif self.last_transaction == "SELL" :
                    self.last_transaction = "BUY"
            # record data exclusive to regular transactions
            if transaction_execution:
                # There is a transaction to execute
                # Instance Variables to update with regular transaction only
                # if it's a regular transaction, the it will be the same as signal
                self.last_transaction = self.signal
                # change last valid signal to current signal
                self.last_valid_signal = self.signal  # Buy, Sell. The last signal that prompted an execution
                self.last_valid_signal_resolution = self.signal_resolution
                self.last_valid_signal_time = self.signal_time
                # Instance variables about the current signal that is being evaluated need to be reset to 'no signal'
                self.signal_index = None
                self.signal = None  # Buy, Sell
                self.signal_time = None
                self.signal_resolution = None
                self.signal_validity = None  # Valid, Invalid, Wait
                self.SECONDARY_RESOLUTION = None
                self.use_smaller_resolution = False
            # Set new stoploss
            set_stoploss_limit()
            # (2/2) Process transaction

            # Code goes here to send transaction to module that processes with exchange

            # Record paper trade
            paper_trade()
            return

        def paper_trade():
            COMMISSION = 0.0008
            current_balance = self.paper_balance_amount
            current_currency = self.paper_balance_currency
            c = self.last_transaction_close
            new_balance = 0.0
            new_currency = None

            if current_currency == "ETH":
                new_balance = current_balance*c*(1-COMMISSION)
                new_currency = "USDT"
            else: #currency is USDT
                new_balance = (current_balance/c)*(1-COMMISSION)
                new_currency = "ETH"

            # update the paper trade instance variables
            self.paper_balance_amount = new_balance
            self.paper_balance_currency = new_currency

            # transaction information to record
            time_ex = execution_time
            timeline_ex = execution_resolution
            time_s = self.last_valid_signal_time
            timeline_s = self.last_valid_signal_resolution
            signal = self.last_valid_signal
            c = self.last_transaction_close
            limit = self.stoploss_limit
            stop_loss = stoploss
            trans = transaction_execution
            balance = self.paper_balance_amount
            currency = self.paper_balance_currency

            append = {"time_ex": [time_ex], "timeline_ex": [timeline_ex], "time_s": [time_s], "timeline_s": [timeline_s],
                      "signal": [signal], "c": [c], "limit": [limit], "stoploss": [stop_loss], "transaction": [trans],
                      "balance": [balance], "currency": [currency]}
            df_append = pd.DataFrame.from_dict(append)
            self.df_transactions = self.df_data.append(df_append, ignore_index=False)

            csv_data = self.df_data_path + '/' + self.date_for_file + 'transactions.csv'
            self.df_data.to_csv(csv_data, index=True, mode='a', header=False)


        # decides whether a  stoploss is needed or not
        # Finished 2/4/2021
        def analyze_stoploss():
            stoploss = False
            minutes = timestamp[0].minute
            limit = self.stoploss_limit
            transaction = self.last_transaction
            last_price = self.last_transaction_close

            # 1 1m candle that goes past 2x limit will trigger stoploss
            def check_1_min():
                try:
                    # slice to find data with the timestamp, resolution
                    data_df_slice = self.df_data.loc[(slice(timestamp, timestamp)), resolution]
                except IndexError:
                    # No data of the timestamp, resolution; nothing happens
                    pass
                else:
                    # There is data, retrieve the price close
                    close = data_df_slice[3]
                    if transaction == "BUY":
                        if close < last_price - (limit*2):
                            stoploss = True
                            # execute stoploss
                    else: #transaction == "SELL"
                        if close > last_price + (limit*2):
                            stoploss = True
                            # execute stoploss
                return stoploss

            # 3 5m candles closing under or over limit will trigger stoploss
            def check_5_min():
                start = None
                extra_min = minutes % 5

                # If the previous transaction was a stoploss triggered @1m (5:26), then this sets the start time
                # at 5:25 so that the 5m candle can be taken into account
                if extra_min != 0:
                    start =timestamp - timedelta(minutes=extra_min)
                else:
                    start = self.last_transaction_timestamp

                end = timestamp
                check_stoploss_df = self.df_data.loc[(slice(start, end), 5), :]
                index_list = check_stoploss_df.index
                candle_colors = []
                candle_past_limit = []
                result_list = []

                # Assign color to the candles
                for idx in index_list:
                    if self.df_data.loc[idx, 'c'] > self.df_data.loc[idx, 'o']:
                        candle_colors.append('green')
                    else:
                        candle_colors.append('red')
                    # Assign positive value c > limit; negative otherwise
                    if self.df_data.loc[idx, 'c'] > limit:
                        candle_past_limit.append(1)
                    else:
                        candle_past_limit.append(-1)

                # combine red and neg; green and pos; record results in list
                if self.last_transaction == "BUY":
                    for i in range (len(candle_colors)):
                        if candle_colors[i] == 'red' and candle_past_limit[i] == -1:
                            result_list.append(1)
                        else:
                            result_list.append(0)
                else:
                    for i in range (len(candle_colors)):
                        if candle_colors[i] == 'green' and candle_past_limit[i] == 1:
                            result_list.append(1)
                        else:
                            result_list.append(0)

                if result_list.count(1) > 2:
                    stoploss = True
                    return(stoploss)
                    # execute stoploss

            # 1  min check needs to run every minute
            stoploss = check_1_min()
            # if the above does not trigger, check the 5m
            if minutes % 5 == 0 and not stoploss:
                temp_stoploss = check_5_min()
                if temp_stoploss:
                    stoploss = temp_stoploss
            return stoploss

        # transaction_close is the close of the candle that executed the transaction
        # Finished 2/4/2021
        def set_stoploss_limit(sd):
            pct_limit = 0.0025  # This number is not in percentage points (0.0025 = 0.25%)
            sd_limit = 0.25  # As a percent of SD. Same as above (0.25 = 25%)
            price = self.last_transaction_close

            limit_pct = price * pct_limit
            limit_sd = price * sd

            limit = limit_pct if limit_pct > limit_sd else limit_sd
            self.stoploss_limit = limit
            return

        # executes a transaction or a stoploss check
        # Finished 2/4/2021
        if not transaction_execution:
            stoploss = analyze_stoploss()
            execute_transaction(transaction_execution, stoploss)
        else:
            execute_transaction(transaction_execution, False)
        return












