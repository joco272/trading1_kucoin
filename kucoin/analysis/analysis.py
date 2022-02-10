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

        self.data_all_var = {'time': str(), 'timeline': float(), 's1': int(), 's2': int(), 'b1': int(), 'b2': int(),
                             'nt': int(), 'o': float(), 'h': float(), 'l': float(), 'c': float(), 'basis': float(),
                             'SD': float(), 'upper2': float(), 'upper1': float(), 'lower2': float(), 'lower1': float(),
                             'vol': float(), 'rsi': float()}

        # instantiate dataframes needed in the analysis
        self.df_data_all = pd.DataFrame(columns=self.data_all_var, index=[])
        self.df_ticker = pd.DataFrame(self.ticker_var, index=[])

        # set index of some dfs to `time`
        self.date = datetime.strptime("2021-01-01 00:00", "%Y-%m-%d %H:%M")
        self.init = pd.DataFrame({'time': [self.date]})
        # dummy data to append as first row
        self.init_data_all = pd.DataFrame(
            {'time': [self.date], 'timeline': [0], 's1': ['s1'], 's2': ['s2'], 'b1': ['b1'],
             'b2': ['b2'], 'nt': ['nt'], 'o': ['o'], 'h': ['h'], 'l': ['l'], 'c': ['c'],
             'basis': ['basis'], 'sd': ['SD'], 'lower2': ['u2'], 'upper2': ['l2'],
             'lower1': ['l1'], 'upper1': ['u1'], 'vol': ['vol'], 'RSI': ['RSI']})
        self.df_data_all = self.df_data_all.append(self.init_data_all, ignore_index=False)  # append dummy data as 1st row
        # set the index when df has 1st row w/dummy data. Ready to append rows
        self.df_data_all = self.df_data_all.set_index(['time', 'timeline', 's1', 's2', 'b1', 'b2', 'nt'])

        # Constants for the maximum rows of each df
        self.MAX_SIZE_df_data_all = 750
        self.MAX_SIZE_df_ticker = 500

        # variables needed to save the data in the dataframe as csv
        self.date_time = datetime.now()
        self.time = self.date_time.strftime("%Y_%m_%d-%H-%M-%S")
        self.date_for_file = self.date_time.strftime("%Y_%m_%d")
        self.ticker_path = 'D:/Dropbox/Trading/James/csv/ticker'
        self.df_data_all_path = 'D:/Dropbox/Trading/James/csv/data_all'

        # trade signal variables:
        self.RSI_OVERBOUGHT = 64
        self.RSI_OVERSOLD = 33

        # time resolution that the algorithm is working with
        self.RESOLUTIONS = [15, 5, 1]  # time resolutions available for teh algo to use
        self.PRIMARY_RESOLUTION = 15  # default working resolution
        self.SECONDARY_RESOLUTION = None
        self.use_smaller_resolution = False

        # THE MOST RECENT SIGNAL, in the process of being evaluated
        # the index of the df row that changed the state to Buy or Sell
        self.signal_index = None
        # Signal type
        self.signal = None  # Buy, Sell
        self.signal_time = None
        self.signal_resolution = None
        # state of the analysis object: Idle (waiting for signal), Buy (waiting for), Sell (Waiting for)
        self.signal_validity = None  # Valid, Invalid, Wait

        # Last executed transaction
        self.last_valid_signal = None  # Buy, Sell. The last signal that prompted an execution
        self.last_valid_signal_time = None
        self.last_valid_signal_resolution = None
        self.last_transaction = None  # Buy, Sell. The last transaction, whether from valid signal or correction
        self.last_transaction_timestamp = None  # of the candle that triggered the last_transaction
        self.last_transaction_resolution = None  # of the candle that triggered the last_transaction
        self.last_transaction_close = None  # of the candle that triggered the last_transaction
        self.last_sd = None  # standard dev (5 min resolution) of last transaction, whether from valid signal or correction
        self.stoploss_limit = None

        # instance variables for paper trading
        self.init_transactions = pd.DataFrame(
            {'time_ex': [self.date], 'timeline_ex': ['timeline_ex'], 'time_s': ['time_s'], 'timeline_s': ['timeline_s'],
             'signal': ['signal'], 'c': ['c'], 'limit': ['limit'], 'stoploss': ['stoploss'],
             'transaction': ['transaction'], 'balance': ['balance'], 'currency': ['currency']})
        self.df_transactions = self.init_transactions.append(self.init_transactions, ignore_index=False)  # append dummy data as 1st row
        self.df_transactions_path = 'D:/Dropbox/Trading/James/paper_trading'
        self.paper_balance_amount = 1
        self.paper_balance_currency = "ETH"

    def change_date_for_file(self, date):
        self.date_for_file = date

    # This code saves the ticker to CSV file. No header
    def merge_ticker(self, df):
        csv_out = self.ticker_path + '/' + self.date_for_file + '_ticker.csv'

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

    # Receive data_all from webhooks processor
    # append to data_all df
    # write new info to csv
    # No merge necessary
    def merge_data_all(self, df):
        # ===================================Display and print======================================
        pd.set_option('display.max_columns', None)
        # pd.set_option("expand_frame_repr", False)
        print(f'{self.time} merge_close function called with', df)
        # df = df.set_index(['time'])
        self.df_data_all = self.df_data_all.append(df, ignore_index=False)
        print(f'{self.time} Close-df appended-size is:', self.df_data_all.shape[0], '\n', self.df_data_all)
        csv_data_all = self.df_data_all_path + '/' + self.date_for_file + 'data_all.csv'
        self.df_data_all.to_csv(csv_data_all, index=True, mode='a', header=False)
        # ===================================Start new Merge method======================================

        df = df.set_index(['time', 'timeline', 's1', 's2', 'b1', 'b2', 'nt'])

        self.df_data_all = self.df_data_all.append(df)
        csv_data = self.df_data_all_path + '/' + self.date_for_file + '_data_all.csv'
        self.df_data_all.to_csv(csv_data, index=True, mode='a', header=False)

        if self.df_data_all.shape[0] > self.MAX_SIZE_df_data_all:
            self.df_data = self.df_data_all[self.df_data_all.shape[0] - self.MAX_SIZE_df_data_all:]
        print(f'{self.time} data_all merge finished-DATA_ALL size is:', self.df_data.shape[0], '\n', self.df_data)

        index_list = df.index
        for idx in index_list:
            self.analyze_signal_and_data(idx)
        return

    # this function is called by merge_data_all. Needs to come before
    def analyze_signal_and_data(self, idx):
        # every candle and every action signal at any resolution should trigger this function

        # 2/7/22: Updated for simpler signals handling
        def check_signals_complete(df_index):  # Check that sub-signals are complete.
            # takes the index as an argument to check if all subsignals have been received
            sub_signals_complete = False

            start_sub = df_index[0]
            check_complete_tl = df_index[1]
            end_sub = start_sub + timedelta(minutes=check_complete_tl - 1)
            if check_complete_tl != self.RESOLUTIONS[-1]:  # does idx have the smallest resolution?
                # if it is the smallest resolution then there are no sub-signals
                sub_signal_res = self.RESOLUTIONS[self.RESOLUTIONS.index(check_complete_tl) + 1]
                resolutions_to_use = self.RESOLUTIONS[self.RESOLUTIONS.index(check_complete_tl):]
                try:
                    sub_signals_df = self.df_data_all.loc[
                                     (slice(start_sub, end_sub), resolutions_to_use, (0, 1), (0, 1), (0, 1), (0, 1),
                                      (0, 1)), :]
                except KeyError:
                    pass
                    # print(f'no sub-signals found for {df_index}')
                    # need to update code. Probably pass? Log to CSV?
                else:
                    sub_signals_df_shape = 0
                    for resolution_to_use in resolutions_to_use:
                        sub_signals_df_shape += resolutions_to_use[0] / resolution_to_use
                    if sub_signals_df.shape[0] == sub_signals_df_shape:  # sub-signals are complete
                        print('sub-signals are complete:', sub_signals_df.shape[0])
                        sub_signals_complete = True  # Added 1/30/22. Make sure I didn't mess up
            return sub_signals_complete  # end of inner function 'check_complete'

        # 2/7/22: Updated for simpler signals handling; now detects buy2 and sell2
        def detect_signal(resolution, start=None):
            # Helper function for use resolution functions. Helps not repeat code
            # Finished 12/13 18:23
            # Untested
            """
            What return values should be used??????
            """
            signal_index = None
            signal = None

            start_df = None
            end_df = None

            # By default assume principal resolution is in use
            start_df = self.df_data_all.iloc[0, 0]
            end_df = self.df_data_all.iloc[-1, 0]

            if start is not None:  # When secondary resolution is in use change start
                start_df = start

            try:  # Is there a SELL signal?
                check_sell_1_1 = self.df_data_all.loc[
                                 (slice(start_df, end_df), (resolution), (1), (0, 1), (0, 1), (0, 1), (0, 1)), :]
            except KeyError:
                # No sell signal
                try:  # IS there a BUY signal?
                    check_buy_1 = self.df_data_all.loc[
                                  (slice(start_df, end_df), (resolution), (0, 1), (0, 1), (1), (0, 1), (0, 1)), :]
                except KeyError:
                    # No buy Signal
                    pass
                else:  # BUY signal detected
                    check_buy = check_buy_1.iloc[-1]
                    print('Buy signal was detected at', check_buy.name)
                    signal_index = check_buy.name
                    signal = 'buy1'
                    try:
                        # IS there a BUY 2 signal?
                        check_buy_2 = self.df_data_all.loc[
                                      (slice(start_df, end_df), (resolution), (0, 1), (0, 1), (0, 1), (1), (0, 1)), :]
                    except KeyError:
                        # No buy 2 Signal
                        pass
                    else:  # BUY 2 signal detected
                        check_buy = check_buy_2.iloc[-1]
                        print('Buy signal was detected at', check_buy_2.name)
                        signal_index = check_buy.name
                        signal = 'buy2'
            else:  # Sell signal detected
                check_sell_1 = check_sell_1_1.iloc[-1]
                signal_index = check_sell_1
                signal = 'sell1'
                try:  # Is there a SELL2 signal?
                    check_sell_2_1 = self.df_data_all.loc[
                                     (slice(start_df, end_df), (resolution), (0, 1), (1), (0, 1), (0, 1), (0, 1)), :]
                except KeyError:
                    # No sell2 signal, but sell1 signal
                    pass
                else:
                    # Sell 2 signal
                    check_sell_2 = check_sell_2_1.iloc[-1]
                    signal_index = check_sell_2
                    signal = 'sell2'
            # end detect_signal
            return signal, signal_index

        # 2/7/22: Updated for simpler signals handling; should probably be updated to receive idx instead of (timestamp, resolution)
        def check_signal_validity(signal, signal_index):
            signal_time = signal_index[0]
            execution_time = None
            execution_resolution = None

            # for buy1, sell1
            def signal_1():
                change_counter = 0
                changes_list = []
                end_time = self.df_data.iloc[-1].name[0]  # item[0] of the series' name (index) is tiemstamp
                check_transaction_df = self.df_data.loc[(slice(signal_time, end_time), resolution), :]
                index_list = check_transaction_df.index
                last_row = index_list[-1]
                execution_time = last_row[0]
                execution_resolution = last_row[1]
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
                    if self.df_data.loc[last_row, 'upper1'] > self.df_data.loc[last_row, 'c'] > self.df_data.loc[last_row, 'lower1']:
                        # Diagram outcome 3
                        signal_status = 'Valid'
                    else:
                        # Diagram outcome 2
                        signal_status = 'Wait'
                else:
                    # Diagram outcome 1
                    signal_status = 'Invalid'
                # end signal_1
                return signal, signal_status, execution_time, execution_resolution

            # for buy2, sell2
            def signal_2():
                change_counter = 0
                changes_list = []
                end_time = self.df_data.iloc[-1].name[0]  # item[0] of the series' name (index) is tiemstamp
                check_transaction_df = self.df_data.loc[(slice(signal_time, end_time), resolution), :]
                index_list = check_transaction_df.index
                last_row = index_list[-1]
                signal_status = None
                execution_time = last_row[0]
                execution_resolution = last_row[1]
                # Has the color of the candles (direction of close) changed only once?
                # (1/2) Assign color to the candles
                for idx in index_list:
                    if self.df_data.loc[idx, 'c'] > self.df_data.loc[idx, 'o']:
                        changes_list.append('green')
                    else:
                        changes_list.append('red')

                # (2/2) Count the times the color of the candles changes
                for i in range(1, len(changes_list)):
                    if changes_list[i - 1] != changes_list[i]:
                        change_counter += 1
                # Diagram decision 1: Did te direction of the closing price change more than once?
                if change_counter == 0:
                    signal_status = 'Wait'
                elif change_counter == 1:
                    signal_status = 'Valid'
                else:
                    signal_status = 'Invalid'
                # end signal_2
                return signal, signal_status, execution_time, execution_resolution

            # decide to use signal_1 or signal_2
            # buy2 and sell2 are a sub-type of buy1 and sell1
            signal_status = None
            if signal == 'buy1' or signal == 'sell1':
                signal, signal_status, execution_time, execution_resolution = signal_1()
            elif signal == 'buy2' or signal == 'sell2':
                signal, signal_status, execution_time, execution_resolution = signal_2()
                #if signal 2 is invalid, it should continue status as signal 1
                if signal_status == 'Invalid':
                    signal, signal_status = signal_1()
                    signal = signal[:len(signal)-1]+'1'
            return signal, signal_status, execution_time, execution_resolution

        # 2/7/22: Calls detect_signal(), which returns (signal, index)
        # The use primary and use secondary functions analyze signals and data and send the execution signal
        # Replaces use_primary() abd use_secondary()
        def perform_analysis(idx):
            # will work with primary or secondary resolution

            self.df_data_all.sort_index(inplace=True)
            index_list = self.df_data_all.index
            resolution = idx[1]
            transaction_execution = False
            execution_time = None
            execution_resolution = None
            # The above vars will be returned unchanged if the idx does not have primary or secondary resolution
            # this can be used to ensure that analysis runs before stoploss check in a case when
            # @ 1m data is received before @5m or @15m. For example:
            # 12:15. data @1m is received before @15m and triggers a stoploss before analysis triggers the correct transaction
            # Running analysis before stoploss prevents this

            signal = None
            signal_status = None
            signal_index = None
            sub_signal = None
            sub_signal_status = None
            sub_signal_index = None

            if resolution == self.PRIMARY_RESOLUTION:
                if check_signals_complete(idx): # data has all the sub-signals
                    signal, signal_index = detect_signal(self.PRIMARY_RESOLUTION)
                    if signal is not None: # there is a signal
                        # check for sub-signal
                        start = signal_index[0]
                        sub_signal, sub_signal_index = detect_signal(
                            self.RESOLUTIONS[self.RESOLUTIONS.index(self.PRIMARY_RESOLUTION) + 1], start)
                        if sub_signal is not None:  # there is a sub-signal
                            sub_signal, sub_signal_status, execution_time, execution_resolution = check_signal_validity(sub_signal, sub_signal_index)
                            if sub_signal_status == 'Valid': # Need to do or return something else?
                                # execute transaction
                                transaction_execution = True
                            elif sub_signal_status == 'Wait':
                                # Wait @ secondary resolution
                                self.signal_index = sub_signal_index
                                self.signal = sub_signal
                                self.signal_time = sub_signal_index[0]
                                self.signal_resolution = sub_signal_index[1]
                                self.use_smaller_resolution = True
                                self.SECONDARY_RESOLUTION = sub_signal_index[1]
                            elif sub_signal_status == 'Invalid':
                                # nothing needs to be done, since principal resolution is being used and will remain in use
                                pass
                        else: # no sub-signal
                            signal, signal_status, execution_time, execution_resolution = check_signal_validity(signal, signal_index)
                            if signal_status == 'Valid': # Need to do or return something else?
                                # execute transaction
                                transaction_execution = True
                            elif signal_status == 'Wait':
                                self.signal_index = signal_index
                                self.signal = signal
                                self.signal_time = signal_index[0]
                                self.signal_resolution = signal_index[1]
                                self.use_smaller_resolution = False
                                self.SECONDARY_RESOLUTION = signal_index[1]
                            elif signal_status == 'Invalid':
                                self.signal_index = None
                                self.signal = None
                                self.signal_time = None
                                self.signal_resolution = None
                                self.use_smaller_resolution = False
                                self.SECONDARY_RESOLUTION = None
            elif resolution == self.SECONDARY_RESOLUTION:
                signal, signal_index = detect_signal(self.SECONDARY_RESOLUTION)
                if signal is not None:  # there is a signal
                    signal, signal_status, execution_time, execution_resolution = check_signal_validity(signal, signal_index)
                    if signal == 'Valid':
                        # execute transaction
                        transaction_execution = True
                    elif signal == 'Wait':
                        # Wait @ secondary resolution
                        self.signal_index = sub_signal_index
                        self.signal = sub_signal
                        self.signal_time = sub_signal_index[0]
                        self.signal_resolution = sub_signal_index[1]
                        self.use_smaller_resolution = True
                        self.SECONDARY_RESOLUTION = sub_signal_index[1]
                    elif signal == 'Invalid':
                        # Revert to principal resolution
                        self.SECONDARY_RESOLUTION = None
                        self.use_smaller_resolution = False
            return transaction_execution, execution_time, execution_resolution

        # Access to inner functions
        timestamp = idx[0]
        resolution = idx[1]

        # Needs to run at every resolution in case signals arrive out of order
        transaction_execution, execution_time, execution_resolution = perform_analysis(idx)
        # The transaction_execution dictates if a transaction or correction (stoploss) will be performed
        # if it is None, no analysis was conducted; if True a transaction will be executed; if False a correction
        # a correction decides whether a stoploss should be executed or not

        if timestamp[0].minute % 5 != 0:  # will execute only @1m
            # Set the time and resolution for the stoploss check according to the idx
            self.check_transaction_execution(idx[0], idx[1], False)
        else:  # if minutes are a multiple of 5
            if transaction_execution is not None:  # if the analysis has been conducted then check stoploss
                self.check_transaction_execution(execution_time, execution_resolution, transaction_execution)
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
                elif self.last_transaction == "SELL":
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
                new_balance = current_balance * c * (1 - COMMISSION)
                new_currency = "USDT"
            else:  # currency is USDT
                new_balance = (current_balance / c) * (1 - COMMISSION)
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

            append = {"time_ex": [time_ex], "timeline_ex": [timeline_ex], "time_s": [time_s],
                      "timeline_s": [timeline_s],
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
                        if close < last_price - (limit * 2):
                            stoploss = True
                            # execute stoploss
                    else:  # transaction == "SELL"
                        if close > last_price + (limit * 2):
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
                    start = timestamp - timedelta(minutes=extra_min)
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
                    for i in range(len(candle_colors)):
                        if candle_colors[i] == 'red' and candle_past_limit[i] == -1:
                            result_list.append(1)
                        else:
                            result_list.append(0)
                else:
                    for i in range(len(candle_colors)):
                        if candle_colors[i] == 'green' and candle_past_limit[i] == 1:
                            result_list.append(1)
                        else:
                            result_list.append(0)

                if result_list.count(1) > 2:
                    stoploss = True
                    return (stoploss)
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
