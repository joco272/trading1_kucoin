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

        self.data_all_var = {'time': str(), 'timeline': float(), 'b1': int(), 's1': int(), 'b2': int(), 's2': int(),
                             'nt': int(), 'o': float(), 'h': float(), 'l': float(), 'c': float(), 'basis': float(),
                             'SD': float(), 'upper2': float(), 'upper1': float(), 'lower2': float(), 'lower1': float(),
                             'vol': float(), 'rsi': float()}

        # instantiate dataframes needed in the analysis
        self.df_data_all = pd.DataFrame(columns=self.data_all_var, index=[])
        self.df_ticker = pd.DataFrame(self.ticker_var, index=[])

        # change D:/Dropbox to C:/Users/jocox/Dropbox
        # when changing from personal to OLLU and viceversa

        # set index of some dfs to `time`
        self.date = datetime.strptime("2021-01-01 00:00", "%Y-%m-%d %H:%M")
        self.init = pd.DataFrame({'time': [self.date]})
        # dummy data to append as first row
        self.init_data_all = pd.DataFrame(
            {'time': [self.date], 'timeline': [0], 'b1': ['b1'], 's1': ['s1'], 'b2': ['b2'],
             's2': ['s2'], 'nt': ['nt'], 'o': ['o'], 'h': ['h'], 'l': ['l'], 'c': ['c'],
             'basis': ['basis'], 'SD': ['sd'], 'upper2': ['u2'], 'upper1': ['u1'],
             'lower2': ['l2'], 'lower1': ['l1'], 'vol': ['vol'], 'RSI': ['RSI']})
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
        self.ticker_path = 'C:/Users/jocox/Dropbox/Trading/James/csv/ticker'
        self.df_data_all_path = 'C:/Users/jocox/Dropbox/Trading/James/csv/data_all'

        # trade signal variables:
        self.RSI_OVERBOUGHT = 64
        self.RSI_OVERSOLD = 33

        # time resolution that the algorithm is working with
        self.RESOLUTIONS = [45, 15]  # time resolutions available for teh algo to use
        self.PRIMARY_RESOLUTION = 45  # default working resolution
        self.SECONDARY_RESOLUTION = 15
        self.use_smaller_resolution = False

        # Signals Data Frame stores signals and their state.
        # Possibles states for the signals are: waiting, invalid (did not meet execution criteria), valid (executed)
        # signal: the signal that was received. Avoids additional df slicing
        # working_res: The resolution in use when the signal was recorded
        self.signals_var = {'time': str(), 'timeline': float(), 'b1': int(), 's1': int(), 'b2': int(), 's2': int(),
                            'nt': int(), 'state': str(), 'signal': str(), 'working_res': int()}
        self.init_signals = pd.DataFrame(
            {'time': [self.date], 'timeline': [0], 'b1': [0], 's1': [0], 'b2': [0], 's2': [0], 'nt': [1],
             'state': 'None', 'signal': 'None', 'working_res': [0]})
        self.df_signals = pd.DataFrame(columns=self.signals_var)
        #print('============SIGNALS DF========================')
        #print(self.df_signals)
        # the DF needs data before the index can be defined
        # self.df_signals = self.df_signals.append(self.init_signals, ignore_index=False)
        # self.df_signals.set_index(['time', 'timeline', 'b1', 's1', 'b2', 's2', 'nt', 'state'], inplace=True)
        # Ensure the DF starts empty
        # drop_idx = self.df_signals.index[0]
        # self.df_signals.drop(drop_idx, inplace=True)


        # record transactions for paper trading and comparison with actual performance
        self.transactions_var = {'time_ex': str(), 'timeline_ex': float(), 'signal_time': str(), 'timeline': float(),
                                 'signal': str(), 'c': float(), 'limit': float(), 'stoploss': bool(),
                                 'transaction': str(), 'balance': float(), 'currency': float()}
        self.df_transactions = pd.DataFrame(columns=self.transactions_var)
        self.df_transactions_path = 'C:/Users/jocox/Dropbox/Trading/James/paper_trading'
        #  1/23/22 @ 23:45Z: Sell1; 1/24/22 @ 00:00Z execute @ 15m, close = $2,516.17
        #  comission 0.01% -20% for paying with Kucoin token = 0.008%
        # starting balance = $2,516.17 (1 ETH eq) - 0.008% = $2,514.16 USDT
        # 2/27 20:30 Buy ETH @ 2626.64; 2617.44 @20:05 = 0.992 ETH
        self.last_transaction = None
        self.last_transaction_close = None
        self.paper_balance_amount = 1
        self.paper_balance_currency = "ETH"

        # set headers for csv files
       #csv_data_all = self.df_data_all_path + '/' + self.date_for_file + '_data_all.csv'
        #self.df_data_all.to_csv(csv_data_all, index=True, mode='a', header=True)
        #self.df_data_all = self.df_data_all[:0]

    def change_date_for_file(self, date):
        self.date_for_file = date

    def change_signal_state(self, signal_index, state, stale_signal = False):
        # change the state of the signal
        # from wait to valid, invalid, or override
        # if a signal becomes valid, then change to valid
        # If a signal becomes invalid, then change to invalid
        # If a sub-signal or signal sub-type become valid, then other signals in wait state should change to override
        #print(f'106- Changing signal:\n{signal_index} to state {state}')
        #temp = self.df_signals.loc[signal_index]
        temp = self.df_signals.loc[
               (slice(signal_index[0], signal_index[0]), signal_index[1], signal_index[2], signal_index[3],
                signal_index[4], signal_index[5], signal_index[6], signal_index[7]), :]
        temp_name = temp.index
        timestamp = [temp_name[0][0]]
        tl = [temp_name[0][1]]
        b1 = [temp_name[0][2]]
        s1 = [temp_name[0][3]]
        b2 = [temp_name[0][4]]
        s2 = [temp_name[0][5]]
        nt = [temp_name[0][6]]
        state = [state]
        signal = [temp.iloc[0][0]]
        working_res = [temp.iloc[0][1]]

        dict_to_append = {'timestamp': timestamp, 'tl': tl, 'b1': b1, 's1': s1, 'b2': b2, 's2': s2, 'nt': nt,
                          'state': state, 'signal': signal, 'working_res': working_res}
        signals_to_append = pd.DataFrame.from_dict(dict_to_append)
        signals_to_append.set_index(['timestamp', 'tl', 'b1', 's1', 'b2', 's2', 'nt', 'state'], inplace=True)

        self.df_signals.drop(temp_name, inplace=True)
        self.df_signals = self.df_signals.append(signals_to_append, ignore_index=False)
        self.df_signals.sort_index(inplace=True)

        if not stale_signal:
            # pass
            self.change_stale_state(signal_index)
        return

    def change_stale_state(self, signal_index):
        """
        This function ensures that when a signal is received in change_signal_state, old signals of the same reslution
        are invalidated to state 5. It does so by calling change_signal_state
        An infinite loop is avoided by sending the stale_signal=True. This indicates that the change signal request
        is sent from change_signal_state so these functions are not calle recursively
        """

        end = signal_index[0]
        resolution = signal_index[1]
        start = self.df_signals.iloc[0].name[0]
        df_to_update = None
        signals_to_update = None

        try:
            df_to_update = self.df_signals.loc[(slice(start, end), resolution,
                                                (0, 1), (0, 1), (0, 1), (0, 1), (0, 1), 0), :]
        except KeyError:
            pass  # Nothing needs to be updated

        if df_to_update is not None:
            if isinstance(df_to_update, pd.core.frame.DataFrame):
                signals_to_update = df_to_update.index
                pass
            else:
                signals_to_update = df_to_update.names

            if signals_to_update is not None:
                for signal in signals_to_update:
                    self.change_signal_state(signal, 5, stale_signal=True)
        return

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
        # pd.set_option('display.max_columns', None)
        # pd.set_option("expand_frame_repr", False)
        # print(f'{self.time} merge_close function called with', df)
        # df = df.set_index(['time'])
        # self.df_data_all = self.df_data_all.append(df, ignore_index=False)
        # print(f'{self.time} Close-df appended-size is:', self.df_data_all.shape[0], '\n', self.df_data_all)
        # csv_data_all = self.df_data_all_path + '/' + self.date_for_file + 'data_all.csv'
        # self.df_data_all.to_csv(csv_data_all, index=True, mode='a', header=False)
        # ===================================Start new Merge method======================================

        # df = df.set_index(['time', 'timeline', 'b1', 's1', 'b2', 's2', 'nt'])
        # print('===========df to append========\n', df)
        # print(f'DF from reader to append is {df}')
        self.df_data_all = self.df_data_all.append(df)
        csv_data_all = self.df_data_all_path + '/' + self.date_for_file + '_data_all.csv'
        df.to_csv(csv_data_all, index=True, mode='a', header=False)

        if self.df_data_all.shape[0] > self.MAX_SIZE_df_data_all:
            self.df_data_all = self.df_data_all[self.df_data_all.shape[0] - self.MAX_SIZE_df_data_all:]
        # print(f'{self.time} data_all merge finished-DATA_ALL size is:', self.df_data_all.shape[0], '\n', self.df_data_all)

        index_list = df.index
        for idx in index_list:
            self.analyze_signal_and_data(idx)
        return

    # this function is called by merge_data_all. Needs to come before
    def analyze_signal_and_data(self, idx):
        # every candle and every action signal at any resolution should trigger this function
        #print(f'analyze_signal_and_data({idx})')

        # 2/7/22: Updated for simpler signals handling
        def check_signals_complete(df_index, resolution):  # Check that sub-signals are complete.
            # takes the index as an argument to check if all subsignals have been received
            #print(f'check_signals_compete({df_index}, {resolution})')
            sub_signals_complete = False
            # start_sub = df_index[0]
            check_complete_tl = resolution  # df_index[1]
            minutes = idx[0].minute
            start_delta = 0
            # Obtain the start time of the candle @ main resolution
            try:
                start_delta = minutes % resolution
            except TypeError:
                print('index:', idx)
                print('minutes:', minutes)
                print('resolution:', resolution)
                print('use smaller resolution:', self.use_smaller_resolution)
            else:
                if not isinstance(start_delta, int):
                    start_delta = start_delta.item()

            start_sub = df_index[0] - timedelta(minutes=start_delta)

            end_delta = check_complete_tl-1
            if not isinstance(end_delta, int):
                end_delta = end_delta.item()

            end_sub = start_sub + timedelta(minutes=end_delta)

            #print(f'check_signals_complete. Res: {resolution}; idx: {df_index}; start: {start_sub}; end {end_sub}')
            if check_complete_tl != self.RESOLUTIONS[-1]:  # does idx have the smallest resolution?
                # if it is the smallest resolution then there are no sub-signals
                # sub_signal_res = self.RESOLUTIONS[self.RESOLUTIONS.index(check_complete_tl) + 1]
                resolutions_to_use = self.RESOLUTIONS[self.RESOLUTIONS.index(check_complete_tl):]
                #print('resolutions:', resolutions_to_use)

                sub_signals_df_shape = 0
                for resolution_to_use in resolutions_to_use:
                    sub_signals_df_shape += check_complete_tl / resolution_to_use
                try:
                    sub_signals_df = self.df_data_all.loc[
                                     (slice(start_sub, end_sub), resolutions_to_use, (0, 1), (0, 1), (0, 1), (0, 1),
                                      (0, 1)), :]
                except KeyError:
                    pass
                    # print(f'no sub-signals found for {df_index}')
                    # need to update code. Probably pass? Log to CSV?
                else:
                    # sub_signals_df_shape = 0
                    # for resolution_to_use in resolutions_to_use:
                        # sub_signals_df_shape += resolutions_to_use[0] / resolution_to_use
                    #print(f'(1) Actual Shape:', sub_signals_df.shape[0], 'should be:', sub_signals_df_shape)
                    #print (f'(2) ===========sub-signals df========\n{sub_signals_df}')
                    if sub_signals_df.shape[0] == sub_signals_df_shape:  # sub-signals are complete
                        sub_signals_complete = True  # Added 1/30/22. Make sure I didn't mess up
                        #print(f'(3) ================sub-signals complete: {sub_signals_complete}')
                        #print(f'check_signals_complete. Res: {resolution}; idx: {df_index}; start: {start_sub}; end {end_sub}')
            return sub_signals_complete  # end of inner function 'check_complete'

        # 2/7/22: Updated for simpler signals handling; now detects buy2 and sell2
        # 2/22/22 0439. Debug: Working OK with sell1
        def detect_signal(resolution, start_idx=None):
            # Helper function for use resolution functions. Helps not repeat code

            if start_idx is not None:
                start = start_idx[0]
            print(f'293-detect_signal. idx: {idx}, (res: {resolution}, start: {start_idx})')

            signals = []
            signal = None
            start_df = None
            end_df = None

            # By default assume principal resolution is in use
            start_df = self.df_data_all.iloc[0].name[0]
            end_df = self.df_data_all.iloc[-1].name[0]

            # Need to ensure that signals of type 2 trigger a resolution change for  that to happen:
            # Needs to also scan main + 1 resolution for signal of type 2 always
            # Possible consequences and conflicts that may arise of this:
            # 1. A sub-signal may execute before a signal is detected @Main res
            # 1a. After execution, res will revert to main and signal @Main will be valid
            # In this case the main signal would immediately have to be addressed as 'null'
            # A check for signals that need a change to 'override' state needs to be done at the end of this function

            # States of signals in self.df_signals:
            # (0) 'wait': waiting for validation if teh signal will be executed or not
            # (1) 'valid': transaction indicated by signal should execute
            # (2) 'executed': transaction indicated by a 'valid' signal has been executed
            # (3) 'invalid': conditions not met for the execution of the transaction
            # (4) 'override': A sub-signal of smaller resolution triggered the transaction of the signal
            # (5) 'null': signal was not invalidated, but:
            #       (a) a signal of a newer timestamp and same resolution is now valid
            #       (b) a signal of a same timestamp and smaller resolution triggers an execution
            #           sub-signal triggers and executes before signal @Main closes with algo running @Main.
            #           ex: 0915 sell2 signal @5m (wait) becomes valid and executes @ 0920 (algo is running @15m)
            #           (cont) 0915 @15 closes and produces a wait signal after above signal executed
            #       (c)  sub-signal triggers before signal @Main closes and executes when main signal closes (algo running @Main).
        #           (d) wrong signal (received a sell after already sold)
            #           ex: sell2 0915 or 0920 @5m executes @ 0925 when 0915 @15m closes
            #        In (b) and (c) there will be a 'valid' @5m and a 'wait' @15m
            #           Need to write code that distinguishes 'null' from 'override' - type 2 signal is the identifier
            #               Only type 1 signals can be sub-signals. Type 2 trigger a resolution change
            #                   So (b) and (c) fall under the same case
            #               Type 1 signals @Main should only be nullified by Type 2 signals of same or latter timestamp in an 'executed' state

            # determines start_df to ensure that signals are only detected once
            # If self.df_signals.shape[0] != 0 and there are 1 or more signals:
            #   (1) detect_signal() will use (newest signal + 1m) of the current resolution as the starting point:
            # self.df_signals
            self.df_signals.sort_index(inplace=True)

            # implements the explanation above
            # The start is changed to the (newest signal + 1m)
            # finished 2/17/22 1247
            if self.df_signals.shape[0] > 0:
                start_signals = self.df_signals.iloc[0].name
                end_signals = self.df_signals.iloc[-1].name
                try:  # is there a 'wait' signal? (First IF)
                    check_signals = self.df_signals.loc[
                                 (slice(start_signals[0], end_signals[0]),
                                  resolution, (0, 1), (0, 1), (0, 1), (0, 1), (0, 1),
                                  (0, 1, 2, 3, 4, 5)), :]
                except KeyError:  # No signals of the needed res yet since the last signal(2)
                    pass
                else:  # The last signal @ resolution
                    delta = check_signals.iloc[-1].name[1]-1
                    if not isinstance(delta, int):
                        delta = delta.item()
                    start_df = check_signals.iloc[-1].name[0] + timedelta(minutes=delta) #timedelta(minutes=delta)

            # Used when the start time is set (searching for sub-signals
            if start_idx is not None:  # When scanning for sub-signals change df_start the signal timestamp
                start_df = start_idx[0]
                delta = start_idx[1]-1
                if not isinstance(delta, int):
                    delta = delta.item()
                end_df = start_df + timedelta(minutes=delta)
                #print(f'363-start_df = {start_df}, end_df = {end_df}')
            # Start of signal detection
            try:  # Is there a sell1 signal?
                check_sell_1_1 = self.df_data_all.loc[
                                 (slice(start_df, end_df), (resolution), (0, 1), (1), (0, 1), (0, 1), (0, 1)), :]
            except KeyError:  # No sell1 signal
                try:  # Is there a buy1 signal?
                    check_buy_1_1 = self.df_data_all.loc[
                                  (slice(start_df, end_df), (resolution), (1), (0, 1), (0, 1), (0, 1), (0, 1)), :]
                except KeyError:  # No buy1 Signal
                    pass
                else:  # buy1 signal detected. There may also be a buy2, since it's a sub-type of buy1
                    check_buy_1 = check_buy_1_1.iloc[-1]
                    #print('376-Buy1 signal was detected at:', check_buy_1.name)
                    signal = 'buy1'
                    signals.append([check_buy_1.name, signal])
                    try:
                        # IS there a BUY 2 signal?
                        ts = check_buy_1.name[0] # signal has to be in the same time and resolution
                        check_buy_2_1 = self.df_data_all.loc[
                                      (slice(ts, ts), (resolution), (0, 1), (0, 1), (1), (0, 1), (0, 1)), :]
                    except KeyError:
                        # No buy2 Signal, but buy1 signal
                        pass
                    else:  # BUY 2 signal detected
                        check_buy_2 = check_buy_2_1.iloc[-1]
                        #print('Buy2 signal was detected at', signal_index)
                        signal = 'buy2'
                        signals.append([check_buy_2.name, signal])
            else:  # sell1 signal detected; there may be a sell2, since it is a sub-type of sell1
                check_sell_1 = check_sell_1_1.iloc[-1]
                signal = 'sell1'
                signals.append([check_sell_1.name, signal])
                try:  # Is there a SELL2 signal?
                    ts = check_sell_1.name[0]  # signal has to be in the same time and resolution
                    #print(f"#256. ts for signal_index[0] = {ts}")
                    #print(f'#257 signal_index = {signal_index}')
                    check_sell_2_1 = self.df_data_all.loc[
                                     (slice(ts, ts), (resolution), (0, 1), (0, 1), (0, 1), (1), (0, 1)), :]
                except KeyError:
                    # No sell2 signal, but sell1 signal
                    pass
                else:
                    # sell2 signal
                    check_sell_2 = check_sell_2_1.iloc[-1]
                    signal = 'sell2'
                    signals.append([check_sell_2.name, signal])
            # End of signal detection
            # Are there type 1 and type 2 signals of the same index? Keep type 2 only
            #print(f'signals list before:\n {signals})')
            #if len(signals) > 0:
                #print(f'First signal:', signals[0][0])
            for i in range(1, len(signals)):
                if signals[i][0] == signals[i-1][0]:
                    if signals[i][1] > signals[i-1][1]:  # buy2 > buy1?
                        signals.remove([signals[i-1][0], signals[i-1][1]])  # keep buy2
                    else:
                        signals.remove([signals[i][0], signals[i][1]])
            #print(f'381-================\nsignals {idx} at res {resolution}:')
            #print(f'382-Signals list:\n{signals}')
            #for signal in signals:
                #print(signal)

            # update self.df_signals
            timestamp, tl, b1, s1, b2, s2, nt, state, signals_lst, res = [], [], [], [], [], [], [], [], [], []

            signals_to_append =None
            num_signals = 0
            if len(signals) > 0:
                #print(f'390-len(signals: {len(signals)}')
                #print(f'391-Signals:{signals}')
                for signal in signals:
                    #print('=========signal========\n',signal)
                    #print('=========signal[0]========\n',signal[0])
                    #print('=========signals_lst========\n',signals_lst)
                    timestamp.append(signal[0][0])
                    tl.append(signal[0][1])
                    b1.append(signal[0][2])
                    s1.append(signal[0][3])
                    b2.append(signal[0][4])
                    s2.append(signal[0][5])
                    nt.append(signal[0][6])
                    state.append(0)
                    signals_lst.append(signal[1])
                    res.append(self.SECONDARY_RESOLUTION if self.use_smaller_resolution else self.PRIMARY_RESOLUTION)

                dict_to_append = {'timestamp': timestamp, 'tl': tl, 'b1': b1, 's1': s1, 'b2': b2, 's2': s2, 'nt': nt,
                                  'state': state, 'signal': signals_lst, 'working_res': res}

                #print('===========dict=======\n', dict_to_append)
                # convert dict to df and then set index, then append
                signals_to_append = pd.DataFrame.from_dict(dict_to_append)
                num_signals = signals_to_append.shape[0]
                signals_to_append.set_index(['timestamp', 'tl', 'b1', 's1', 'b2', 's2', 'nt', 'state'], inplace=True)
                #print(f'412-Signals to append\n{signals_to_append}\n=================')
                if self.df_signals.shape[0] != 0:
                    #print(f'414 Signals to append df shape[0] is {signals_to_append.shape[0]}')
                    #print(f'415=============signals to append index\n{signals_to_append.index[0]}\n=================')
                    for index_to_append in signals_to_append.index:
                        ts = index_to_append[0]
                        tl = index_to_append[1]
                        b1 = index_to_append[2]
                        s1 = index_to_append[3]
                        b2 = index_to_append[4]
                        s2 = index_to_append[5]
                        nt = index_to_append[6]
                        state = [0, 1, 2, 3, 4, 5]
                        try:
                            # self.df_signals.loc[index_to_append]
                            #check_append = self.df_signals.loc[(slice(ts, ts), tl, b1, s1, b2, s2, nt, state), :]
                            # df.xs retrieves a dataframe when using parts of a multiIndex or
                            # a series when using all of the mI. KeyErrror raised if the key does not exist
                            # https://pandas.pydata.org/pandas-docs/version/0.22/generated/pandas.DataFrame.xs.html
                            check_ts = self.df_signals.xs((ts, tl))
                            print(f'477-Check_ts{check_ts.index}:\n{check_ts}')
                        except KeyError:  # The index does not exist in the DF. Append it.
                            #print('429-The signal to append does not exists. Append it')
                            #print(f'self._df_signals: {self.df_signals}')
                            self.df_signals = self.df_signals.append(signals_to_append, ignore_index=False)
                            self.df_signals.sort_index(inplace=True)
                        else:
                            #print(f'430-Checking for dups with {ts}, {tl}, {b1}, {s1}, {b2}, {s2}, {nt}, {state}')
                            #print(f'431-=============\nThe signal to append already exists. Do not append it\{signals_to_append}\n============')
                            #print(f'433-The repeated line is:\n{ts, tl}')
                            signals_to_append = signals_to_append[:0]  # Drop the repeated signals so they are not returned
                            #print(f'434-The size of the empty df is {check_append.shape[0]}')
                            # An empty DF is returned sometimes, probably because a range of values is used for state
                            #if check_append.shape[0] == 0:
                                #self.df_signals = self.df_signals.append(signals_to_append, ignore_index=False)
                            pass  # Nothing needs to be done. The Signal is already in the DF
                else:
                    self.df_signals = signals_to_append
                self.df_signals.sort_index(inplace=True)

            # Code to check for signals that need state change to override goes here?????
            # Or maybe needs to go in transaction execution, so it updates right away?

            # Obtain values to return
            wait_signals = None
            #print(f'462-Num_signals: {num_signals}')
            if num_signals != 0:
                wait_signals = signals_to_append
                num_signals = signals_to_append.shape[0]

            if start_idx is None:
                if self.df_signals.shape[0] > 0:
                    start_signals = self.df_signals.iloc[0]
                    end_signals = self.df_signals.iloc[-1]
                    try:  # this has to be done for the entire signal df and select the last for the working res
                        wait_signals = self.df_signals.loc[
                                       (slice(start_signals.name[0], end_signals.name[0]), resolution,
                                        (0, 1), (0, 1), (0, 1), (0, 1), (0, 1), (0)), :]  # df containing the wait signals
                    except KeyError: # There are no wait signals
                        pass
                    else:
                        if wait_signals is not None:
                            num_signals = wait_signals.shape[0]  # The number of wait signals included in the wait_signals df
            else:
                if signals_to_append is not None:
                    if signals_to_append.shape[0]>0:
                        wait_signals = signals_to_append
                        num_signals = wait_signals.shape[0]
                    else:
                        num_signals = 0
                        end_signals = self.df_signals.iloc[-1]
                        try:  # this has to be done for the entire signal df and select the last for the working res
                            wait_signals = self.df_signals.loc[
                                           (slice(start_idx[0], end_signals.name[0]), resolution,
                                            (0, 1), (0, 1), (0, 1), (0, 1), (0, 1), (0)), :]  # df containing the wait signals
                        except KeyError:  # There are no wait signals
                            pass
                        else:
                            if wait_signals is not None:
                                num_signals = wait_signals.shape[0]  # The number of wait signals included in the wait_signals df


            if num_signals > 0:
                #print(f'489============Wait Signals DF with idx {idx} @res: {resolution}\n', wait_signals, '\n======================\n')
                print(f'490-Use_smaller_res: {self.use_smaller_resolution}')
            #print(f'455-(detect) wait signals:\n{wait_signals}')

            #print(f'\n=======Checking for Signal==========\n start: {start_df}, end: {end_df}, resolution: {resolution}\n==================')
            #print(f'=============signals to append\n{signals_to_append}\n=================')
            #print(f'495===============DF Signals at idx: {idx}\n{self.df_signals}\n====================')
            #print(f'496-Num signals: {num_signals}, wait signals: {wait_signals}')
            return num_signals, wait_signals  # num_signals helps avoid df slicing in subsequent steps


        def check_signal_validity(signal_index, resolution):
            print(f'501- check_signal_validity({resolution})')
            print(f'502- Validating Signal_index: {signal_index} at (Time: {idx[0], idx[1]})')
            print(f'503-use_smaller_res: {self.use_smaller_resolution}')
            signal = self.df_signals.loc[signal_index]

            if isinstance(signal, pd.core.frame.DataFrame):
                signal = signal.iloc[-1]

            #signal_index = None
            #print(f'457 signal: {signal}')
            #print(f'458 signal type: {type(signal)}')
            #print(f'459 signal_index: {signal_index}')
            #print(f'460 signal_index type: {type(signal_index)}')
            #print(f'458 signal[-1]: {signal[-1]}')
            signal_time = signal_index[0]
            signal_type = signal[0][-1]
            signal = signal[0][:-1]
            signal_state = 0
            start_signals = signal_time
            execution_time = None
            execution_resolution = None
            #print(f'477-validation time: {idx[0]}, {idx[1]}')

            #print(f'469-signal: {self.df_signals.loc[signal_index][0]}')
            #print(f'470-signal time: {self.df_signals.loc[signal_index].index[0]}')


            #if self.df_signals.shape[0] > 0:  # there are signals
                #start_signals = self.df_signals.iloc[0].name[0]
                #end_signals = self.df_signals.iloc[-1].name[0]
                #try:  # is there a 'wait' signal? (First IF)
                    #check_wait = self.df_signals.loc[
                                 #(slice(start_signals, end_signals), resolution, (0, 1), (0, 1), (0, 1), (0, 1), (0, 1), 0), :]  # Wait signal
            #except KeyError:
                    # There is no wait signal at the current resolution
                    #pass
            #else:
                    #signal = check_wait.iloc[-1]
                    #signal_index = check_wait.iloc[-1].name
                    #signal_time = signal_index[0]
                    #signal_type = check_wait.iloc[-1][0][-1:]  # signal 1 or 2 for buy or sell
            #else:  # There are no signals. Function should not run
                #pass

            # for buy1, sell1
            def signal_1():
                signal_state_1 = 0
                change_counter = 0
                changes_list = []
                end_time = self.df_data_all.iloc[-1].name[0]  # item[0] of the series' name (index) is tiemstamp
                #print(f'(signal1)Ready to check transaction with signal_time {signal_time}, end: {end_time}, res: {resolution}')
                check_transaction_df = self.df_data_all.loc[(slice(signal_time, end_time), resolution, (0, 1), (0, 1), (0, 1), (0, 1), (0, 1)), :]
                print(f'553-validity check. start: {signal_time}, end: {end_time}, res: {resolution}')
                index_list = check_transaction_df.index
                last_row = index_list[-1]
                execution_time = last_row[0]
                execution_resolution = last_row[1]
                # signal_status = None
                # Has the color of the candles (direction of close) changed only once?
                # (1/2) Assign color to the candles
                for idx in index_list:
                    if self.df_data_all.loc[idx, 'c'] > self.df_data_all.loc[idx, 'o']:
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
                    if self.df_data_all.loc[last_row, 'upper1'] > self.df_data_all.loc[last_row, 'c'] > self.df_data_all.loc[last_row, 'lower1']:
                        # Diagram outcome 3
                        signal_state_1 = 1 # 'valid')
                    else:  # This line is not needed???????
                        # Diagram outcome 2
                        pass  # nothing changes since the signals are initiated in a 'wait' state
                else:
                    # Diagram outcome 1
                    signal_state_1 = 3  # 'invalid')
                # end signal_1
                #print(f'\n================signal_1() signal: \n{signal}, \nstate: {signal_state_1} \nat {execution_time}, {execution_resolution}\n==================\n')

                #print(f'585-last row: {last_row}, working res {resolution}\n changes_list: {changes_list}, change_counter: {change_counter}')
                return signal_state_1, execution_time, execution_resolution

            # for buy2, sell2
            def signal_2():
                signal_state_2 = 0
                change_counter = 0
                changes_list = []
                end_time = self.df_data_all.iloc[-1].name[0]  # item[0] of the series' name (index) is timestamp
                check_transaction_df = self.df_data_all.loc[(slice(signal_time, end_time), resolution, (0, 1), (0, 1), (0, 1), (0, 1), (0, 1)), :]
                index_list = check_transaction_df.index
                last_row = index_list[-1]  # signal
                signal_status = None
                execution_time = last_row[0]
                execution_resolution = last_row[1]
                # Has the color of the candles (direction of close) changed only once?
                # (1/2) Assign color to the candles
                for idx in index_list:
                    if self.df_data_all.loc[idx, 'c'] > self.df_data_all.loc[idx, 'o']:
                        changes_list.append('green')
                    else:
                        changes_list.append('red')

                # (2/2) Count the times the color of the candles changes
                for i in range(1, len(changes_list)):
                    if changes_list[i - 1] != changes_list[i]:
                        change_counter += 1
                # Diagram decision 1: Did te direction of the closing price change more than once?
                if change_counter == 0: # Do nothing. Signal already in wait state
                    pass
                elif change_counter == 1:
                    signal_state_2 = 1  # 'valid'
                else:
                    signal_state_2 = 2  # 'invalid'
                # end signal_2
                #print(f'Signal2(). signal_time = {signal_time}, state = {signal_state_2}, resolution = {resolution}')
                return signal_state_2, execution_time, execution_resolution

            # decide to use signal_1 or signal_2
            # buy2 and sell2 are a sub-type of buy1 and sell1

            if signal_index is not None:  # Do not run the function if there is no signal
                if signal_type == '1':
                    signal_state, execution_time, execution_resolution = signal_1()
                elif signal_type == '2':
                    signal_state, execution_time, execution_resolution = signal_2()
                    #if signal 2 is invalid, it should continue status as signal 1
                    if signal_state == 3:  # 0-wait, 1-valid, 2-executed, 3-invalid, 4-override, 5-null
                        signal_state, execution_time, execution_resolution = signal_1()
                        signal = signal[:len(signal)-1]+'1'
            else:  # No signals. Do nothing
                pass
            #if signal_state == 0:
                #print(f'608-Validation result: {signal_index[0]},{signal_index[1]}\nres: {resolution}, state: {signal_state}\n=============')
            return signal, signal_state  # signal will be a series

        # 2/7/22: Calls detect_signal(), which returns (signal, index)
        # The use primary and use secondary functions analyze signals and data and send the execution signal
        # Replaces use_primary() abd use_secondary()
        def perform_analysis(idx):
            # will work with primary or secondary resolution
            self.df_data_all.sort_index(inplace=True)
            index_list = self.df_data_all.index
            resolution = self.PRIMARY_RESOLUTION if not self.use_smaller_resolution else self.SECONDARY_RESOLUTION
            transaction_execution = False
            execution_time = None # idx[0] #
            execution_resolution = None # idx[1] #
            signals_complete = None
            secondary_signals_complete = None
            valid_signal = None

            #print(f'================Analyzing({idx} @{resolution} resolution')
            # indicates that an analysis was conducted, even if no valid signals are returned
            # when analysis_conduct == True, selected_valid_signal is None, signals_complete == True
            # then stoploss_check() can run

            # NOTE 02/18/22 0718: The notes below are for the previous version of this function
            # that did not use self.df_signals. Unsure if the notes are valid????
            # The a/ove vars will be returned unchanged if the idx does not have primary or secondary resolution
            # this c n be used to ensure that analysis runs before stoploss check in a case when
            # @ 1m data is received before @5m or @15m. For example:
            # 12:15. data @1m is received before @15m and triggers a stoploss before analysis triggers the correct transaction
            # Running analysis before stoploss prevents this

            signal = None
            signal_status = None
            signal_index = None
            sub_signal = None
            sub_signal_status = None
            sub_signal_index = None
            num_signals, wait_signals = None, None
            secondary_signal_index = None

            # ======================Section===============================================================
            # Finished 02/18/22 0719
            # This section coordinates check_signals_complete(), detect_signal() and check_signal_validity()
            if not self.use_smaller_resolution:
                #print('Using main resolution:', idx)
                # Need to perform analysis also of secondary to search for type 2
                minutes = idx[0].minute + (idx[0].hour * 60)

                # If this is not a closing candle (at any res) from @main res. Examples follow:
                # Should not run: 5:14@1, 5:10@5, 5:00@15
                # Should not run: 5:29@1, 5:25@5, 5:15@15
                if (minutes + idx[1]) % self.PRIMARY_RESOLUTION != 0:
                    # completeness check is needed to ensure to transaction collision with stoploss
                    #print(f'checking @secondary for type 2 @idx {idx}:', self.SECONDARY_RESOLUTION)
                    #print(f'========Data ALL===========\n{self.df_data_all}\n==============')
                    secondary_signals_complete = check_signals_complete(idx, self.SECONDARY_RESOLUTION)
                    #print(f'completeness is {secondary_signals_complete} for idx {idx}')
                    # check for type 2 signals in secondary res
                    if secondary_signals_complete:
                        num_secondary_signals, secondary_wait_signals = detect_signal(self.SECONDARY_RESOLUTION)
                        #print(f'700-secondary signals complete. Number of wait signals @idx {idx}: {num_secondary_signals}\n{secondary_wait_signals}')
                        if num_secondary_signals > 0:  # There are signals
                            secondary_signal = secondary_wait_signals.iloc[-1]
                            secondary_signal_index = secondary_signal.name
                            secondary_signal_type = secondary_signal[0][-1:]
                            if secondary_signal_type == '2':
                                #print(f'706-==================\n{idx[0]} checking validity with {secondary_signal_index} at res {self.SECONDARY_RESOLUTION}')
                                sub_signal, sub_signal_state = check_signal_validity(secondary_signal_index, self.RESOLUTIONS[self.RESOLUTIONS.index(self.PRIMARY_RESOLUTION) + 1])
                                # sub_signal_status = sub_signal[0] INCORRECT
                                #print(f'Sub-signal type 2 state(idx: {idx}):\n', sub_signal_state)
                                #print(f'========Data ALL===========\n{self.df_data_all}\n==============')
                                #print(f'========signals===========\n{self.df_signals}\n==============')
                                # if the signal is valid, there is no need to change res, since it will execute
                                if sub_signal_state == 0:
                                    #print('changing to smaller res')
                                    self.use_smaller_resolution = True
                                elif sub_signal_status == 1:  # 'valid': # Need to do or return something else?
                                    # execute transaction
                                    # Is this really needed? Or can the execution routine scan for valid siganls????
                                    # Should execution run every minute????
                                    self.change_signal_state(secondary_signal_index, 1)
                                    valid_signal = secondary_signal_index
                                    transaction_execution = True
                                elif sub_signal_status == 3:  # 'invalid':
                                    self.change_signal_state(secondary_signal_index.index, 3)
                #print(f"725-signals complete check with index {idx[0]}, {idx[1]} and resolution {resolution}")
                signals_complete = check_signals_complete(idx, resolution)
                if signals_complete: # data has all the sub-signals
                    #signal, signal_index = detect_signal(self.PRIMARY_RESOLUTION)
                    num_signals, wait_signals = detect_signal(resolution) # Only wait signals???
                    print(f'\n\n==============\n730-signals complete {idx} @main. Number of wait signals:', num_signals)
                    print(f'731-wait signals:\n{wait_signals}')
                    if num_signals > 0 and not self.use_smaller_resolution:  # There are wait signals @ main
                        signal_index = wait_signals.iloc[-1].name
                        signal_time = signal_index[0]
                        print(f'735=======================\n{idx} detected signal @ {resolution}\nsignal index:{signal_index}\n==============')
                        # signal_index
                        num_sub_signals, wait_sub_signals = 0, None
                        # only check for sub-signals when the current index is a signal
                        if (idx[0] == signal_index[0]) and (idx[1] == signal_index[1]):
                            print(f'739-{idx[0]} calling detect_signal({self.SECONDARY_RESOLUTION}, Start: {signal_index})')
                            num_sub_signals, wait_sub_signals = detect_signal(self.SECONDARY_RESOLUTION, signal_index)
                            print(f'741-wait sub_signals:\n {wait_sub_signals}')
                            #print(f'742-df_signals:\n{self.df_signals}')
                        #print(f'detected signal @ main with {num_sub_signals} sub-signals\nsub_signals:\n{wait_sub_signals}')
                        #print(f'checking sub-signals with sart: {start}, res:', self.RESOLUTIONS[self.RESOLUTIONS.index(self.PRIMARY_RESOLUTION) + 1])
                        if num_sub_signals > 0:  # there is a sub-signal (needs wait or valid ONLY)
                            #print(f'==========SUBS==================')
                            sub_signal_index = wait_sub_signals.iloc[-1].name
                            print(f'746-=======================\n{idx[0]} checking validity with {sub_signal_index} with res {resolution}')
                            #print(f'708-Workign Resolution is {resolution}')
                            sub_signal, sub_signal_status = check_signal_validity(sub_signal_index, self.SECONDARY_RESOLUTION)
                            print(f'750-sub_signal {sub_signal_index}@{resolution}: {sub_signal}\n status:{sub_signal_status}\n===========')
                            if sub_signal_status == 1:  # 'valid': # Need to do or return something else?
                                # execute transaction
                                #Is this really needed? Or can the execution routine scan for valid siganls????
                                # Should execution run every minute????
                                self.change_signal_state(sub_signal_index, 1)
                                valid_signal = sub_signal_index
                                transaction_execution = True
                            elif sub_signal_status == 0:  # 'wait':
                                # Wait @ secondary resolution
                                self.use_smaller_resolution = True
                            elif sub_signal_status == 3:  # 'invalid':
                                #print(f'706-sub_signal is {sub_signal_index}')
                                self.change_signal_state(sub_signal_index, 3)
                                self.use_smaller_resolution = False
                                #print(f'733-====================\n{idx[0]} checking validity with\n {signal_index} at res {self.PRIMARY_RESOLUTION}')
                                signal, signal_status = check_signal_validity(signal_index, self.PRIMARY_RESOLUTION)
                                #print(f'735-checking validity. Signal: {signal}, Status: {signal_status}\n=================')
                                #print(f'736-Signals DF \n{self.df_signals}')
                                #print(f'737-Wait Sub-Signals\n {wait_sub_signals}')
                                if signal_status == 1:  # 'valid': # Do not need to do anything for other signals
                                    # execute transaction
                                    self.change_signal_state(signal_index, 1)
                                    valid_signal = signal_index
                                    #print(f'742=================\nTransaction execution with signal {valid_signal}\n====================')
                                    #print(f'743================== Signals DF\n{self.df_signals}\n====================')
                                    transaction_execution = True
                                elif signal_status == 3:
                                    self.change_signal_state(signal_index, 3)
                        else: # no sub-signal, but signal; check
                            #print(f'748-=====================\n{idx[0]} checking validity with {signal_index} at res {self.PRIMARY_RESOLUTION}')
                            signal, signal_status = check_signal_validity(signal_index, self.PRIMARY_RESOLUTION)
                            #print(f'750-Main signal status is {signal_status}')
                            #print(f'checking validity. Signal: {signal}, Index: {signal_index}')
                            if signal_status == 1:  # 'valid': # Do not need to do anything for other signals
                                # execute transaction
                                self.change_signal_state(signal_index, 1)
                                valid_signal = signal_index
                                transaction_execution = True
                            elif signal_status == 3:
                                self.change_signal_state(signal_index, 3)
                                print(f'801-Changing signal status {signal_index} to state 3')
            elif self.use_smaller_resolution:
                if (idx[0].minute) % self.SECONDARY_RESOLUTION == 0:
                    print("803-analysis with secondary resolution", idx[0], idx[1])
                    signals_complete = check_signals_complete(idx, self.SECONDARY_RESOLUTION)
                    if signals_complete:  # data has all the sub-signals
                        print(f'806-signals complete at idx: {idx}')
                        # check validity of last signal before detecting new signal
                        start = self.df_signals.iloc[0].name[0]
                        end = self.df_signals.iloc[-1].name[0]
                        df_to_validate = None
                        idx_to_validate = None
                        try:
                            df_to_validate = self.df_signals.loc[(slice(start, end), self.SECONDARY_RESOLUTION, (0, 1),
                                             (0, 1), (0, 1), (0, 1), (0, 1), 0), :]
                            print(f'812-df to validate. Start: {start}, end: {end}, res: {self.SECONDARY_RESOLUTION}\n{df_to_validate}')
                        except KeyError: #
                            print('No signal at this res (803 & 806), do nothing\n')
                            print('813-Changing use_smaller to False')
                            self.use_smaller_resolution = False
                            pass
                        else:
                            print(f"816- State of. last row: \n{df_to_validate.iloc[-1]}\nstate: {df_to_validate.iloc[-1].name[7]}")
                            if df_to_validate.iloc[-1].name[7] == 0:  # is this a wait signal
                                idx_to_validate = df_to_validate.iloc[-1].name
                                old_signal, old_state = check_signal_validity(idx_to_validate, self.SECONDARY_RESOLUTION)
                                print(f'825-Result of validation: {old_signal} has state: {old_state}')
                                if old_state == 1:
                                    valid_signal = idx_to_validate
                                    # print(f'867-idx_to_validate: {idx_to_validate}')
                                    # self.change_signal_state(idx_to_validate, 1)
                                    transaction_execution = True
                                    pass
                                elif old_state != 0:
                                    print('827-Changing use_smaller to False')
                                    self.change_signal_state(idx_to_validate, old_state)
                                    self.use_smaller_resolution = False
                        num_signals, wait_signals = detect_signal(self.SECONDARY_RESOLUTION)
                        #print(f'755- Wait signals: {wait_signals}')
                        if wait_signals is not None and self.use_smaller_resolution:
                            signal_index = wait_signals.iloc[-1].name
                            #print(f'number of wait signals: {num_signals}, wait signals: {wait_signals}')
                            if num_signals > 0:
                                #print('729-checking validity')
                                #print(f'771-signal_index: {signal_index}, type: {type(signal_index)}')
                                signal, signal_status = check_signal_validity(signal_index, self.SECONDARY_RESOLUTION)
                                #print(f'773-Main signal status is {signal_status}')
                                if signal_status == 1:  # 'valid':
                                    #print(f'Signal {signal} is valid')
                                    # execute transaction
                                    valid_signal = signal_index
                                    self.change_signal_state(signal_index, 1)
                                    transaction_execution = True
                                elif signal_status == 0:  # 'wait':
                                    pass
                                elif signal_status == 3:  # 'invalid':
                                    self.use_smaller_resolution = False
                                    self.change_signal_state(signal_index, 3)
                                elif signal_status == 2:
                                    self.use_smaller_resolution = False
                        elif wait_signals is None and self.use_smaller_resolution:
                            # Need to check if there is a wait signal and check its validity
                            try:
                                df_to_validate = self.df_signals.loc[(slice(start, end), self.SECONDARY_RESOLUTION,
                                                                      (0, 1), (0, 1), (0, 1), (0, 1), (0, 1), 0), :]
                                print(
                                    f'912-df to validate. Start: {start}, end: {end}, res: {self.SECONDARY_RESOLUTION}\n{df_to_validate}')
                            except KeyError:  # No wait signal. Revert to main
                                print('No signal at this res (803 & 806), do nothing\n')
                                print('913-Changing use_smaller to False')
                                self.use_smaller_resolution = False
                            else:  # There is a signal. Validate
                                idx_to_validate = df_to_validate.iloc[-1].name
                                old_signal, old_state = check_signal_validity(idx_to_validate,
                                                                              self.SECONDARY_RESOLUTION)
                                print(f'917-Result of validation: {old_signal} has state: {old_state}')
                                if old_state == 1:
                                    valid_signal = idx_to_validate
                                    # print(f'921-idx_to_validate: {idx_to_validate}')
                                    # self.change_signal_state(idx_to_validate, 1)
                                    transaction_execution = True
                                    pass
                                elif old_state != 0:
                                    print('926-Changing use_smaller to False')
                                    self.change_signal_state(idx_to_validate, old_state)
                                    self.use_smaller_resolution = False

            #print(f"analysis with {resolution} resolution", idx[0], idx[1])
            # ======================Section===============================================================
            # Finished 02/18/22 0719
            # This section decides what signal will be passed for execution
            selected_valid_signal = None  # will be a index
            if self.df_signals.shape[0] > 0:  # is there a valid signal?
                start = self.df_signals.iloc[0].name[0]
                end = self.df_signals.iloc[-1].name[0]
                #print(self.df_signals)
                try:
                    check_signals = self.df_signals.loc[
                                    (slice(start, end), (self.PRIMARY_RESOLUTION, self.SECONDARY_RESOLUTION),
                                     (0, 1), (0, 1), (0, 1), (0, 1), (0, 1), 1), :] #  Valid signals
                except KeyError:  # No valid signal to execute
                    pass
                    # raise KeyError
                else:
                    if check_signals.shape[0] == 1:  # change to check_signals.shape[0] == 1 (ignore stater data)
                        # There is only 1 signal to process
                        selected_valid_signal = check_signals.iloc[-1].name
                        #print(f'796===============\n The selected_valid_signal to execute is:\n{selected_valid_signal}\n=================')
                    else:
                        idxs = check_signals.index
                        idx_selected = idxs[0]
                        if len(idxs) > 1:
                            for i in range(1, len(idxs)):
                                if idxs[i][0] > idxs[i-1][0]:
                                    idx_selected = idxs[i]  # idx_selected will have an index with the newest timestamp
                            for j in range(len(idxs)):
                                if idxs[j][0] == idx_selected[0]:  # second index with newer timestamp?
                                    if idxs[j][1] < idx_selected[1]:  # does it have smaller resolution?
                                        idx_selected = idxs[j]
                            selected_valid_signal = idx_selected  # check_signals.loc[idx_selected]

            transaction_execution = True if selected_valid_signal is not None else False

            if selected_valid_signal is not None:
                #print(f'Ex, Selected Valid Signal: {selected_valid_signal.name[0]}, {selected_valid_signal.name[1]}, {selected_valid_signal[0]} @{selected_valid_signal[1]}m')
                pass

            if valid_signal is not None:
                #print(f'ExSelected Signal: {valid_signal.name[0]}, {valid_signal.name[1]}, {valid_signal[0]} @{valid_signal[1]}m')
                pass
            #print(f'809===============\n The selected_valid_signal to execute is:\n{selected_valid_signal}\n=================')
            return selected_valid_signal, signals_complete, secondary_signals_complete


        # Access to inner functions
        #print('index = ', idx)
        timestamp = idx[0]
        resolution = idx[1]
        mins = timestamp.minute + (timestamp.hour * 60)

        # Needs to run at every resolution in case signals arrive out of order

        # NOTE 02/18/22 0729: (selected_valid_signal == None) means no transaction execution, so stoploss_check() needs to be done
        # selected_valid_signal is a series
        # signals_complete is about the working resolution
        # secondary_signals_complete returns a value when self.use_smaller_resolution == False, else None
        # @1m stoploss should only run in certain minutes to avoid possible transaction collisions
        # Identifies the timestamps that are used in the completeness check of secondary and primary resolutions
        # Eg: mins 4 and 9 are needed in the completeness check of @5m, and min 14 for the @5 and @15

        second_res = self.RESOLUTIONS[self.RESOLUTIONS.index(self.PRIMARY_RESOLUTION)+1]
        # ==============================section=====================================
        # Future version note: This decision is not needed if analysis @1m returns (selected_valid_signal = None)?????
        # Decides whether to process a transaction or not before a stoploss_check()
        if resolution == self.RESOLUTIONS[-1] and mins % second_res != second_res-1:
            # run stoploss_check() will decide if it needs to run
            # sending idx with no signal will run stoploss_check()
            self.check_transaction_execution(idx)
            pass
        else:
            selected_valid_signal, signals_complete, secondary_signals_complete = perform_analysis(idx)
            secondary_signals_complete = signals_complete if self.use_smaller_resolution else secondary_signals_complete
            #print(f'================result of perform_analysis: ============')
            #print(f'valid_signal {selected_valid_signal}, \n'
                  #f'signals_complete: {signals_complete}, \n'
                  #f'sub-signals Complete: {secondary_signals_complete}\n======================================')
            if not self.use_smaller_resolution:
                # print('===============Main Res in use===================')
                if signals_complete:
                    # If there is no signal, selected_valid signal = None; will trigger stoploss_check()
                    #print(f'850===============\n The selected_valid_signal to execute is:\n{selected_valid_signal}\n=================')
                    self.check_transaction_execution(idx, selected_valid_signal)
            else:
                if secondary_signals_complete:
                    # If there is no signal, selected_valid signal = None; will trigger stoploss_check()
                    #print(f'===========Execution==========================')
                    #print(f'Valid Signal {selected_valid_signal}, index: {idx}')
                    #print('================================================')
                    self.check_transaction_execution(idx, selected_valid_signal)
        return

    # Incomplete. Work in progress 2/5/22
    # Called by analyze_signal_and_data
    # Receives valid signals. Decides if a transaction should happen, and what type of transaction
        # If self.last_transaction = 'buy' and new valid signal 'buy', then no transaction
    # Note 02/19/22: Should receive idx, selected_valid_signal??????
    # def check_transaction_execution(self, execution_time, execution_resolution, transaction_execution):
    def check_transaction_execution(self, idx, selected_valid_signal=None):
        # idx: The index of the signal that triggered the transaction (can be any res)
        # selected_valid_signal: The series with the valid signal passed from self.analyze_signal_and_data()
        next_transaction = None
        proposed_transaction = None
        timestamp = idx[0]
        resolution = idx[1]
        transaction_execution = False if selected_valid_signal is None else True
        used_stoploss = None  # Keep track of what stoploss was used in the execution

        # Sets next_transaction to the value of the next transaction that needs to happen
        if selected_valid_signal is not None:
            if self.last_transaction is not None:
                if self.last_transaction == 'sell':
                    next_transaction = 'buy'
                else:
                    next_transaction = 'sell'
            else:
                if self.paper_balance_currency == 'ETH':
                    next_transaction = 'sell'
                else:
                    next_transaction = 'buy'
            # Sets the value of the proposed transaction
            #print(f'885 Selected valid signal is {selected_valid_signal} and is of type {type(selected_valid_signal)}')
            #print(f'886 Selected valid signal is {self.df_signals.loc[selected_valid_signal]}')
            #print(f'887 Proposed transaction is {self.df_signals.loc[selected_valid_signal][0][:-1]}')
            proposed_transaction_info = self.df_signals.loc[selected_valid_signal]
            proposed_transaction = None
            if isinstance(proposed_transaction_info, pd.core.frame.DataFrame):
                proposed_transaction = proposed_transaction_info.iloc[-1][0][:-1]
            else:
                proposed_transaction = self.df_signals.loc[selected_valid_signal][0][:-1]


        # executes a regular trade or a correction transaction
        # Finished 02/19/22 1628
        def execute_transaction(transaction_execution, stoploss):
            #print('954========execute transaction is called====================')
            #print (f'955-Selected valid signal:{selected_valid_signal}')
            # (1/2) Data recording and changes to instance variables
            # returns the values of SD and close to use in either stoploss or transaction
            close_delta = 0
            mins = timestamp.minute + (timestamp.hour * 60)

            if self.df_transactions.shape[0] > 0:
                last_transaction = self.df_transactions.iloc[-1]  # Series

            if resolution == self.RESOLUTIONS[-1]:
                close_delta = mins % self.SECONDARY_RESOLUTION
                if not isinstance(close_delta, int):
                    close_delta = -close_delta.item()
                else:
                    close_delta = -close_delta
            elif resolution == self.PRIMARY_RESOLUTION:
                close_delta = self.PRIMARY_RESOLUTION - self.SECONDARY_RESOLUTION

            close_time = timestamp + timedelta(minutes=close_delta)
            #print(f'close_time = {close_time}')
            execution_candle = self.df_data_all.loc[
                                     (slice(close_time), self.SECONDARY_RESOLUTION, (0, 1), (0, 1), (0, 1), (0, 1), (0, 1)), :]
            #execution_candle = self.df_data_all.loc[execution_candle_index]
            #print('execution_candle:\n', execution_candle)

            #print('XXXXXXXXXXXX\n', execution_candle.iloc[-1])
            ex_idx = execution_candle.iloc[-1].name
            close = self.df_data_all.loc[idx]['c']
            close_idx = execution_candle.loc[ex_idx, 'c']
            #print('index:', ex_idx)
            sd = execution_candle.loc[ex_idx, 'SD']

            #print(f'987-transaction time: {timestamp} @{resolution} with close = {close}')
            #print(f'988-@5 equivalent: time: {ex_idx[0]} with close = {close_idx}')
            #print(f"execution candle\n", execution_candle)


            # Record data for all transactions
            # Sets the value for a stoploss; if regular transaction, value will be changed in "if transaction_execution:"

            dict_to_append = {
                'time_ex': [idx[0]],
                'timeline_ex': [idx[1]],
                'signal_time': [],
                'timeline': [],
                'signal': [],
                'c': [close],
                'limit': [],
                'stoploss': [stoploss],
                'transaction': [proposed_transaction],
                'balance': [],
                'currency': []
            }

            # record data exclusive to regular transactions
            if transaction_execution:
                # There is a transaction to execute
                # Record signals data that led to execution

                #self.SECONDARY_RESOLUTION = None
                self.use_smaller_resolution = False
                #print(f"==========\nselected valid signal\n{selected_valid_signal}\n==========")

                dict_to_append['signal_time'] = selected_valid_signal[0]
                dict_to_append['timeline'] = selected_valid_signal[1]
                dict_to_append['signal'] = self.df_signals.loc[selected_valid_signal].iloc[0]
            else:
                if stoploss:  # Process stoploss
                    dict_to_append['signal_time'] = 'None'
                    dict_to_append['timeline'] = 0
                    dict_to_append['signal'] = used_stoploss

            # Set new stoploss
            dict_to_append['limit'] = set_stoploss_limit(close, sd)
            # (2/2) Process transaction
            # Send to finalize_transaction()
            # That will record paper trade and send to module that processes with exchange
            #print('913============\n', dict_to_append, '\n',stoploss)
            finalize_transaction(dict_to_append, stoploss)
            return


        # Finished 02/20/22 1512
        def finalize_transaction(dict_to_append, stoploss):
            print('1042-Paper Trade')
            print(f'1043-Proposed transaction: {proposed_transaction}. Next transaction: {next_transaction}')
            COMMISSION = 0.0008
            current_balance = self.paper_balance_amount
            current_currency = self.paper_balance_currency
            c = dict_to_append.get('c')[-1]
            new_balance = 0.0
            new_currency = None
            print()
            # This is the transaction execution (in paper)
            if proposed_transaction == "sell":
                new_balance = current_balance * c * (1 - COMMISSION)
                new_currency = "USDT"
            elif proposed_transaction == "buy":  # currency is USDT
                new_balance = (current_balance / c) * (1 - COMMISSION)
                new_currency = "ETH"

            print(f'1059-{proposed_transaction} ETH at {c}. Ending Balance of {new_balance} {new_currency}. Stoploss: {stoploss}')


            # transaction information to record
            self.last_transaction = proposed_transaction
            self.paper_balance_currency = new_currency
            self.paper_balance_amount = new_balance

            dict_to_append['balance'] = new_balance
            dict_to_append['currency'] = new_currency

            #print(dict_to_append)
            df_append = pd.DataFrame.from_dict(dict_to_append)
            self.df_transactions = self.df_transactions.append(df_append, ignore_index=True)

            csv_data = self.df_data_all_path + '/' + self.date_for_file + 'transactions.csv'
            if self.df_transactions.shape[0] > 1:
                df_append.to_csv(csv_data, index=True, mode='a', header=False)
            else:
                df_append.to_csv(csv_data, index=True, mode='a', header=True)

            # update self.df_signals here:
            if selected_valid_signal is not None:  # It will be None in the case of a stoploss
                signal_idx = selected_valid_signal
                self.change_signal_state(signal_idx, 2)  # executed
                print(f'1085- Signal {signal_idx} has been changed to state: 2')
                self.use_smaller_resolution = False

            print(f'1086-use smaller resolution is {self.use_smaller_resolution}\n================================')

            # check that there are no valid or waiting signals previous to the executed one
            start = self.df_signals.iloc[0].name[0]  # series.name[0] should result in timestamp
            end = self.df_signals.iloc[-1].name[0]
            #print('signals_index=============\n===============\n', self.df_signals.index[0], '\n==============')
            #print('Start is of type:', type(start))
            try:  # are there signals that need updating?
                signals_to_update = self.df_signals.loc[(slice(start, end), self.RESOLUTIONS, (0, 1), (0, 1), (0, 1),
                                                         (0, 1), (0, 1), (0, 1)), :].index # wait or valid
            except:
                pass  # No signals to update
            else:
                #print('signals_to_update\n', signals_to_update)
                for signal_to_update in signals_to_update:  # disable the above signals
                    self.change_signal_state(signal_to_update, 5)
                    #print(f'1076- Updating signal {signal_to_update} to state 5')

            #print(f'1104===============signals {self.df_signals}\n======================')


        # decides whether a  stoploss is needed or not
        # Finished 2/20/2022
        def analyze_stoploss():
            #print(f'1110-Analyze Stoploss')
            stoploss = False
            used_stoploss = None

            minutes = timestamp.minute + (timestamp.hour * 60)
            limit = self.df_transactions.iloc[-1][6]
            last_price = self.df_transactions.iloc[-1][5]
            transaction = self.df_transactions.iloc[-1][8]
            last_transaction_time = self.df_transactions.iloc[-1][0]
            last_transaction_res = self.df_transactions.iloc[-1][1]
            last_data_time = self.df_data_all.iloc[-1].name[0]
            last_data_res = self.df_data_all.iloc[-1].name[1]

            # print(f'Analyze stoploss. Last transaction: {transaction}, price: {last_price}, limit: {limit}')

            delta = last_data_res - 1
            if not isinstance(delta, int):
                delta = delta.item()
            end = last_data_time + timedelta(minutes=delta)



            # 1 1m candle that goes past 2x limit will trigger stoploss
            # Finished 02/20/22 0819
            # This part should analyze price changes from the close of the last transaction to the close
            # of close of the current @1m or @5m candle in self.df_data_all
            # Both 1m and 5m checks need to be corrected for this 02/20/22 0831
            def check_1_min():
                #print('1137=========check_1_min=============')
                stoploss = False
                start = last_transaction_time
                #print(f'1140========start: {start}, end: {end}')

                if last_transaction_res == 5:
                    start = last_transaction_time  # + timedelta(minutes=4)
                elif last_transaction_res == 15:
                    start = last_transaction_time  # + timedelta(minutes=14)

                #print(f'========all_data=========\n{self.df_data_all.iloc[-20:]}')
                #print('=============all_data shape===============', self.df_data_all.shape[0])
                try:
                    # slice to find data with the timestamp, resolution
                    data_df_slice = self.df_data_all.loc[
                                     (slice(start, end), (1), (0, 1), (0, 1), (0, 1), (0, 1), (0, 1)), :]
                except IndexError:
                    raise IndexError  # No data of the timestamp, there should be data!!!!
                else:
                    # There is data, retrieve the price close
                    #print('1159-slice index: ',data_df_slice)
                    slice_key = data_df_slice.index[-1]
                    #print(f'checking stoploss @1m with df_data_all slice:\n{slice_key}\n=================')
                    close = data_df_slice.at[slice_key, 'c']
                    if transaction == "buy":
                        if close < last_price - (limit * 2000000):  # limi*2. Set a high number to disable
                            stoploss = True
                            #print(f'1166==========stoploss==========\n{close} < limit*2({limit} @1m\n=============)')
                            #print(f'1167-Analyze stoploss. Last transaction: {transaction}, price: {last_price}, limit: {limit}')
                            # execute stoploss
                    elif transaction == "sell":
                        if close > last_price + (limit * 2000000):  # limi*2. Set a high number to disable
                            stoploss = True
                            #print(f'1172==========stoploss==========\n{close} > limit*2({limit}) @1m\n=====idx: {idx}========)')
                            #print(f'1173-Analyze stoploss. Last transaction: {transaction}, price: {last_price}, limit: {limit}')
                            # execute stoploss
                return (stoploss, '1_min')

            # 3 5m candles closing under or over limit will trigger stoploss
            def check_5_min():
                print('1179================Check_5 min=====================')
                start = last_transaction_time
                extra_min = ((last_transaction_time.hour * 60) + last_transaction_time.minute) % self.SECONDARY_RESOLUTION
                stoploss = False
                if not isinstance(extra_min, int):
                    extra_min = extra_min.item()

                # If the previous transaction was a stoploss triggered @1m (5:26), then this sets the start time
                # at 5:25 so that the 5m candle can be taken into account
                if extra_min != 0:
                    start = timestamp - timedelta(minutes=extra_min)

                #print(f'========start: {start}, end: {end}')
                #print(f'===========last data\n{self.df_data_all.iloc[-1].name[0]} @{self.df_data_all.iloc[-1].name[1]} \n====================')

                try:
                    check_stoploss_df = self.df_data_all.loc[
                                        (slice(start, end), self.SECONDARY_RESOLUTION, (0, 1), (0, 1), (0, 1), (0, 1), (0, 1)), :]
                except KeyError:
                    # no 5 min data yet, since it runs on every candle
                    pass
                else:
                    index_list = check_stoploss_df.index
                    candle_colors = []
                    candle_past_limit = []
                    result_list = []
                    result_summary = 0

                    # Assign color to the candles
                    for idx in index_list:
                        if self.df_data_all.loc[idx, 'c'] > self.df_data_all.loc[idx, 'o']:
                            candle_colors.append('green')
                        else:
                            candle_colors.append('red')
                        # Assign positive value c > limit; negative otherwise
                        if self.df_data_all.loc[idx, 'c'] > (last_price + limit):
                            candle_past_limit.append(1)
                        elif self.df_data_all.loc[idx, 'c'] < (last_price - limit):
                            candle_past_limit.append(-1)
                        else:
                            candle_past_limit.append(0)

                    # combine red and neg; green and pos; record results in list
                    if transaction == "buy":
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

                    # counts how many pairs of consecutive candles have crossed the limit
                    for j in range(1, len(result_list)):
                        if result_list[j-1] == 1 and result_list[j] == 1:
                            result_summary += 1

                    #if result_list.count(1) > 2:
                    if result_summary > 0:
                        print(f'1088==========stoploss==========\ limit has been reached ({limit} @5m\n=============)')
                        print(f'Analyze stoploss. Last transaction: {transaction}, price: {last_price}, limit: {limit}')
                        stoploss = True
                return (stoploss, '5_min')
                        # execute stoploss

            # 1  min check needs to run every minute
            stoploss, used_stoploss = check_1_min()

            # if the above does not trigger, check the 5m
            if minutes % self.SECONDARY_RESOLUTION == 0 and not stoploss:
                temp_stoploss, used_stoploss = check_5_min()
                if temp_stoploss:
                    stoploss = temp_stoploss
                    used_stoploss = str(self.SECONDARY_RESOLUTION)+'_secondary'

            #print(f'1254-stoploss is {stoploss}')
            return (stoploss, used_stoploss)

        # transaction_close is the close of the candle that executed the transaction
        # Finished 2/4/2021
        def set_stoploss_limit(close, sd):
            pct_limit = 0.005  # This number is not in percentage points (0.0025 = 0.25%)
            sd_limit = 0.5  # As a percent of SD. Same as above (0.25 = 25%)

            limit_pct = close * pct_limit
            limit_sd = sd * sd_limit

            limit = limit_pct if limit_pct > limit_sd else limit_sd
            return limit


        # executes a transaction or a stoploss check
        # Finished 2/4/2021
        if not transaction_execution: # transaction_execution is False; no transaction, run stoploss_check()
            if self.df_transactions.shape[0] > 0: # if there is a previous transaction (do not run if algo just started)
                stoploss, used_stoploss = analyze_stoploss() # analyze if a stoploss needs to be executed
                # print(f'perform execute_transaction with EX: {transaction_execution} and Stoploss: {stoploss}')
                # Maybe execute this line only if stoploss is True???
                if stoploss:
                    if self.last_transaction == 'buy':
                        proposed_transaction = 'sell'
                    else:
                        proposed_transaction = 'buy'
                    execute_transaction(transaction_execution, stoploss) # perform the stoploss if needed
            else: # if there is no previous transaction
                # print("No previous transaction. No stoploss needed")
                pass
        else:  # transaction_execution == True
            # Just bought. Now wait for sell signal (next_transaction) to trigger transaction should be sell. Is it?
            #print('======\n', 'transaction_execution is ', transaction_execution, '\n================')
            #print('proposed_transaction', proposed_transaction, '\nnext_transaction', next_transaction)
            if proposed_transaction == next_transaction:
                # proceed with transaction execution
                execute_transaction(transaction_execution, stoploss=False)
            else:
                # selected valid signal needs to be nullified
                self.change_signal_state(selected_valid_signal, 5)
                #print(f'1269 Changing signal {selected_valid_signal} to state 5')
        return
