import json
import logging
import pprint
import csv
import pandas as pd
from kucoin.user.user import UserData
from kucoin.trade.trade import TradeData
import json
import websocket
import threading
from kucoin.base_request.base_request import KucoinBaseRestApi

logger = logging.getLogger()


class KuCoinSpotClient:
    def __init__(self, sandbox=False):
        self.ws = None

        self.sandbox_key = '6102dc5bbc85c200065a5cf7'
        self.sandbox_secret = '98f593a9-de00-4ae6-90f8-47d7f239773b '
        self.sandbox_passphrase = 'Dunedog99*'

        self.sb_sub1_key = '61031b0fbc85c200065a5d4b'
        self.sb_sub1_secret = '885767af-5871-41ca-ab3e-957c527cc4e6'
        self.sb_sub1_passphrase = 'Dunedog99*'
        self.sb_sub_user1 = ''

        self.main_key = '61046016ead9100006ee2c3c'
        self.main_secret = 'bca41fe7-bf77-42a2-a16c-b4f2b6a72ade'
        self.main_passphrase = 'Dunedog99*'

        self.sub1_key = '6106e528eadc7d000640f64d'
        self.sub1_secret = 'bf18297d-f270-4bce-a9ba-617d9f5131c2'
        self.sub1_passphrase = 'Dunedog99*'
        self.sub_user1 = '60c4e4491fb5e80006b22dff'

        # Base URLs:
        self.url = 'https://api.kucoin.com'
        self.sandbox_url = 'https://openapi-sandbox.kucoin.com'

        #Channel subscription ID
        self.id = 1

        # Initialize Pandas df objects to run web_hooks_processor
        # These need to be in the WebHooks processor-Analysis module
        op_var = {'o': float(), 'lower2': float(), 'upper2': float(), 'lower1': float(), 'upper1': float()}
        self.df_data_open = pd.DataFrame(op_var, index=[])
        cl_var = {'h': float(), 'l': float(), 'c': float(), 'vol': float(), 'RSI': float()}
        self.df_data_close = pd.DataFrame(cl_var, index=[])
        tk_var = {'time': '', 'price': float()}
        self.df_ticker = pd.DataFrame(tk_var, index=[])

        #create a parallel thread for the WebSocket
        t = threading.Thread(target=self.start_ws)
        t.start()

        logger.info('KuCoin Spot client has initialized')

    def get_balance(self):
        # Addressed in get_sub_account
        return

    def place_order(self):
        # Methods for this in TradeData
        return

    def cancel_order(self):
        # Methods for this in TradeData
        return

    def get_order_status(self):
        # Methods for this in TradeData
        # get_order_details?
        client = TradeData(self.sub1_key, self.sub1_secret, self.sub1_passphrase)
        order_stat = client.get_order_details('6106e6ea87af860006a4f53a')
        return

    def get_completed_orders(self):
        # Print is working
        args = {'status': 'done', 'side': 'sell'}
        orders = TradeData(self.main_key, self.main_secret, self.main_passphrase).get_order_list()
        for i in orders.get('items'):
            print('===============================')
            print('order ID:', i.get('id'))
            print('order type:', i.get('type'))
            print('currency pair:', i.get('symbol'))
            print('order side:', i.get('side'))
            print('order size:', i.get('size'))
            print('funds recvd:', i.get('dealFunds'))

        for o in orders:
            if str(o).startswith('currentPage'):
                print(o)
                for i in o:
                    print(i)
                    if str(i).startswith('items'):
                        print(i)

        return orders

    def create_limit_buy_order(self):
        # Return:
        # {'orderId': '6106e6ea87af860006a4f53a'}
        client = TradeData(self.sub1_key, self.sub1_secret, self.sub1_passphrase)
        order_id = client.create_limit_order('ETH-USDT', 'sell', '0.1', '12000.01', '10906')
        return order_id

    def create_market_order(self, side, oid):
        client = TradeData(self.sub1_key, self.sub1_secret, self.sub1_passphrase)
        order = client.create_market_order('ETH-USDT', side, oid)
        return order

    def cancel_all_orders(self):
        # Return:
        # {'cancelledOrderIds': ['6106e6ea87af860006a4f53a']}
        client = TradeData(self.sub1_key, self.sub1_secret, self.sub1_passphrase)
        cancellation = client.cancel_all_orders()
        return cancellation

    def get_accounts_info(self):
        """Returns a 2D list of all accounts
            each inner list is one account in the format
            [
            [subName1, subUserID1],
            [subName2, subUserID2]
            ]"""

        acc = UserData(self.main_key, self.main_secret, self.main_passphrase).get_sub_accounts()
        accounts_info = []

        for a in acc:
            if not str(a['subName']).startswith('robot'):
                accounts_info.append([a['subName'], a['subUserId']])
                # print('Account Name:', a['subName'])
                # print('Sub User ID:', a['subUserId'])
        return accounts_info

    def get_sub_account(self):
        sub_acc = UserData(self.main_key, self.main_secret, self.main_passphrase).get_sub_account(self.sub_user1)
        main_balances = dict()
        trade_balances = dict()
        margin_balances = dict()
        """ First Level: rint(a0
        subUserId
        subName
        mainAccounts --- List of dictionaries
        tradeAccounts --- List of dictionaries
        marginAccounts --- List of dictionaries(empty)
        sample dict: {'currency': 'ETH', 'balance': '0.1', 'available': '0.1', 'holds': '0', 'baseCurrency': 'BTC', 
        'baseCurrencyPrice': '16.78331068', 'baseAmount': '0.00595829'}"""

        print('Account Name:', sub_acc['subName'])
        for a in sub_acc:
            # print(a,":", sub_acc[a])
            if str(a).endswith('Accounts'):
                for c in sub_acc[a]:
                    if float(c.get('balance')) > 0:
                        print(a, c.get('currency'), 'balance:', c.get('balance'))

        return

# ===============Web Sockets implemented here=========================
    # REDUNDANT: IMPLEMENTED IN WebSocket class @ kucoin.web_socket.web_socket.py
    #Get the websocket token
    def get_ws_token(self, public=True):
        # https://docs.kucoin.com/#websocket-feed
        api = KucoinBaseRestApi(self.main_key, self.main_secret, self.main_passphrase, False, self.url, False)
        if public:
            tk = api._request('POST', '/api/v1/bullet-public')
        else:
            tk = api._request('POST', '/api/v1/bullet-private')

        token = tk['token']
        is_dict = tk['instanceServers'][0]
        ws_endpoint = is_dict['endpoint']
        ws_pingInterval = int(is_dict['pingInterval'])/1000
        ws_pingTimeout = int(is_dict['pingTimeout'])/1000

        connect_str = ws_endpoint+'?token='+token

        print(f'Token: {tk}')
        print('ping interval:', ws_pingInterval)
        print('timeout:', ws_pingTimeout)

        return (connect_str, ws_pingTimeout, ws_pingInterval)

    # open the connection
    def start_ws(self, public=True):
        connect_str, ws_pingTimeout, ws_pingInterval = self.get_ws_token(public)
        self.ws = websocket.WebSocketApp(connect_str, on_open=self.on_open, on_close=self.on_close,
                                         on_error=self.on_error, on_message=self.on_message)

        self.ws.run_forever(ping_timeout=ws_pingTimeout, ping_interval=ws_pingInterval)
        return

    # ================================web socket actions========================================
    def on_open(self, ws):
        logger.info('Kucoin Web Socket connection opened.')
        self.subscribe_channel('ETH-USDT')
        return

    def on_close(self, ws):
        logger.warning(f'KuCoin Web Socket connection has closed.')
        return

    def on_error(self, ws, msg):
        logger.error(f'KuCoin Web Socket connection error: {msg}')
        return

    def on_message(self, ws, msg):
        # Returns a str(dict())
        if msg.startswith('{"type":"message"'):
            self.process_ticker(msg)
        else:
            print(f'Other message: {msg}')
            print('Websocket connection is now working')
        return

    # Process ticker info received with web socket
    def process_ticker(self, msg):
        d = json.loads(msg) # convert message to dict
        sequence = d['data']['sequence']
        time_stamp = d['data']['time']
        price = d['data']['price']
        print('Ticker ETH-USDT. Price:',price, '==Time:', time_stamp)
        # time stamp is is milliseconds
        return

    # subscribe to web socket channels
    def subscribe_channel(self, symbol):
        data = dict()

        data['id'] = self.id
        data['type'] = 'subscribe'
        data['topic'] = '/market/ticker:'+symbol
        data['privateChannel'] = 'false'
        data['response'] = 'true'

        self.ws.send(json.dumps(data))

        self.id += 1
        return





