import tkinter as tk
import logging
import pprint
from threading import Thread

from kucoin.user.user import UserData
from kucoin.trade.trade import TradeData
from kucoin.clients.kuCoin_client import KuCoinSpotClient as SpotClient
from kucoin.analysis.analysis import Analysis
import kucoin.daemon as start_daemon
from kucoin.web_socket.web_socket import WebSocket

# Analysis object must be initialized first> It needs to be persistent. It will contain the live df's)
analysis = Analysis() #=========================================================================================================


logger = logging.getLogger()

logger.setLevel(logging.DEBUG)

stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s :: %(message)s')
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.INFO)

file_handler = logging.FileHandler('info.log')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)

logger.addHandler(stream_handler)
logger.addHandler(file_handler)

if __name__ == '__main__':
    sandbox_key = '6102dc5bbc85c200065a5cf7'
    sandbox_secret = '98f593a9-de00-4ae6-90f8-47d7f239773b '
    sandbox_passphrase = 'Dunedog99*'

    sb_sub1_key = '61031b0fbc85c200065a5d4b'
    sb_sub1_secret = '885767af-5871-41ca-ab3e-957c527cc4e6'
    sb_sub1_passphrase = 'Dunedog99*'
    sb_sub_user1 = ''

    main_key = '61046016ead9100006ee2c3c'
    main_secret = 'bca41fe7-bf77-42a2-a16c-b4f2b6a72ade'
    main_passphrase = 'Dunedog99*'

    sub1_key = '6106e528eadc7d000640f64d'
    sub1_secret = 'bf18297d-f270-4bce-a9ba-617d9f5131c2'
    sub1_passphrase = 'Dunedog99*'
    sub_user1 = '60c4e4491fb5e80006b22dff'

    #Base URLs:
    base_url = 'https://api.kucoin.com'
    sandbox_url = 'https://openapi-sandbox.kucoin.com'

    #Web Socket URLs:
    wss_url = ''

    is_sandbox = False
    is_v1api = False
    url = ''

    def get_balance(self):
        # Addressed in get_sub_account
        return


    def place_order(self):
        # Methods for this in TradeData
        return


    def cancel_order(self):
        # Methods for this in TradeData
        return


    def get_order_status():
        # Methods for this in TradeData
        # get_order_details?
        client = TradeData(sub1_key, sub1_secret, sub1_passphrase)
        order_stat = client.get_order_details('6106e6ea87af860006a4f53a')
        return

    def get_completed_orders():
        # Print is working
        args = {'status':'done', 'side':'sell'}
        orders = TradeData(main_key, main_secret, main_passphrase).get_order_list()
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

    def create_limit_buy_order():
        # Return:
        # {'orderId': '6106e6ea87af860006a4f53a'}
        client = TradeData(sub1_key, sub1_secret, sub1_passphrase)
        order_id = client.create_limit_order('ETH-USDT', 'sell', '0.1', '12000.01', '10906')
        return order_id

    def create_market_order(side, oid):
        client = TradeData(sub1_key, sub1_secret, sub1_passphrase)
        order = client.create_market_order('ETH-USDT', side, oid)
        return order

    def cancel_all_orders():
        # Return:
        # {'cancelledOrderIds': ['6106e6ea87af860006a4f53a']}
        client = TradeData(sub1_key, sub1_secret, sub1_passphrase)
        cancellation = client.cancel_all_orders()
        return cancellation

    def get_accounts_info():
        """Returns a 2D list of all accounts
            each inner list is one account in the format
            [
            [subName1, subUserID1],
            [subName2, subUserID2]
            ]"""

        acc = UserData(main_key, main_secret, main_passphrase).get_sub_accounts()
        accounts_info = []

        for a in acc:
            if not str(a['subName']).startswith('robot'):
                accounts_info.append([a['subName'], a['subUserId']])
                # print('Account Name:', a['subName'])
                # print('Sub User ID:', a['subUserId'])
        return accounts_info

    def get_sub_account():

        sub_acc = UserData(main_key, main_secret, main_passphrase).get_sub_account(sub_user1)
        main_balances = dict()
        trade_balances = dict()
        margin_balances = dict()
        """ First Level: rint(a0
        subUserId
        subName
        mainAccounts --- List of dictionaries
        tradeAccounts --- List of dictionaries
        marginAccounts --- List of dictionaries(empty)
        sample dict: {'currency': 'ETH', 'balance': '0.1', 'available': '0.1', 'holds': '0', 'baseCurrency': 'BTC', 'baseCurrencyPrice': '16.78331068', 'baseAmount': '0.00595829'}"""

        print('Account Name:', sub_acc['subName'])
        for a in sub_acc:
            # print(a,":", sub_acc[a])
            if str(a).endswith('Accounts'):
                for c in sub_acc[a]:
                    if float(c.get('balance')) > 0:
                        print(a, c.get('currency'), 'balance:', c.get('balance'))

        return

    def print_item(item):
        print(item)
        return

    # get_sub_account()
    # print(get_accounts_info())
    # get_completed_orders()
    #pprint.pprint(TradeData(main_key, main_secret, main_passphrase).get_order_list())
    # print(TradeData(main_key, main_secret, main_passphrase).get_order_list())
    # print(create_limit_buy_order())
    # print(get_order_status())
    # print(TradeData(sub1_key, sub1_secret, sub1_passphrase).get_order_list())
    # print(create_market_order('sell', '123'))
    # pprint.pprint(UserData(main_key, main_secret, main_passphrase).get_sub_account(sub_user1))
    # print(cancel_all_orders())
    # pprint.pprint(UserData(sub1_key, sub1_secret, sub1_passphrase).get_account_ledger())
    # pprint.pprint(TradeData(sub1_key, sub1_secret, sub1_passphrase).get_order_list())
    root = tk.Tk()

    from kucoin.daemon import daemon #starts the daemon process that runs timer and  Websocket on a thread. Should go at the end
    print('LIne after the daemon start in main2')
    # Nothing will run after this line
    # t2 = Thread(target=WebSocket(analysis).start_ws())
    # t2.setDaemon(True)
    # t2.start()

    root.mainloop()