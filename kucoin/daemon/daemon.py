from threading import Thread
from kucoin.web_socket.web_socket import WebSocket
from kucoin.timer.timer import Timer
from kucoin.analysis.analysis import Analysis
from kucoin.flsk import flsk
from kucoin.read_historical_data import read_historical_data
from multiprocessing import Process

analysis = Analysis('use')

# t1 = None

def runTimer():
    Timer(analysis).file_name_timer()

def printName():
    print('Daemon2 Name: ', __name__)


if __name__ == "kucoin.daemon.daemon":

    print('\nRunning Daemon: ', __name__)
    t1 = Thread(target=runTimer)
    # t1.setDaemon(True)
    t1.start()

    t3 = Thread(target=flsk.run_flsk, args=[analysis])
    t3.start()
    print('started flsk')

    print("started t2--")
    t2 = Thread(target=WebSocket(analysis).start_ws)
    # t2 = Process(target=WebSocket(analysis).start_ws)
    # t2.setDaemon(True)
    t2.start()
    print('started WS')

if __name__ == "__main__":


    print(f'Running Daemon: {__name__}')

    t1 = Process(target=runTimer)
    # t1.setDaemon(True)


    t3 = Process(target=flsk.run_flsk, args=(analysis,))

    print('started flsk')

    print("started t2")
    t2 = Process(target=WebSocket(analysis).start_ws)
    # t2.setDaemon(True)

    t = Thread(target=read_historical_data.read_historical_data(analysis))
    # t.start()

    t1.start()
    t3.start()
    t2.start()
    print('started WS')