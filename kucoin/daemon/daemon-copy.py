from threading import Thread
from kucoin.web_socket.web_socket import WebSocket
from kucoin.timer.timer import Timer
from kucoin.analysis.analysis import Analysis
from kucoin.flsk import flsk
from multiprocessing import Process

analysis = Analysis('use')

def runThreads():
    t11 = Thread(target=Timer(analysis).file_name_timer)
    t12 = Thread(target=flsk.run_flsk, args=[analysis])
    t11.start()
    print("started timer from daemon")
    t12.start()
    print('started flsk from daemon')

def runProcesses():
    p10=Process(target=runThreads)
    p11 = Process(target=WebSocket(analysis).start_ws)
    p10.start()
    p11.start()
    print('Started WS from daemon')


if __name__ == "kucoin.daemon.daemon":

    print('Running Daemon: ', __name__)
    runProcesses()


if __name__ == "__main__":

    print(f'Running Daemon: {__name__}')
    runProcesses()