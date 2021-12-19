import json
from flask import Flask, request, Response
from datetime import datetime
from time import sleep
from threading import Thread
from kucoin.clients.kuCoin_client import KuCoinSpotClient as Client
from kucoin.web_hooks_processor.web_hooks_processor import WebHookProcessor

app = Flask('hooks')
dataOpenOutPath = 'D:/Dropbox/Trading/James/incomingHooks/open/'
dataCloseOutPath = 'D:/Dropbox/Trading/James/incomingHooks/close/'
tradeOutPath = 'D:/Dropbox/Trading/James/incomingHooks/trades/'
dataOutPath = 'D:/Dropbox/Trading/James/incomingHooks/data/'
actionOutPath = 'D:/Dropbox/Trading/James/incomingHooks/action/'

analysis = None

# Webhook processor object will be called to process wh:
#(1) Will send df to Analysis object to update the df's
#(2) Will append data to csv file

# gets the timestamp data to include as part of the file name. Helps keep a record of received hooks
# Finished 8/20
def getTime(filename):

    date = datetime.now().strftime("%Y_%m_%d_%I-%M-%S")


    """    openFile = f'open_{date}'
        closeFile = f'close_{date}'
        tradeFile = f'trade_{date}'
        dataFile = f'data_{date}'
        if filename == 'openFile':
            return openFile
        elif filename == 'closeFile':
            return closeFile
        elif filename == 'tradeFile':
            return tradeFile
        elif filename == 'dataFile':
            return dataFile
        else:
            return(date)"""

    return f'{filename}_{date}'

# validate: Compares the incoming webhook against a sample to avoid code injections
def validateData(sample, content):
    # sample = '1_open_sample.json'
    # bad = 'bad.json'
    validationResult = 0
    sample_dict = dict()
    # print('sample:', sample)
    validation = False

    # print('content:',content)

    with open(sample, 'r') as infile:
        sample_dict = json.load(infile)

    sample_keys = list(sample_dict.keys())
    content_keys = list(content.keys())
    sample_values = list(sample_dict.values())
    content_values = list(content.values())

    if len(sample_keys) == len(content_keys): #Same number of keys?
        for k in range (0,len(sample_keys)):
            if sample_keys[k] == content_keys[k]: # Do the keys match?
                    if len(sample_values) == len(content_values): # Same number of values?
                        # compare types
                        for i in range (0, len(sample_values)):
                            try:
                                float(sample_values[i]) # can convert sample value to float?
                                try:
                                    float(content_values[i]) #Above True?: This should be float
                                except:
                                    validationResult += 1 #content value is str but should be float
                                    # print('Unexpected value in position', i, 'of the received JSON: Expected float')
                            except: # if sample and content are str, they should be the same
                                if sample_values[i] == sample_values[i]:
                                    pass
                                else:
                                    validationResult += 1
                                    # print('Unexpected string value in position', i, 'of the received JSON')
                    else:
                        validationResult += 1
            else:
                validationResult += 1
    else:
        validationResult += 1

    if validationResult == 0:
        validation = True
    else:
        validation = False
    return validation

@app.route('/ac', methods=['POST'])
def dataPostJsonHandler():
    content = request.get_json()
    actionFile = getTime('action')
    sample = actionOutPath+'1_action_sample.json'

    if validateData(sample, content):
        with open(actionOutPath+actionFile+'.json', 'w') as outfile:
            json.dump(content, outfile)
        print('Successful validation:', content)
        # send to processor
        Thread(target=WebHookProcessor().process_webhook_action, args=[content, analysis]).start()

    else:
        with open(actionOutPath + actionFile + '_incorrect.json', 'w') as outfile:
            json.dump(content, outfile)
        print('Incorrect action file. File not processed')
    return Response(status=200)

# dt: Handles data(open, close from different timelines)
# Finished 8/20

@app.route('/dt', methods=['POST'])
def opPostJsonHandler():
    content = request.get_json()

    # print('request type:', type(request.data))
    # print('request data:', request.data)

    if content['type'] == 'data':
        if content['title'] == 'open':
            sample = dataOpenOutPath+'1_open_sample.json'
            openFile = getTime('open')
            if validateData(sample, content):
                with open(dataOpenOutPath + openFile + '.json', 'w') as outfile:
                    json.dump(content, outfile)
                print('Successful validation:', content)
                Thread(target=WebHookProcessor().process_webhook_data, args=[content, analysis]).start()
            else:
                with open(dataOpenOutPath + openFile + '_bad_.json', 'w') as outfile:
                    json.dump(content, outfile)
                print('Message not validated', content)
        elif content['title'] == 'close':
            sample = dataCloseOutPath+'1_close_sample.json'
            closeFile = getTime('close')
            if validateData(sample, content):
                Thread(target=WebHookProcessor().process_webhook_data, args=[content, analysis]).start()
                print('Successful validation:', content)
                with open(dataCloseOutPath + closeFile + '.json', 'w') as outfile:
                    json.dump(content, outfile)
            else:
                with open(dataCloseOutPath + closeFile + '_bad_.json', 'w') as outfile:
                    json.dump(content, outfile)
                print('Message not validated', content)
        else:
            print('Message headers not valid', content)
    else:
        print('Message headers not valid', content)

    return Response(status=200)

#NOT USED
"""@app.route('/cl', methods=['POST'])
def clPostJsonHandler():
    content = request.get_json()
    closeFile = getTime('closeFile')
    sample = dataCloseOutPath+'1_close_sample.json'

    if validateData(sample, content):
        with open(dataCloseOutPath+closeFile+'.json', 'w') as outfile:
            json.dump(content, outfile)
        print('Successful validation:', content)
        # transfer to main execution
    else:
        with open(dataCloseOutPath + closeFile + '_incorrect.json', 'w') as outfile:
            json.dump(content, outfile)
        print('Incorrect dataClose file. File not processed')
    return Response(status=200)"""

# tr: handles transaction signals (buy, sell from different timelines)
# Finished 8/20
@app.route('/tr', methods=['POST'])
def trPostJsonHandler():
    content = request.get_json()
    tradeFile = getTime('trade')
    sample = tradeOutPath+'1_trade_sample.json'

    if validateData(sample, content):
        with open(tradeOutPath+tradeFile+'.json', 'w') as outfile:
            json.dump(content, outfile)
        print('Successful validation:', content)
        # send to processor
        Thread(target=WebHookProcessor().process_webhook_trade, args=[content, analysis]).start()

    else:
        with open(tradeOutPath + tradeFile + '_incorrect.json', 'w') as outfile:
            json.dump(content, outfile)
        print('Incorrect dataClose file. File not processed')
    return Response(status=200)

# run: starts the flask app from the 'main' module
# Finished 8/20
def run_flsk(analysis_ob):
    global analysis
    analysis = analysis_ob
    sleep(1)
    app.run(host='192.168.0.118', port=80)
    return

"""if __name__ == '__main__':
    app.run(debug=True)"""
