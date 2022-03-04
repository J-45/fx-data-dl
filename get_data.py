#!/usr/bin/python3

from socket import error as SocketError
import concurrent.futures
import urllib.request
import datetime
import shutil
import gzip
import os
import io

NUMBER_OF_THREADS   = 10
BASE_URL            = 'https://tickdata.fxcorporate.com/'
FIRST_YEAR          = 2018
LAST_YEAR           = datetime.datetime.now().year
# https://www.fxcm.com/markets/insights/anatomy-of-a-volatile-currency/#volatile-currencies-in-action
SYMBOL_LIST         = ['EURGBP', 'GBPCHF','GBPUSD']
# SYMBOL_LIST         = ['AUDCAD','AUDCHF','AUDJPY', 'AUDNZD','CADCHF','EURAUD','EURCHF','EURGBP' 
# 'EURJPY','EURUSD','GBPCHF','GBPJPY','GBPNZD','GBPUSD','GBPCHF','GBPJPY'
# 'GBPNZD','NZDCAD','NZDCHF','NZDJPY','NZDUSD','USDCAD','USDCHF','USDJPY']
url_count           = 0
bad_url_list        = []

def download(url, FILE_PATH, bad_url_list):
    try:
        requests    = urllib.request.urlopen(url)
        with open(FILE_PATH, 'wb') as local_file:
            compressed = io.BytesIO(requests.read())
            decompressed = gzip.GzipFile(fileobj=compressed)
            shutil.copyfileobj(decompressed, local_file)
        print(url)
    except urllib.error.HTTPError as exception:
        print(url, exception)
        bad_url_list.append(url)
    except ConnectionResetError as exception: # Catch "Connection reset by peer"
        print(url, exception)
        bad_url_list.append(url)
    return bad_url_list

with concurrent.futures.ThreadPoolExecutor(max_workers=NUMBER_OF_THREADS) as executor:
    futures = []
    for SYMBOL in SYMBOL_LIST:
        os.makedirs(SYMBOL, exist_ok=True)
        for YEAR in range(FIRST_YEAR, LAST_YEAR + 1):
            if YEAR != LAST_YEAR:
                END_WEEK    = datetime.date(YEAR,12,29).isocalendar()[1] # last week of the year
            else:
                END_WEEK    = datetime.datetime.now().isocalendar()[1] - 2 # two week before today
            for weeknumber in range(1, END_WEEK + 1):
                DATA_URL    = BASE_URL + SYMBOL+'/'+str(YEAR)+'/'+str(weeknumber)+'.csv.gz'
                FILE_PATH   = f'{SYMBOL}/{YEAR}_{weeknumber}.csv'
                futures.append(executor.submit(download, url=DATA_URL,FILE_PATH=FILE_PATH,bad_url_list=bad_url_list))
                url_count   +=1
    for future in concurrent.futures.as_completed(futures):
        bad_url_list = future.result()

print(f"\n{len(bad_url_list)} bad urls out of {url_count}:")
for bad_url in bad_url_list:
    print(bad_url)
