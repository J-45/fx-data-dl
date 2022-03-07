#!/usr/bin/python3

import concurrent.futures
import urllib.request
import datetime
import shutil
import gzip
import os
import io

NUMBER_OF_THREADS   = 10 # Good enough for 50 Mo/s (400 Mb/s) bandwidth
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
UNPACK              = False

def download(URL, file_path, bad_url_list):
    try:
        REQUESTS    = urllib.request.urlopen(URL)
        if UNPACK:
            with open(file_path, 'wb') as Local_file:
                COMPRESSED = io.BytesIO(REQUESTS.read())
                DECOMPRESSED = gzip.GzipFile(fileobj=COMPRESSED)
                shutil.copyfileobj(DECOMPRESSED, Local_file)
        else:
            with open(file_path+".gz", 'wb') as Local_file:
                shutil.copyfileobj(io.BytesIO(REQUESTS.read()), Local_file)
                
        print(URL)
    except urllib.error.HTTPError as Exception:
        print(URL, Exception)
        bad_url_list.append(URL)
    except ConnectionResetError as Exception: # Catch "Connection reset by peer"
        print(URL, Exception)
        bad_url_list.append(URL)
    return bad_url_list

with concurrent.futures.ThreadPoolExecutor(max_workers=NUMBER_OF_THREADS) as Executor:
    futures = []
    for SYMBOL in SYMBOL_LIST:
        os.makedirs(SYMBOL, exist_ok=True)
        for YEAR in range(FIRST_YEAR, LAST_YEAR + 1):
            if YEAR != LAST_YEAR:
                END_WEEK    = datetime.date(YEAR,12,29).isocalendar()[1] # last week of the year
            else:
                END_WEEK    = datetime.datetime.now().isocalendar()[1] - 2 # two week before today
            for WEEKNUMBER in range(1, END_WEEK + 1):
                DATA_URL    = BASE_URL + SYMBOL+'/'+str(YEAR)+'/'+str(WEEKNUMBER)+'.csv.gz'
                FILE_PATH   = f'{SYMBOL}/{YEAR}_{WEEKNUMBER}.csv'
                futures.append(Executor.submit(download, URL=DATA_URL,file_path=FILE_PATH,bad_url_list=bad_url_list))
                url_count   +=1
    for FUTURE in concurrent.futures.as_completed(futures):
        bad_url_list = FUTURE.result()

print(f"\n{len(bad_url_list)} bad urls out of {url_count}:")
for BAD_URL in bad_url_list:
    print(BAD_URL)
