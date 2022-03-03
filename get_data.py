#!/usr/bin/python3

import concurrent.futures
import urllib.request
import datetime
import shutil
import gzip
import os
import io

base_url            = 'https://tickdata.fxcorporate.com/'
first_year          = 2018
last_year           = datetime.datetime.now().year
# https://www.fxcm.com/markets/insights/anatomy-of-a-volatile-currency/#volatile-currencies-in-action
symbol_list         = ['EURGBP', 'GBPCHF','GBPUSD']
# symbol_list         = ['AUDCAD','AUDCHF','AUDJPY', 'AUDNZD','CADCHF','EURAUD','EURCHF','EURGBP' 
# 'EURJPY','EURUSD','GBPCHF','GBPJPY','GBPNZD','GBPUSD','GBPCHF','GBPJPY'
# 'GBPNZD','NZDCAD','NZDCHF','NZDJPY','NZDUSD','USDCAD','USDCHF','USDJPY']
bad_url_list        = []

def download(url, file_path):
    try:
        requests    = urllib.request.urlopen(url)
        with open(file_path, 'wb') as local_file:
            compressed = io.BytesIO(requests.read())
            decompressed = gzip.GzipFile(fileobj=compressed)
            shutil.copyfileobj(decompressed, local_file)
        print(url)
        return True
    except urllib.error.HTTPError as exception:
        print(url, exception)
        return False

for symbol in symbol_list:
    os.makedirs(symbol, exist_ok=True)
    for year in range(first_year, last_year + 1):
        print(symbol, year)
        if year != last_year:
            end_week    = datetime.date(year,12,29).isocalendar()[1] # last week of the year
        else:
            end_week    = datetime.datetime.now().isocalendar()[1] - 2 # two week before today
        for weeknumber in range(1, end_week + 1):
            data_url    = base_url + symbol+'/'+str(year)+'/'+str(weeknumber)+'.csv.gz'
            file_path   = f'{symbol}/{year}_{weeknumber}.csv'
            if not download(data_url, file_path):
                bad_url_list.append(data_url)

print("\nBad urls:")
for bad_url in bad_url_list:
    print(bad_url)
