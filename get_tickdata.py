#!/usr/bin/python3

import os, gzip, shutil, urllib, datetime 
from io import BytesIO

url                 = 'https://tickdata.fxcorporate.com/'
first_year          = 2018
last_year           = datetime.datetime.now().year
symbol_list         = ['AUDCAD','AUDCHF','AUDJPY', 'AUDNZD','CADCHF','EURAUD','EURCHF','EURGBP' 
'EURJPY','EURUSD','GBPCHF','GBPJPY','GBPNZD','GBPUSD','GBPCHF','GBPJPY'
'GBPNZD','NZDCAD','NZDCHF','NZDJPY','NZDUSD','USDCAD','USDCHF','USDJPY']
error_list          = []

for symbol in symbol_list:
    os.makedirs(symbol, exist_ok=True)
    for year in range(first_year, last_year + 1):
        if year != last_year:
            end_week = datetime.date(year,12,29).isocalendar()[1] # last week of the year
        else:
            end_week = datetime.datetime.now().isocalendar()[1] - 1 # get last week
        for weeknumber in range(1, end_week):
            url_data = url + symbol+'/'+str(year)+'/'+str(weeknumber)+'.csv.gz'
            print(url_data)
            try:
                requests    = urllib.request.urlopen(url_data)
            except urllib.error.HTTPError as exception:
                error_list.append(url_data)
                print(exception)
            with open(f'{symbol}/{year}_{weeknumber}.csv', 'wb') as local_file:
                compressed = BytesIO(requests.read())
                decompressed = gzip.GzipFile(fileobj=compressed)
                shutil.copyfileobj(decompressed, local_file)

print(f'Missing:\n{error_list}')
