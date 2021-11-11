import gzip
import urllib
import datetime 

url                 = 'https://tickdata.fxcorporate.com/' ##This is the base url 
url_suffix          = 'csv.gz'
first_year          = 2018
last_year           = datetime.datetime.now().year
start_wk            = 1
symbol_list         = ['AUDCAD','AUDCHF','AUDJPY', 'AUDNZD','CADCHF','EURAUD','EURCHF','EURGBP' 
'EURJPY','EURUSD','GBPCHF','GBPJPY','GBPNZD','GBPUSD','GBPCHF','GBPJPY'
'GBPNZD','NZDCAD','NZDCHF','NZDJPY','NZDUSD','USDCAD','USDCHF','USDJPY']
error_list          = []

for symbol in symbol_list:
    for year in range(first_year, last_year + 1):
        if year != last_year:
            end_week = datetime.date(year,12,29).isocalendar()[1] ##last week of the year
        else:
            end_week = datetime.datetime.now().isocalendar()[1] # gt current week

        print(symbol, year)
        for weeknumber in range(start_wk, end_week):
            url_data = url + symbol+'/'+str(year)+'/'+str(weeknumber)+'.'+url_suffix
            print(url_data)
            try:
                requests    = urllib.request.urlopen(url_data)
                with gzip.open(f'{symbol}_{year}_{weeknumber}.{url_suffix}', 'wb') as zip:
                    zip.write(requests.read())
            except urllib.error.HTTPError as exception:
                error_list  += [url_data]
                print(exception)

print(f'Missing:\n{error_list}')