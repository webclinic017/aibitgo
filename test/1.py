# This example uses Python 2.7 and the python-request library.

import json

import pandas as pd
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
parameters = {
    'start': '1',
    'limit': '5000',
    'convert': 'USD'
}

headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': '0a51850c-1fbd-4634-9bdb-79edcaa3a439',
}

session = Session()
session.headers.update(headers)

try:
    response = session.get(url, params=parameters)
    data = json.loads(response.text)
    coin_list = []
    for d in data['data']:
        platform = d.get('platform', {})
        if platform:
            if platform.get('name') == 'Binance Smart Chain':
                d['platform_id'] = platform.get('id')
                d['platform_name'] = platform.get('name')
                d['platform_symbol'] = platform.get('symbol')
                d['platform_slug'] = platform.get('slug')
                d['platform_token_address'] = platform.get('token_address')
                quote = d.get('quote', {})
                if quote:
                    usd = quote.get('USD')
                    if usd:
                        d.update(usd)
                coin_list.append(d)
                del d['quote']
                del d['platform']
    df = pd.DataFrame(coin_list)
    df.to_csv('coinmarketcap.csv')
    print(df)
except (ConnectionError, Timeout, TooManyRedirects) as e:
    print(e)
