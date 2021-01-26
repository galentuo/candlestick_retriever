#!/usr/bin/env python
# coding: utf-8

"""Download historical candlestick data for all trading pairs on Binance.com.
All trading pair data is checked for integrity, sorted and saved as both a CSV
and a Parquet file. The CSV files act as a raw buffer on every update round.
The Parquet files are much more space efficient (~50GB vs ~10GB).
"""

__author__ = 'GOSUTO.AI, github.com/aliel'

import os
import subprocess
import json
import random

import time
from datetime import date, datetime, timedelta

import requests
import hashlib

import pandas as pd

import src.preprocessing as pp
from src.args import get_args

API_BASE = 'https://api.binance.com/api/v3/'

LABELS = [
    'open_time',
    'open',
    'high',
    'low',
    'close',
    'volume',
    'close_time',
    'quote_asset_volume',
    'number_of_trades',
    'taker_buy_base_asset_volume',
    'taker_buy_quote_asset_volume',
    'ignore'
]


METADATA = {
    'id': 'jorijnsmit/binance-full-history',
    'title': 'Binance Full History',
    'isPrivate': False,
    'licenses': [{'name': 'other'}],
    'keywords': [
        'business',
        'finance',
        'investing',
        'currencies and foreign exchange'
    ],
    'collaborators': [],
    'data': []
}

API_KEY = None
API_SECRET = None

def write_metadata(n_count):
    """Write the metadata file dynamically so we can include a pair count."""

    METADATA['subtitle'] = f'1 minute candlesticks for all {n_count} cryptocurrency pairs'
    METADATA['description'] = f"""### Introduction\n\nThis is a collection of all 1 minute candlesticks of all cryptocurrency pairs on [Binance.com](https://binance.com). All {n_count} of them are included. Both retrieval and uploading the data is fully automated—see [this GitHub repo](https://github.com/gosuto-ai/candlestick_retriever).\n\n### Content\n\nFor every trading pair, the following fields from [Binance's official API endpoint for historical candlestick data](https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md#klinecandlestick-data) are saved into a Parquet file:\n\n```\n #   Column                        Dtype         \n---  ------                        -----         \n 0   open_time                     datetime64[ns]\n 1   open                          float32       \n 2   high                          float32       \n 3   low                           float32       \n 4   close                         float32       \n 5   volume                        float32       \n 6   quote_asset_volume            float32       \n 7   number_of_trades              uint16        \n 8   taker_buy_base_asset_volume   float32       \n 9   taker_buy_quote_asset_volume  float32       \ndtypes: datetime64[ns](1), float32(8), uint16(1)\n```\n\nThe dataframe is indexed by `open_time` and sorted from oldest to newest. The first row starts at the first timestamp available on the exchange, which is July 2017 for the longest running pairs.\n\nHere are two simple plots based on a single file; one of the opening price with an added indicator (MA50) and one of the volume and number of trades:\n\n![](https://www.googleapis.com/download/storage/v1/b/kaggle-user-content/o/inbox%2F2234678%2Fb8664e6f26dc84e9a40d5a3d915c9640%2Fdownload.png?generation=1582053879538546&alt=media)\n![](https://www.googleapis.com/download/storage/v1/b/kaggle-user-content/o/inbox%2F2234678%2Fcd04ed586b08c1576a7b67d163ad9889%2Fdownload-1.png?generation=1582053899082078&alt=media)\n\n### Inspiration\n\nOne obvious use-case for this data could be technical analysis by adding indicators such as moving averages, MACD, RSI, etc. Other approaches could include backtesting trading algorithms or computing arbitrage potential with other exchanges.\n\n### License\n\nThis data is being collected automatically from crypto exchange Binance."""

    with open('compressed/dataset-metadata.json', 'w') as file:
        json.dump(METADATA, file, indent=4)


def get_batch(params=None, api_path='klines', timeout=30):
    """Use a GET request to retrieve a batch of candlesticks. Process the JSON into a pandas
    dataframe and return it. If not successful, return an empty dataframe.
    """
        
    try:
        headers = None
        # pass credential if needed
        if API_KEY != None and API_SECRET != None:
            servertime = requests.get(f'{API_BASE}time')
            servertimeobject = json.loads(servertime.text)
            servertimeint = servertimeobject['serverTime']
            hashedsig = hashlib.sha256(API_SECRET.encode('utf-8'))
            params['signiature'] = hashedsig
            params['timestamp'] = servertimeint
            headers = {'X-MBX-APIKEY': API_KEY}
        response = requests.get(f'{API_BASE}{api_path}', params, timeout=timeout, headers=headers)
        
    except requests.exceptions.ConnectionError:
        print('Connection error, Cooling down for 5 mins...')
        time.sleep(5 * 60)
        return get_batch(params, api_path, timeout)
    
    except requests.exceptions.Timeout:
        print('Timeout, Cooling down for 5 min...')
        time.sleep(5 * 60)
        return get_batch(params, api_path, timeout)
    
    except requests.exceptions.ConnectionResetError:
        print('Connection reset by peer, Cooling down for 5 min...')
        time.sleep(5 * 60)
        return get_batch(params, api_path, timeout)

    if response.status_code == 200:
        if api_path == 'klines':
            return pd.DataFrame(response.json(), columns=LABELS)
        else:
            return pd.DataFrame(response.json())

    print(f'Got erroneous response back: {response}')
    return pd.DataFrame([])

def all_trade_to_csv(base, quote, params=None, with_parquet=False):
    """Collect a list of candlestick batches with all candlesticks of a trading pair,
    concat into a dataframe and write it to CSV.
    """

    args = get_args()
    filepath = f'data/trade_{base}-{quote}.csv'

    api_path = 'aggTrades'

    # see if there is any data saved on disk already
    try:
        if params['fromId'] == 0:
            batches = [pd.read_csv(filepath)]
            last_id = batches[-1]['a'].max()
            params['fromId'] = last_id + 1
        else:
            last_id = params['fromId']
            params['fromId'] = last_id + 1
        batches = [pd.DataFrame([])] # clear
        # if already have data start from last_id
    except FileNotFoundError:
        batches = [pd.DataFrame([])]
        last_id = params['fromId']


    old_lines = len(batches[-1].index)

    # gather all trades available, starting from the last id loaded from disk or provided fromId
    # stop if the id that comes back from the api is the same as the last one
    previous_id = -1

    while previous_id != last_id:
        # stop if we reached data
        if previous_id >= last_id and previous_id > 0:
            break

        previous_id = last_id

        new_batch = get_batch(
            params=params,
            api_path=api_path,
            timeout=args.timeout
        )

        # requesting candles from the future returns empty
        # also stop in case response code was not 200
        if new_batch.empty:
            break

        last_id = new_batch['a'].max()
        print(last_id, previous_id)
        timestamp = new_batch['T'].max()

        # update fromId to continue from last id
        params['fromId'] = last_id + 1;

        batches.append(new_batch)
        last_datetime = datetime.fromtimestamp(timestamp / 1000)

        covering_spaces = 20 * ' '
        print(datetime.now(), base, quote, str(last_datetime)+covering_spaces, end='\r', flush=True)

        # if huge data
        # compute size @TODO get field not hardcoded
        lines = len(batches)*params['limit']
        if lines >= 5000:
            df = pp.prepare_df(batches, field='a');
            pp.append_to_csv(df, filepath)
            # reset
            batches.clear()


    if len(batches) > 1:
        df = pp.prepare_df(batches, field='a')

    if with_parquet:
        # write clean version of csv to parquet
        parquet_name = f'{base}-{quote}.parquet'
        full_path = f'compressed/{parquet_name}'
        pp.write_raw_to_parquet(df, full_path)
        METADATA['data'].append({
            'description': f'All {data_type} history for the pair {base} and {quote} at {interval} intervals. Counts {df.index.size} records.',
            'name': parquet_name,
            'totalBytes': os.stat(full_path).st_size,
            'columns': []
        })

    # in the case that new data was gathered write it to disk
    if len(batches) > 1:
        pp.append_to_csv(df, filepath)
        #df.to_csv(filepath, index=False)
        return len(df.index) - old_lines
    return 0
    
def all_candle_to_csv(base, quote, params=None, interval='1m', with_parquet=False):
    """Collect a list of candlestick batches with all candlesticks of a trading pair,
    concat into a dataframe and write it to CSV.
    """

    filepath = f'data/candle_{base}-{quote}_interval-{interval}.csv'

    api_path = 'klines'

    # see if there is any data saved on disk already
    try:
        batches = [pd.read_csv(filepath)]
        last_timestamp = batches[-1]['open_time'].max()
    except FileNotFoundError:
        batches = [pd.DataFrame([], columns=LABELS)]
        last_timestamp = params['startTime']

    old_lines = len(batches[-1].index)

    # gather all candlesticks available, starting from the last timestamp loaded from disk or 0
    # stop if the timestamp that comes back from the api is the same as the last one
    previous_timestamp = None

    while previous_timestamp != last_timestamp:
        # stop if we reached data from today
        if date.fromtimestamp(last_timestamp / 1000) >= date.today():
            break

        previous_timestamp = last_timestamp

        params['startTime'] = last_timestamp + 1

        new_batch = get_batch(
            params=params,
            api_path=api_path
        )
        
        # requesting candles from the future returns empty
        # also stop in case response code was not 200
        if new_batch.empty:
            break

        last_timestamp = new_batch['open_time'].max()
            
        # sometimes no new trades took place yet on date.today();
        # in this case the batch is nothing new
        if previous_timestamp == last_timestamp:
            break

        batches.append(new_batch)
        last_datetime = datetime.fromtimestamp(last_timestamp / 1000)

        covering_spaces = 20 * ' '
        print(datetime.now(), base, quote, interval, str(last_datetime)+covering_spaces, end='\r', flush=True)

        lines = len(batches)*params['limit']
        if lines >= 5000:
            df = pp.prepare_df(batches, field='open_time');
            pp.append_to_csv(df, filepath)
            # reset
            batches.clear()

    if len(batches) > 1:
        df = pp.prepare_df(batches, field='open_time')

    if with_parquet:
        # write clean version of csv to parquet
        parquet_name = f'{base}-{quote}.parquet'
        full_path = f'compressed/{parquet_name}'
        pp.write_raw_to_parquet(df, full_path)
        METADATA['data'].append({
            'description': f'All {data_type} history for the pair {base} and {quote} at {interval} intervals. Counts {df.index.size} records.',
            'name': parquet_name,
            'totalBytes': os.stat(full_path).st_size,
            'columns': []
        })

    # in the case that new data was gathered write it to disk
    if len(batches) > 1:
        pp.append_to_csv(df, filepath)
        #df.to_csv(filepath, index=False)
        return len(df.index) - old_lines
    return 0

def get_historical_candlesticks(base, quote):
    args = get_args()

    with_parquet = args.parquet
    
    symbol = base+quote
    interval = args.interval
    start_at = args.start_at
    limit = args.limit
    
    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': start_at,
        'limit': limit
    }

    return all_candle_to_csv(base=base, quote=quote, params=params,
                             interval=interval, with_parquet=with_parquet)

def get_historical_agg_trades(base, quote):
    args = get_args()

    with_parquet = args.parquet
    
    symbol = base+quote
    start_at = args.start_at
    limit = args.limit
    
    params = {
        'symbol': symbol,
        'fromId': start_at,
        'limit': limit
    }
    
    return all_trade_to_csv(base=base, quote=quote, params=params, with_parquet=with_parquet)

def main():
    """Main loop; loop over all currency pairs that exist on the exchange.
    """
    args = get_args()
    print(args)

    with_parquet = args.parquet
    upload_parquet = args.upload
    interval = args.interval
    data_type = args.dtype
    pairs = "".join(args.pairs.split()) # remove whitespace 

    if pairs == 'all':
        # get all pairs currently available
        all_symbols = pd.DataFrame(requests.get(f'{API_BASE}exchangeInfo').json()['symbols'])
        all_pairs = [tuple(x) for x in all_symbols[['baseAsset', 'quoteAsset']].to_records(index=False)]
    else:
        all_pairs = [tuple(pair.split('-')) for pair in pairs.split(',')]
        #all_pairs = [('BTC', 'USDT')]
        #all_pairs = [('DF', 'ETH')]

    # randomising order helps during testing and doesn't make any difference in production
    random.shuffle(all_pairs)

    # make sure data folders exist
    os.makedirs('data', exist_ok=True)
    os.makedirs('compressed', exist_ok=True)

    # do a full update on all pairs
    n_count = len(all_pairs)
    for n, pair in enumerate(all_pairs, 1):
        base, quote = pair

        # default params for klines
        symbol = base+quote
        if data_type == 'candle':
            new_lines = get_historical_candlesticks(base, quote)

        elif data_type == 'trade':
            new_lines = get_historical_agg_trades(base, quote)

        if new_lines > 0:
            print(f'{datetime.now()} {n}/{n_count} Wrote {new_lines} new lines to file for {data_type}_{base}-{quote}_interval-{interval}')
        else:
            print(f'{datetime.now()} {n}/{n_count} Already up to date with {data_type}_{base}-{quote}_interval-{interval}')

    # clean the data folder and upload a new version of the dataset to kaggle
    try:
        os.remove('compressed/.DS_Store')
    except FileNotFoundError:
        pass

    if with_parquet and upload_parquet:
        write_metadata(n_count)
        yesterday = date.today() - timedelta(days=1)
        subprocess.run(['kaggle', 'datasets', 'version', '-p', 'compressed/', '-m', f'full update of all {n_count} pairs up to {str(yesterday)}'])
        os.remove('compressed/dataset-metadata.json')

if __name__ == '__main__':
    args = get_args()

    if args.check_trade != None:
        pp.check_trade_index(args.check_trade)
    else:
        main()
