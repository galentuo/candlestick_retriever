import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="Download historical candlestick data for all available trading pairs and historical trades, @see https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md to understand related parameters")
    
    parser.add_argument('-dt', '--dtype',
                        help='Provide which data to fetch candle|trade (default candle)', default='candle')
    parser.add_argument('-p', '--pairs',
                        help='Provide pair names to fetch (default all)', default='all')

    parser.add_argument('-i', '--interval',
                        help='Set time frame interval (default 1m) only if type is candle', default='1m')
    
    parser.add_argument('--limit',
                        help='Set max results returned per request (default 1000)', default=1000)

    parser.add_argument('--start-at',
                        help='Set start time or index according to data type (default 0)',
                        default=0, type=int)

    parser.add_argument('--parquet',
                        help='Build parquet as well (default False)', default=False, type=bool)

    parser.add_argument('--upload',
                        help='Upload parquet to kaggle (default False)', default=False, type=bool)

    parser.add_argument('--key',
                        help='Binance api key', default=None)

    parser.add_argument('--secret',
                        help='Binance secret key', default=None)

    parser.add_argument('--timeout',
                        help='Stop waiting for a response from Binance after (x) Seconds (default 30)', default=30)

    parser.add_argument('--check-trade',
                        help='Check trade consistency (provide a csv filepath)', default=None)

    args = parser.parse_args()

    return args


def get_args():
    args = parse_args()
    return args
