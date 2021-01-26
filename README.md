# candlestick_retriever
Retrieve all historical candlestick data from crypto exchange Binance and upload it to Kaggle.

## Dependencies

- `pandas`
- `requests`
- `pyarrow`
- `kaggle`

## Usage

./main.py --help

```
usage: main.py [-h] [-dt DTYPE] [-p PAIRS] [-i INTERVAL] [--limit LIMIT] [--start-at START_AT] [--parquet PARQUET] [--upload UPLOAD] [--key KEY] [--secret SECRET] [--timeout TIMEOUT]
               [--check-trade CHECK_TRADE]

Download historical candlestick data for all available trading pairs and historical trades, @see https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md to understand related parameters

optional arguments:
  -h, --help            show this help message and exit
  -dt DTYPE, --dtype DTYPE
                        Provide which data to fetch candle|trade (default candle)
  -p PAIRS, --pairs PAIRS
                        Provide pair names to fetch (default all)
  -i INTERVAL, --interval INTERVAL
                        Set time frame interval (default 1m) only if type is candle
  --limit LIMIT         Set max results returned per request (default 1000)
  --start-at START_AT   Set start time or index according to data type (default 0)
  --parquet PARQUET     Build parquet as well (default False)
  --upload UPLOAD       Upload parquet to kaggle (default False)
  --key KEY             Binance api key
  --secret SECRET       Binance secret key
  --timeout TIMEOUT     Stop waiting for a response from Binance after (x) Seconds (default 30)
  --check-trade CHECK_TRADE
                        Check trade consistency (provide a csv filepath)
```

## Running

# Get all pairs with default interval value (1m)
Simply run `./main.py` to either download or update every single pair available:

# Get single pair with specific interval
./main.py --pairs 'ETH-USDT' --interval 1d

# Get more pairs
./main.py --pairs 'ETH-USDT, ETH-BTC, ADA-USDT' --interval 1d

```
[...]
2020-08-22 17:44:24.178846 959/970 Wrote 83000 new lines to file for ETH-USDT 
2020-08-22 17:45:13.963455 960/970 Wrote 83000 new lines to file for ETH-BTC 
2020-08-22 17:45:14.573595 961/970 Already up to date with ADA-USDT
2020-08-22 17:46:06.781870 962/970 Wrote 83000 new lines to file for ETH-BTC 
[...]
```


Once that is completed you should end up with a directory with a Parquet file for each pair, currently 970 files totaling ~12GB if --parquet arguments is set.

Add --upload if you want to upload parquet files to kaggle
