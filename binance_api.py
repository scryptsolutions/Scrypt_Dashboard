import ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from Utilities import dt_to_unix, unix_to_dt, convert_to_seconds, save_to_database, see_full_dfs
import time
import requests
import json


# my Ip88.98.200.224
# Manual adjustments -> in __init__, have to update list_of_imms


def ticker_date_match(symbol):
    try:
        substring = symbol.split("_")[1]
    except:
        return 'NA'
    if len(substring) == 6:
        try:
            result = datetime.strptime(substring, '%y%m%d')
            return result
        except:
            return 'NA'
    elif len(substring) == 4:
        try:
            result = datetime.strptime(substring, '%m%d')
            result = datetime(datetime.today().year, result.month, result.day)
            return result
        except:
            return 'NA'
    else:
        return 'NA'


class binance:

    def __init__(self, mode='private', exchange_type='future'):
        self.api = "EQhH6s81LF57Wh2XsLj6VNA6EIdb8vhNHgLKF8bTUTYbjV5G4bdBZuduERXNHIbP"
        self.secret = "KkH07hTDUn9A392Dm5b8CKa742GjDq77K8bagsF6uPBTskTBR2cLT3q3Rh2K8WMI"
        self.mode = mode
        self.exchange_type = exchange_type
        self.exchange_name = 'Binance'
        if self.mode == 'private':
            self.exchange = ccxt.binance({'apiKey': self.api, 'secret': self.secret, 'enableRateLimit': True,
                                          'options': {
                                              'defaultType': self.exchange_type}, })
        elif self.mode == 'public':
            self.exchange = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': exchange_type, }, })
        else:
            self.exchange = 'Not valid type Selected'

        # Commonly used time objects
        self.now_datetime = datetime.today()
        self.now_unix = int(self.now_datetime.timestamp() * 1000)
        self.close_datetime = datetime.combine(date.today(), datetime.min.time())
        self.close_unix = int(self.close_datetime.timestamp() * 1000)
        self.list_of_imms = ['/USD', '-PERP', '-20190927', '-20191227', '-20200327', '-20200626', '-20200925',
                             '-20201225', '-0326', '-0625', '-0924', '-1231']
        self.imm_expiries = ['210326', '210625', '210924', '211231']
        self.timeframes = list(self.exchange.timeframes.values())

        self.base_spot_url = 'https://api.binance.com'
        self.base_usdmode_url = 'https://fapi.binance.com'
        self.base_coinmode_url = 'https://dapi.binance.com'
        self.alternative_urls = ["https://api1.binance.com", "https://api2.binance.com", "https://api3.binance.com"]
        self.timezone = 'UTC'
        self.timestamp = 'ms'
        self.close_datetime_hour = datetime.now().replace(microsecond=0, second=0, minute=0)
        self.close_unix_hour = dt_to_unix(self.close_datetime_hour)

        if self.exchange_type == 'spot':
            self.base_url = self.base_spot_url
        elif self.exchange_type == 'delivery':
            self.base_url = self.base_coinmode_url
        elif self.exchange_type == 'future':
            self.base_url = self.base_usdmode_url

    # Pulling through REST (quicker)
    def get_symbols(self):
        if self.exchange_type == 'spot':
            fetch = self.base_url + '/api/v3/exchangeInfo'
        elif self.exchange_type == 'delivery':
            fetch = self.base_url + '/dapi/v1/exchangeInfo'
        elif self.exchange_type == 'future':
            fetch = self.base_url + '/fapi/v1/exchangeInfo'
        else:
            return 'incorrect endpoint'

        r = requests.get(fetch).json()['symbols']
        return [i['symbol'] for i in r]

    def get_kline(self, symbol, interval, startTime=None, endTime=None, limit=1000):
        if self.exchange_type == 'spot':
            fetch = self.base_url + '/api/v3/klines'
        elif self.exchange_type == 'delivery':
            fetch = self.base_url + '/dapi/v1/klines'
        elif self.exchange_type == 'future':
            fetch = self.base_url + '/fapi/v1/klines'
        else:
            return 'incorrect endpoint'
        r = requests.get(fetch, params={'symbol': symbol, 'interval': interval, 'startTime': startTime,
                                        'endTime': endTime, 'limit': limit})
        d = json.dumps(r.json())
        try:
            df = pd.read_json(d)
            df.columns = ['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume',
                          'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore.']
            df['Datetime'] = df['Open time'].apply(lambda x: datetime.fromtimestamp(x / 1000))
            try:
                expiry = ticker_date_match(symbol)
                df['d2m'] = expiry - df['Datetime']
                df['time'] = df['Open time']
                return df.set_index('time')
            except Exception as e:
                df['d2m'] = 0
                df['time'] = df['Open time']
                return df.set_index('time')
        except Exception as e:
            return e

    def get_all_kline(self, symbol, interval, startTime, endTime=None, limit=1000):
        datasets = {}
        initial_df = self.get_kline(symbol, interval, startTime=startTime, endTime=None, limit=limit)
        i = initial_df.index[-1]
        datasets[i] = initial_df

        if endTime is None:
            last = self.close_unix_hour
        else:
            last = endTime
        # Loop api
        while i <= last:
            try:
                df = pd.DataFrame(self.get_kline(symbol, interval, startTime=int(i), endTime=None, limit=limit))
                if df.index[-1] == i:
                    break
                else:
                    i = df.index[-1]
                    datasets[i] = df
            except Exception as e:
                print(e)
                continue
        try:
            result = pd.concat(datasets, join='inner')
            result.index = result.index.get_level_values(1)
        except Exception as e:
            print(e)
            return initial_df
        result = result[~result.index.duplicated(keep='first')]
        return result

    def get_mark_price(self, symbol):
        if self.exchange_type == 'delivery':
            fetch = self.base_url + '/dapi/v1/premiumIndex'
        elif self.exchange_type == 'future':
            fetch = self.base_url + '/fapi/v1/premiumIndex'
        else:
            return 'incorrect endpoint'
        r = requests.get(fetch, params={'symbol': symbol})
        d = json.dumps(r.json())
        try:
            df = pd.read_json(d, orient='index')
            return df
        except Exception as e:
            return e

    def get_funding(self, symbol, startTime=None, endTime=None, limit=1000):
        if self.exchange_type == 'delivery':
            fetch = self.base_url + '/dapi/v1/fundingRate'
        elif self.exchange_type == 'future':
            fetch = self.base_url + '/fapi/v1/fundingRate'
        else:
            return 'incorrect endpoint'
        r = requests.get(fetch, params={'symbol': symbol, 'startTime': startTime,
                                        'endTime': endTime, 'limit': limit})
        d = json.dumps(r.json())
        try:
            df = pd.read_json(d)
            df['fundingTime'] = (df['fundingTime'].astype(float) / 1000).round() * 1000
            df['Datetime'] = df['fundingTime'].apply(lambda x: datetime.fromtimestamp(x / 1000))
            df['24hr_funding'] = df['fundingRate'].rolling(3).sum()
            df['24hr_funding_annualised'] = df['24hr_funding'] * 365
            return df.set_index('fundingTime')
        except Exception as e:
            return e

    def get_all_funding(self, symbol, startTime, endTime=None, limit=1000):
        datasets = {}
        initial_df = self.get_funding(symbol, startTime=startTime, endTime=None, limit=limit)
        i = initial_df.index[-1]
        datasets[i] = initial_df

        if endTime is None:
            last = self.close_unix_hour
        else:
            last = endTime

        # Loop api
        while i < self.close_unix_hour:
            try:
                df = pd.DataFrame(self.get_funding(symbol, startTime=int(i), endTime=None, limit=limit))
                if df.index[-1] == i:
                    break
                else:
                    i = df.index[-1]
                    datasets[i] = df
            except Exception as e:
                print(e)
                continue
        try:
            result = pd.concat(datasets, join='inner')
            result.index = result.index.get_level_values(1)
        except Exception as e:
            print(e)
            return initial_df
        result = result[~result.index.duplicated(keep='first')]
        return result

    def imm1_kline(self, symbol, interval, startTime=dt_to_unix(datetime(2021, 1, 1)), limit=1000):
        ticker_list = [symbol + '_' + s for s in self.imm_expiries]
        datasets = {}
        for t in ticker_list:
            expiry = dt_to_unix(ticker_date_match(t) + timedelta(hours=8))
            df = self.get_all_kline(symbol, interval, startTime, None, limit)
            df = df[df.index <= expiry]
            datasets[t] = df
        result = pd.concat(datasets, join='inner').reset_index(level=0)
        result = result[~result.index.duplicated(keep='first')]
        return result

    # contractType only for Coin_Mode: Options are ALL, CURRENT_QUARTER, NEXT_QUARTER, PERPETUAL
    def get_openinterestdata(self, symbol, interval, contractType=None, startTime=None, limit=500):
        if self.exchange_type == 'delivery':
            fetch = self.base_url + '/futures/data/openInterestHist'
        elif self.exchange_type == 'future':
            fetch = self.base_url + '/futures/data/openInterestHist'
        else:
            return 'incorrect endpoint'
        endTime = dt_to_unix(unix_to_dt(startTime) + timedelta(days=1))
        if contractType is None:
            r = requests.get(fetch, params={'symbol': symbol, 'period': interval, 'startTime': startTime,
                                            'endTime': endTime, 'limit': limit})
        else:
            r = requests.get(fetch, params={'pair': symbol, 'contractType': contractType, 'period': interval,
                                            'startTime': startTime, 'endTime': endTime, 'limit': limit})
        d = json.dumps(r.json())
        df = pd.read_json(d, convert_dates=False)
        try:
            df['Datetime'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(x / 1000))
            df = df.set_index('timestamp')
        except Exception as e:
            return e
        return df

    def get_all_openinterestdata(self, symbol, interval, startTime, contractType=None, endTime=None, limit=500):
        datasets = {}
        initial_df = self.get_openinterestdata(symbol, interval, contractType=contractType,
                                               startTime=startTime, limit=limit)
        i = initial_df.index[-1]
        datasets[i] = initial_df

        if endTime is None:
            last = self.close_unix_hour
        else:
            last = endTime
        # Loop api
        while i <= last:
            try:
                df = pd.DataFrame(self.get_openinterestdata(symbol, interval, contractType=contractType,
                                                            startTime=int(i), limit=limit))
                if df.index[-1] == i:
                    break
                else:
                    i = df.index[-1]
                    datasets[i] = df
            except Exception as e:
                print(e)
                continue
        try:
            result = pd.concat(datasets, join='inner')
            result.index = result.index.get_level_values(1)
        except Exception as e:
            print(e)
            return initial_df
        result = result[~result.index.duplicated(keep='first')]
        return result

    def get_toplongshortpositionratio(self, symbol, interval, startTime=None, limit=500):
        endTime = dt_to_unix(unix_to_dt(startTime) + timedelta(days=1))
        if self.exchange_type == 'delivery':
            fetch = self.base_url + '/futures/data/topLongShortPositionRatio'
            r = requests.get(fetch, params={'pair': symbol, 'period': interval, 'startTime': startTime,
                                            'endTime': endTime, 'limit': limit})
        elif self.exchange_type == 'future':
            fetch = self.base_url + '/futures/data/topLongShortPositionRatio'
            r = requests.get(fetch, params={'symbol': symbol, 'period': interval,
                                            'startTime': startTime, 'endTime': endTime, 'limit': limit})
        else:
            return 'incorrect endpoint'

        d = json.dumps(r.json())
        df = pd.read_json(d, convert_dates=False)
        try:
            df['Datetime'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(x / 1000))
            df = df.set_index('timestamp')
        except Exception as e:
            return e
        return df

    def get_all_toplongshortpositionratio(self, symbol, interval, startTime, endTime=None, limit=500):
        datasets = {}
        initial_df = self.get_toplongshortpositionratio(symbol, interval, startTime=startTime, limit=limit)
        i = initial_df.index[-1]
        datasets[i] = initial_df

        if endTime is None:
            last = self.close_unix_hour
        else:
            last = endTime
        # Loop api
        while i <= last:
            try:
                df = pd.DataFrame(self.get_toplongshortpositionratio(symbol, interval, startTime=int(i), limit=limit))
                if df.index[-1] == i:
                    break
                else:
                    i = df.index[-1]
                    datasets[i] = df
            except Exception as e:
                print(e)
                continue
        try:
            result = pd.concat(datasets, join='inner')
            result.index = result.index.get_level_values(1)
        except Exception as e:
            print(e)
            return initial_df
        result = result[~result.index.duplicated(keep='first')]
        return result

    def get_toplongshortaccountratio(self, symbol, interval, startTime=None, limit=500):
        endTime = dt_to_unix(unix_to_dt(startTime) + timedelta(days=1))
        if self.exchange_type == 'delivery':
            fetch = self.base_url + '/futures/data/topLongShortAccountRatio'
            r = requests.get(fetch, params={'pair': symbol, 'period': interval, 'startTime': startTime,
                                            'endTime': endTime, 'limit': limit})
        elif self.exchange_type == 'future':
            fetch = self.base_url + '/futures/data/topLongShortAccountRatio'
            r = requests.get(fetch, params={'symbol': symbol, 'period': interval,
                                            'startTime': startTime, 'endTime': endTime, 'limit': limit})
        else:
            return 'incorrect endpoint'

        d = json.dumps(r.json())
        df = pd.read_json(d, convert_dates=False)
        try:
            df['Datetime'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(x / 1000))
            df = df.set_index('timestamp')
        except Exception as e:
            return e
        return df

    def get_all_toplongshortaccountratio(self, symbol, interval, startTime, endTime=None, limit=500):
        datasets = {}
        initial_df = self.get_toplongshortaccountratio(symbol, interval, startTime=startTime, limit=limit)
        i = initial_df.index[-1]
        datasets[i] = initial_df

        if endTime is None:
            last = self.close_unix_hour
        else:
            last = endTime
        # Loop api
        while i <= last:
            try:
                df = pd.DataFrame(self.get_toplongshortaccountratio(symbol, interval, startTime=int(i), limit=limit))
                if df.index[-1] == i:
                    break
                else:
                    i = df.index[-1]
                    datasets[i] = df
            except Exception as e:
                print(e)
                continue
        try:
            result = pd.concat(datasets, join='inner')
            result.index = result.index.get_level_values(1)
        except Exception as e:
            print(e)
            return initial_df
        result = result[~result.index.duplicated(keep='first')]
        return result

    # Utility Functions
    def get_first_timestamp(self, symbol):
        interval = '1d'
        limit = None
        start = '2019-03-01 00:00:00'
        params = {'market_name': symbol}
        start_time = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
        since = int(start_time.timestamp() * 1000)
        ohlcv = pd.DataFrame(self.exchange.fetch_ohlcv(symbol, interval, since, limit, params))
        ohlcv.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
        return ohlcv['Time'][0]

    def get_last_timestamp(self, symbol):
        return self.exchange.fetchTicker(symbol)['timestamp']

    def fetch_all_tickers(self):
        return list(self.exchange.fetchTickers().keys())

    # Account Functions
    def current_positions(self):
        stable = 'USDT'
        df = pd.DataFrame(self.exchange.fetchBalance()['info']['positions'])
        df['size'] = pd.to_numeric(df['positionAmt'])
        cp = df[df['size'] != 0].dropna()[['symbol', 'size']]
        cp['side'] = np.where(cp['size'] > 0, 'buy', 'sell')
        cp['future'] = cp['symbol'].apply(
            lambda x: x[:x.find(stable)] + '/' + stable if x.endswith(stable) else x)

        # Setting up the Dataframe (general data manipulation)
        cp['Exchange'] = 'Binance'
        cp['Underlying'] = cp['future'].str[:3]
        cp['Type'] = cp['future'].apply(
            lambda x: 'PERP' if x.endswith(stable) else x[x.find("_") + 3:])
        cp['Key'] = cp['Exchange'] + cp['Underlying'] + cp[
            'Type']
        cp['size'] = pd.to_numeric(cp['size'])
        cp = cp[cp['size'] != 0]
        cp['Position'] = cp['size']
        cp['Date'] = cp['Type'].apply(lambda x: x if x.isdigit() else '0101')
        cp['Expiry Date'] = cp['Date'].apply(lambda x: datetime(self.close_datetime.year + 1,
                                                                datetime.strptime(x, '%m%d').month,
                                                                datetime.strptime(x, '%m%d').day
                                                                ) if self.close_datetime >
                                                                     datetime(self.close_datetime.year,
                                                                              datetime.strptime(x, '%m%d').month,
                                                                              datetime.strptime(x, '%m%d').day
                                                                              )
        else datetime(self.close_datetime.year,
                      datetime.strptime(x, '%m%d').month,
                      datetime.strptime(x, '%m%d').day
                      ))
        return cp

    def deposit_history(self):
        deposits = pd.DataFrame(self.exchange.fetch_deposits())
        deposits['datetime'] = pd.to_datetime(deposits['datetime'])
        return deposits

    def funding_history(self, start_time=datetime(2021, 9, 1)):
        initial_time = dt_to_unix(start_time)
        fut_list = list(self.trade_history().reset_index()['symbol'])
        fut_list = [x for x in fut_list if not any(c.isdigit() for c in x)]
        fut_list = list(dict.fromkeys(fut_list))
        markets = self.exchange.load_markets()
        dic = {}
        for fut in fut_list:
            datasets = {}
            initial_df = pd.DataFrame(self.funding_history_loop(fut, initial_time))
            i = initial_df.index[-1]
            datasets[i] = initial_df
            # Loop as api can only access 100 funding payments at one time
            while i < self.close_datetime:
                try:
                    df = pd.DataFrame(self.funding_history_loop(fut, dt_to_unix(i)))
                    i = df.index[-1]
                    datasets[i] = df
                except:
                    continue
            dic[fut] = pd.concat(datasets, join='inner')
            dic[fut].index = dic[fut].index.get_level_values(1)
        return dic

    def funding_history_loop(self, fut, start_time):
        # Setup request
        markets = self.exchange.load_markets()
        try:
            df = pd.DataFrame(self.exchange.fetch_funding_history(fut, start_time))
            df['time'] = pd.to_datetime(df['datetime']).dt.tz_localize(None)
            df = df.set_index('time')
            df = df.sort_values('time', ascending=True)
            df['cumfunding'] = df['amount'].cumsum()
            df = df[['symbol', 'datetime', 'amount', 'cumfunding']]
        except:
            return pd.DataFrame()
        return df

    def trade_history(self, start_time=datetime(2021, 9, 1)):
        try:
            markets = self.exchange.load_markets()
            since = int(start_time.timestamp() * 1000)
            # end = datetime.today()
            # end_time = int(end.timestamp() * 1000)
            list_futs = list(self.current_positions()['future'])
            trade_history = {}
            for fut in list_futs:
                try:
                    trade_history[fut] = pd.DataFrame(self.exchange.fetchMyTrades(fut, since, None))
                except:
                    continue

            df = pd.concat(trade_history).set_index('symbol')
            df['datetime'] = pd.to_datetime(df['datetime'])
            df['price'] = pd.to_numeric(df['price'])
            df['size'] = pd.to_numeric(df['amount'])
            df['longs'] = np.where(df['side'] == "buy", 1, 0) * df['size']
            df['shorts'] = np.where(df['side'] == "sell", -1, 0) * df['size']
            df['longpos*px'] = df['longs'] * df['price']
            df['shortpos*px'] = df['shorts'] * df['price']
        except:
            return pd.DataFrame()
        return df

    def trading_fees_history(self):
        df = self.trade_history().reset_index()
        result = df[['symbol', 'datetime', 'id', 'takerOrMaker', 'price', 'amount', 'cost']]
        result = pd.concat([result, pd.DataFrame(df['fee'].to_list())], axis=1)
        return result

    # Sum of last 24hr funding use offset = 3
    def last_funding_rates(self, symbol, offset):
        markets = self.exchange.load_markets()
        market = self.exchange.market(symbol)
        params = {'symbol': market['id']}

        df = pd.DataFrame(self.exchange.fapiPublic_get_fundingrate(params))
        df['fundingTime'] = (df['fundingTime'].astype(float) / 1000).round() * 1000
        df['fundingTime'] = df['fundingTime'].apply(lambda x: datetime.fromtimestamp(x / 1000))
        df = df.rename(columns={'fundingTime': "time", 'fundingRate': 'rate'})
        df['rate'] = pd.to_numeric(df['rate'])
        return df['rate'].tail(offset).sum()

    def pull_funding(self, symbol, since):
        markets = self.exchange.load_markets()
        market = self.exchange.market(symbol)
        params = {
            'symbol': market['id'],
            'startTime': since,  # ms to get funding rate from INCLUSIVE.
            # 'endTime': end,  # ms to get funding rate until INCLUSIVE.
            'limit': 1000,  # default 100, max 1000
        }
        df = pd.DataFrame(self.exchange.fapiPublic_get_fundingrate(params))
        df['fundingTime'] = (df['fundingTime'].astype(float) / 1000).round() * 1000
        df['fundingTime'] = df['fundingTime'].apply(lambda x: datetime.fromtimestamp(x / 1000))
        df = df.rename(columns={'fundingTime': "time", 'fundingRate': 'rate'})
        df['rate'] = pd.to_numeric(df['rate'])
        df['24hr_funding'] = df['rate'].rolling(3).sum()
        df['24hr_funding_annualised'] = df['24hr_funding'] * 365
        return df

    def close_price(self, symbol, offset=0):
        limit = None
        params = {'market_name': symbol}
        interval = '1h'
        start_time = self.close_datetime - timedelta(days=offset)
        since = dt_to_unix(start_time)
        df = pd.DataFrame(self.exchange.fetch_ohlcv(symbol, interval, since, limit, params))
        df.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
        df['Time'] = df['Time'].apply(lambda x: datetime.fromtimestamp(x / 1000))
        return float(df[df['Time'] == start_time]['Open'])

    def pull_data(self, symbol, interval, since):
        limit = 1000
        params = {'market_name': symbol}
        df = pd.DataFrame(self.exchange.fetch_ohlcv(symbol, interval, since, limit, params))
        df.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
        df['Time'] = df['Time'].apply(lambda x: datetime.fromtimestamp(x / 1000))
        df = df.set_index('Time').apply(pd.to_numeric, errors='coerce')
        return df

    def pull_all_data(self, symbol, interval, since):
        datasets = {}
        initial_df = self.pull_data(symbol, interval, since)
        i = initial_df.index[-1]
        datasets[i] = initial_df
        # Loop api
        while i < self.close_datetime:
            try:
                df = pd.DataFrame(self.pull_data(symbol, interval, dt_to_unix(i)))
                i = df.index[-1]
                datasets[i] = df
            except:
                continue
        try:
            result = pd.concat(datasets, join='inner')
            result.index = result.index.get_level_values(1)
        except:
            return initial_df
        return result

    def live_price(self, symbol, field):
        px = pd.DataFrame(self.exchange.fetch_ticker(symbol)).head(1)[field]
        return float(px)

    def get_futures(self, symbol):
        stable = 'USDT'
        filter = [symbol, stable]
        dict = self.exchange.fetch_tickers()
        list_of_tickers = list(dict.keys())
        filtered_list = [i for i in list_of_tickers if all(x in i for x in filter)]
        return filtered_list

    def order_book(self, symbol):
        df = pd.DataFrame(self.exchange.fetchOrderBook(symbol))
        df['bid_price'], df['bid_size'] = zip(*df['bids'])
        df['ask_price'], df['ask_size'] = zip(*df['asks'])
        df['mid_price'] = (df['bid_size'] * df['ask_price'] + df['ask_size'] * df['bid_price']) / (
                df['ask_size'] + df['bid_size'])
        df['bid_spread'] = df['mid_price'] - df['bid_price']
        df['ask_spread'] = df['ask_price'] - df['mid_price']
        df['spread'] = df['ask_price'] - df['bid_price']
        df['bid_size_cum'] = df['bid_size'].cumsum()
        df['ask_size_cum'] = df['ask_size'].cumsum()
        return df[['symbol', 'bid_size_cum', 'bid_size', 'bid_price', 'mid_price', 'ask_price', 'ask_size',
                   'ask_size_cum', 'spread']]

    def spread_crossed(self, symbol, notional):
        binance_ob = self.order_book(symbol)
        # ASK VWAP, Spread Crossed
        binance_ob['exec_ask'] = np.where(binance_ob['ask_size_cum'] < notional, binance_ob['ask_size'],
                                          notional - binance_ob['ask_size_cum'].shift(1).fillna(value=0))
        binance_ob['exec_ask_final'] = abs(binance_ob['exec_ask'].iloc[0:binance_ob['exec_ask'].lt(0).idxmax()])
        binance_ob['exec_ask_final'] = binance_ob['exec_ask_final'].fillna(value=0)
        binance_ob['exec_ask_price'] = np.where(binance_ob['exec_ask_final'] == 0, 0, binance_ob['ask_price'])
        vwap_ask = (binance_ob['exec_ask_price'] * binance_ob['exec_ask_final']).sum() / notional
        ask_spread_from_mid = vwap_ask - binance_ob['mid_price'].head(1).iloc[0]

        # BID VWAP, Spread Crossed
        binance_ob['exec_bid'] = np.where(binance_ob['bid_size_cum'] < notional, binance_ob['bid_size'],
                                          notional - binance_ob['bid_size_cum'].shift(1).fillna(value=0))
        binance_ob['exec_bid_final'] = abs(binance_ob['exec_bid'].iloc[0:binance_ob['exec_bid'].lt(0).idxmax()])
        binance_ob['exec_bid_final'] = binance_ob['exec_bid_final'].fillna(value=0)
        binance_ob['exec_bid_price'] = np.where(binance_ob['exec_bid_final'] == 0, 0, binance_ob['bid_price'])
        vwap_bid = (binance_ob['exec_bid_price'] * binance_ob['exec_bid_final']).sum() / notional
        bid_spread_from_mid = vwap_bid - binance_ob['mid_price'].head(1).iloc[0]

        data = {'bid_spread_from_mid': bid_spread_from_mid,
                'vwap_bid': vwap_bid,
                'vwap_ask': vwap_ask,
                'ask_spread_from_mid': ask_spread_from_mid}

        result = pd.DataFrame(data.items()).set_index(0)
        result = result.rename(columns={1: symbol})
        return result

    # Historical DataBase Functions
    def funding_data_all(self, perp):
        start = self.get_first_timestamp(perp)
        markets = self.exchange.load_markets()
        market = self.exchange.market(perp)
        response = pd.DataFrame(self.exchange.fapiPublic_get_fundingrate({
            'symbol': market['id'],
            'startTime': start,  # ms to get funding rate from INCLUSIVE.
            # 'endTime': end,  # ms to get funding rate until INCLUSIVE.
            'limit': 1000,  # default 100, max 1000
        }))
        response['fundingTime'] = (response['fundingTime'].astype(float) / 1000).round() * 1000
        df = response.rename(columns={'fundingTime': "time"})

        i = int(df['time'].iloc[-1])
        # Create dictionary of the multiple Binance calls (api)
        datasets = {str(i): df}
        while i < self.close_unix:
            try:
                new_timestamp_unix = i + 3600 * 1000
                new_timestamp_str = str(new_timestamp_unix)

                response = pd.DataFrame(self.exchange.fapiPublic_get_fundingrate({
                    'symbol': market['id'],
                    'startTime': new_timestamp_unix,  # ms to get funding rate from INCLUSIVE.
                    # 'endTime': end,  # ms to get funding rate until INCLUSIVE.
                    'limit': 1000,  # default 100, max 1000
                }))

                response['fundingTime'] = (response['fundingTime'].astype(float) / 1000).round() * 1000
                response = response.rename(columns={'fundingTime': "time"})
                datasets[new_timestamp_str] = response
                i = int(datasets[new_timestamp_str]['time'].tail(1).iloc[0])
                time.sleep(0.05)
            except:
                break

        final = pd.concat(datasets, ignore_index=True)
        final = final.rename(columns={'fundingRate': 'rate'})
        final['time'] = pd.to_numeric(final['time']).apply(lambda x: datetime.fromtimestamp(x / 1000))
        final = final.sort_values(by="time").set_index('time')
        final['rate'] = final['rate'].apply(pd.to_numeric, errors='coerce')
        final = final[~final.index.duplicated(keep='first')]
        final['hourlyrate'] = final['rate'] / 8
        idx = pd.period_range(min(final.index), max(final.index), freq='1h').to_timestamp()
        final = final.reindex(idx)
        final['hourlyrate'] = final['hourlyrate'].fillna(method='bfill')
        final['annualised'] = final['hourlyrate'] * 24 * 365
        return final

    def historical_data_all(self, symbol, interval):
        last_timestamp = self.now_datetime
        results = {}
        ticker_list = [symbol + s for s in self.list_of_imms]
        dict = {}
        for t in ticker_list:
            try:
                since = self.get_first_timestamp(t)
                dict[t] = self.pull_data(t, interval, since).reset_index()
                i = dict[t]['Time'].tail(1).iloc[0]
                datasets = {}
                datasets[str(i)] = dict[t]
                try:
                    while i < last_timestamp:
                        new_ts = str((i + timedelta(seconds=convert_to_seconds(interval))))
                        new_start = datetime.strptime(new_ts, '%Y-%m-%d %H:%M:%S')
                        since = int(new_start.timestamp() * 1000)
                        try:
                            datasets[new_ts] = self.pull_data(t, interval, since).reset_index()
                            i = datasets[new_ts]['Time'].tail(1).iloc[0]
                        except:
                            break

                    # Combine datasets
                    results[t] = pd.concat(datasets, ignore_index=True).set_index('Time')
                    results[t] = results[t].apply(pd.to_numeric, errors='coerce')
                    # Check full time series has no duplicated values and remove keeping first value
                    results[t] = results[t][~results[t].index.duplicated(keep='first')]
                    # Check full time series and interpolate missing values
                    idx = pd.period_range(min(results[t].index), max(results[t].index), freq=interval).to_timestamp()
                    results[t] = results[t].reindex(idx).interpolate().dropna()
                except:
                    results[t] = dict[t].set_index('Time')
                    results[t] = results[t].apply(pd.to_numeric, errors='coerce')
                    results[t] = results[t][~results[t].index.duplicated(keep='first')]
                    # Check full time series and interpolate missing values
                    idx = pd.period_range(min(results[t].index), max(results[t].index), freq=interval).to_timestamp()
                    results[t] = results[t].reindex(idx).interpolate().dropna()
            except:
                continue
        # Calculate days to expiry for future contracts
        for t in ticker_list:
            try:
                if ticker_date_match(t) == 'NA':
                    results[t]['d2m'] = 0
                else:
                    expiry = ticker_date_match(t)
                    results[t]['d2m'] = (expiry - results[t].index) / np.timedelta64(1, 'D')
            except:
                continue
        # Calculate Spread to Perp and Spot Data
        perp_str = symbol + '-PERP'
        spot_str = symbol + '/USD'
        perp = results[perp_str]
        spot = results[spot_str]
        for t in ticker_list:
            try:
                t_to_perp = (results[t] - perp).dropna()
                t_to_perp['Spread'] = ((t_to_perp['Open'] / perp['Open']) * (365 / t_to_perp['d2m'])).replace(np.inf, 0)
                results[t + '_to_' + perp_str] = t_to_perp

                t_to_spot = (results[t] - spot).dropna()
                t_to_spot['Spread'] = ((t_to_spot['Open'] / spot['Open']) * (365 / t_to_spot['d2m'])).replace(np.inf, 0)
                results[t + '_to_' + spot_str] = t_to_spot
            except:
                continue

        # Compute % change and Price Change for each period
        for r in results:
            results[r]['px_chg'] = results[r]['Close'] - results[r]['Open']
            results[r]['pct_chg'] = ((results[r]['Close'] - results[r]['Open']) / results[r]['Open']) * 100
        return results

    def imm_1(self, symbol, dict):
        global new_ts
        ticker_list = [symbol + s for s in self.list_of_imms][2:]
        dict_subset = {key: value for key, value in dict.items() if key in ticker_list}
        imm1 = {}
        for i, (key, df) in enumerate(dict_subset.items()):
            key_expiry = ticker_date_match(key)
            if i == 0:
                imm1[key] = df.loc[:key_expiry]
            else:
                if df.loc[new_ts:key_expiry].empty:
                    imm1[key] = df.loc[new_ts:]
                else:
                    imm1[key] = df.loc[new_ts:key_expiry]
            new_ts = key_expiry + timedelta(hours=1)
        result = pd.concat(imm1).reset_index(level=0, drop=True)
        return result

    def imm_2(self, symbol, dict):
        global new_ts, key_expiry
        ticker_list = [symbol + s for s in self.list_of_imms][2:]
        dict_subset = {key: value for key, value in dict.items() if key in ticker_list}
        imm2 = {}
        for i, (key, df) in enumerate(dict_subset.items()):
            if i == 0:
                key_expiry = ticker_date_match(key)
            elif i == 1:
                imm2[key] = df.loc[:key_expiry]
                new_ts = key_expiry + timedelta(hours=1)
                key_expiry = ticker_date_match(key)
            else:
                if df.loc[new_ts:key_expiry].empty:
                    imm2[key] = df.loc[new_ts:]
                    new_ts = key_expiry + timedelta(hours=1)
                    key_expiry = ticker_date_match(key)
                else:
                    imm2[key] = df.loc[new_ts:key_expiry]
                    new_ts = key_expiry + timedelta(hours=1)
                    key_expiry = ticker_date_match(key)
        result = pd.concat(imm2).reset_index(level=0, drop=True)
        return result

    def full_dict_data(self, crypto, interval):
        dict = self.historical_data_all(crypto, interval)
        perp = dict[crypto + '-PERP']
        imm1 = self.imm_1(crypto, dict)
        imm1_to_perp = (imm1 - perp).dropna()
        imm1_to_perp['Spread'] = imm1_to_perp['Open'] / perp['Open'] * (365 / imm1_to_perp['d2m']).replace(np.inf, 0)
        imm1_to_perp['pct_chg'] = (imm1_to_perp['px_chg'] / imm1_to_perp['Open']) * 100

        funding = self.funding_data_all(perp)

        result = {**dict, crypto + '_imm1': imm1, crypto + '_imm1_to_perp': imm1_to_perp,
                  crypto + '_funding': funding}
        return result

    # Dashboard Functions
    def all_positions(self):
        cp = self.current_positions()
        th = self.trade_history().reset_index().sort_values('datetime', ascending=False)
        summary = cp.set_index('future')
        funding = self.funding_history()
        today = self.close_datetime
        # Loop through each future for FTX
        loop = list(th['symbol'].drop_duplicates())
        for fut in loop:
            # Average Entry Px
            filter = pd.DataFrame(th[th['symbol'] == fut])
            total_longs = filter['longs'].sum()
            total_shorts = filter['shorts'].sum()
            net_pos = total_longs + total_shorts
            if total_longs == 0:
                avglongprice = 0
            else:
                avglongprice = filter['longpos*px'].sum() / total_longs

            if total_shorts == 0:
                avgshortprice = 0
            else:
                avgshortprice = filter['shortpos*px'].sum() / total_shorts

            # Assign to summary dataframe
            summary.loc[fut, 'total_longs'] = total_longs
            summary.loc[fut, 'total_shorts'] = total_shorts
            summary.loc[fut, 'net_pos'] = net_pos
            summary.loc[fut, 'avglongprice'] = avglongprice
            summary.loc[fut, 'avgshortprice'] = avgshortprice
            summary.loc[fut, 'realised_pnl'] = (avgshortprice - avglongprice) * min(abs(total_longs),
                                                                                    abs(total_shorts))
            summary.loc[fut, 'AvgEntryPx'] = np.where(net_pos >= 0, avglongprice, avgshortprice)

            # Close Px
            try:
                summary.loc[fut, 'ClosePx'] = self.close_price(fut, 0)
            except:
                summary.loc[fut, 'ClosePx'] = 0
            # Funding Today
            try:
                summary.loc[fut, 'Funding_Today'] = float(funding[fut]['cumfunding'].tail(1)) - \
                                                    float(funding[fut]['cumfunding'].loc[today])
            except:
                summary.loc[fut, 'Funding_Today'] = 0

            # Funding Accrued
            try:
                summary.loc[fut, 'Funding_Accrued'] = float(
                    funding[fut].loc[:self.close_datetime]['cumfunding'].tail(1))
            except:
                summary.loc[fut, 'Funding_Accrued'] = 0

        return summary

    def spreads(self, all_positions):
        raw = all_positions.reset_index().dropna()
        imm_only = raw[raw['Type'] != 'PERP']

        df = imm_only[['Exchange', 'Underlying', 'Type', 'Position', 'future']]. \
            rename(columns={"Type": "Leg_1", "Position": "Leg_1_Position"})
        df['Leg_1_Symbol'] = df['future']
        df['Leg_2'] = 'PERP'
        df['Leg_2_Position'] = -df['Leg_1_Position']
        df['Leg_2_Symbol'] = df['Underlying'] + '/USDT'
        # Entry, Close and Live Px's

        df['AvgEntryPx'] = df['Leg_1_Symbol'].apply(lambda x: float(raw[raw['future'] == x]['AvgEntryPx'])) - \
                           df['Leg_2_Symbol'].apply(lambda x: float(raw[raw['future'] == x]['AvgEntryPx']))

        df['ClosePx'] = df['Leg_1_Symbol'].apply(lambda x: float(raw[raw['future'] == x]['ClosePx'])) - \
                        df['Leg_2_Symbol'].apply(lambda x: float(raw[raw['future'] == x]['ClosePx']))

        df['Funding_Today'] = df['Leg_2_Symbol'].apply(lambda x: float(raw[raw['future'] == x]['Funding_Today']) /
                                                                 float(raw[raw['future'] == x]['Position'])) * df[
                                  'Leg_2_Position']

        df['Funding_Accrued'] = df['Leg_2_Symbol'].apply(lambda x: float(raw[raw['future'] == x]['Funding_Accrued']) /
                                                                   float(raw[raw['future'] == x]['Position'])) * df[
                                    'Leg_2_Position']

        # Summary Statistics
        df['last_funding'] = df['Leg_2_Symbol'].apply(lambda x: self.last_funding_rates(x, 1))
        df['last_funding_24hr'] = df['Leg_2_Symbol'].apply(lambda x: self.last_funding_rates(x, 3))
        df['funding_annualised_24hr'] = df['last_funding_24hr'] * 365

        # Next Funding Calculation + Carry Calc             # RESEARCH AND AMEND
        # df['NextFundingRate'] = df['LastFundingRate']
        # df['NextFundingRateAnnualised'] = df['LastFundingAnnualised']
        # Historical Metrics (Avg + StD + Vol + Z-Score)
        carrystats = {}
        for a, b in zip(df['Leg_1_Symbol'], df['Leg_2_Symbol']):
            carrystats[a + b] = self.spread_metrics(a, b, 60, '4h')
        carrytable = pd.concat(carrystats, ignore_index=True)[
            ['carry', 'carry_mean', 'carry_std', 'carry_chg', 'carry_vol', 'z_score']]
        result = pd.concat([df.reset_index(), carrytable], axis=1)
        result['Key'] = result['Leg_1_Symbol'] + ':' + result['Leg_2_Symbol']
        return result

    # offset is in days pls, interval 4hrs default, 6 pts per day
    def spread_metrics(self, leg_1, leg_2, offset, interval):
        # Load IMM Leg DATA and assign d2m column
        since = dt_to_unix(self.close_datetime - timedelta(days=offset))
        imm = self.pull_data(leg_1, interval, since)
        imm_expiry = ticker_date_match(leg_1)
        imm['d2m'] = (imm_expiry - imm.index) / np.timedelta64(1, 'D')

        # Load Perp Data
        perp = self.pull_data(leg_2, interval, since)
        perp['d2m'] = 0

        # Calculate spread
        spread = (imm - perp).dropna()
        spread_rate = (spread / perp).drop(columns='d2m')
        spread_annualised = spread_rate['Open'] * 365 * (1 / imm['d2m'])

        # Funding time series
        hist_funding = self.funding_data_all(leg_2)[unix_to_dt(since):]
        hist_funding['24hr_rate'] = hist_funding['hourlyrate'].rolling(window=24).sum()
        hist_funding['24hr_rate_annualised'] = hist_funding['24hr_rate'] * 365

        # Calculate carry and carry statistics
        carry = pd.concat([spread_annualised, hist_funding], axis=1, join='inner')
        carry['carry'] = carry[0] - hist_funding['24hr_rate_annualised']  # spread minus funding
        carry = carry.fillna(method='bfill')
        carry['carry_mean'] = carry['carry'].rolling(window=offset).mean()
        carry['carry_std'] = carry['carry'].rolling(window=offset).std()
        carry['carry_chg'] = carry['carry'].diff(1)
        carry = carry.fillna(method='bfill')
        carry['carry_vol'] = carry['carry_chg'].rolling(window=offset).std()
        carry['z_score'] = (carry['carry'] - carry['carry_mean']) / carry['carry_std']
        return carry.tail(1)

    def outrights(self, raw, spreads):
        outrights = pd.DataFrame(raw[
                                     ['side', 'Exchange', 'Underlying', 'Type', 'realised_pnl', 'AvgEntryPx', 'ClosePx',
                                      'Funding_Today',
                                      'Funding_Accrued']])
        outrights['Position'] = raw['Position'] - spreads.set_index('Leg_2_Symbol')['Leg_2_Position']
        outrights['Funding_Today'] = -outrights['Funding_Today']
        outrights['Funding_Accrued'] = -outrights['Funding_Accrued']
        return outrights

    # SQL Database Functions

    def current_positions_24hr(self):
        stable = 'USDT'
        df = pd.DataFrame(self.exchange.fetchBalance()['info']['positions'])
        df['size'] = pd.to_numeric(df['positionAmt'])
        cp = df[df['size'] != 0].dropna()[['symbol', 'size']]
        cp['side'] = np.where(cp['size'] > 0, 'buy', 'sell')
        cp['future'] = cp['symbol'].apply(
            lambda x: x[:x.find(stable)] + '/' + stable if x.endswith(stable) else x)

        # Setting up the Dataframe (general data manipulation)
        cp['Exchange'] = 'Binance'
        cp['Underlying'] = cp['future'].str[:3]
        cp['Type'] = cp['future'].apply(
            lambda x: 'PERP' if x.endswith(stable) else x[x.find("_") + 3:])
        cp['Key'] = cp['Exchange'] + cp['Underlying'] + cp[
            'Type']
        cp['size'] = pd.to_numeric(cp['size'])
        cp = cp[cp['size'] != 0]
        cp['Position'] = cp['size']
        cp['Date'] = cp['Type'].apply(lambda x: x if x.isdigit() else '0101')
        cp['Expiry Date'] = cp['Date'].apply(lambda x: datetime(self.close_datetime.year + 1,
                                                                datetime.strptime(x, '%m%d').month,
                                                                datetime.strptime(x, '%m%d').day
                                                                ) if self.close_datetime >
                                                                     datetime(self.close_datetime.year,
                                                                              datetime.strptime(x, '%m%d').month,
                                                                              datetime.strptime(x, '%m%d').day
                                                                              )
        else datetime(self.close_datetime.year,
                      datetime.strptime(x, '%m%d').month,
                      datetime.strptime(x, '%m%d').day
                      ))
        return cp

    def live_candlestick(self, symbol, since):

        pass

    def pull_data_unix(self, symbol, interval, since):
        limit = None
        params = {'market_name': symbol}
        df = pd.DataFrame(self.exchange.fetch_ohlcv(symbol, interval, since, limit, params))
        df.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
        return df.set_index('Time')

    def pull_all_data_unix(self, symbol, interval, since):
        datasets = {}
        initial_df = self.pull_data_unix(symbol, interval, since)
        i = initial_df.index[-1]
        datasets[i] = initial_df
        # Loop api
        while i < self.close_unix:
            try:
                df = pd.DataFrame(self.pull_data_unix(symbol, interval, int(i)))
                i = df.index[-1]
                datasets[i] = df
            except:
                continue
        try:
            result = pd.concat(datasets, join='inner')
            result.index = result.index.get_level_values(1)
        except:
            return initial_df
        return result

    def pull_funding_unix(self, symbol, since):
        markets = self.exchange.load_markets()
        market = self.exchange.market(symbol)
        params = {
            'symbol': market['id'],
            'startTime': int(since - 8.64e+7),  # ms to get funding rate from INCLUSIVE.
            # 'endTime': end,  # ms to get funding rate until INCLUSIVE.
            'limit': 1000,  # default 100, max 1000
        }
        df = pd.DataFrame(self.exchange.fapiPublic_get_fundingrate(params))
        df['datetime'] = (df['fundingTime'].astype(float) / 1000).round() * 1000
        df['datetime'] = df['datetime'].apply(lambda x: datetime.fromtimestamp(x / 1000))
        df = df.rename(columns={'fundingTime': "time", 'fundingRate': 'rate'})
        df['rate'] = pd.to_numeric(df['rate'])
        df['24hr_funding'] = df['rate'].rolling(3).sum()
        df['24hr_funding_annualised'] = df['24hr_funding'] * 365
        return df.set_index('time').dropna()

    # Use for example BTCUSDT not BTC/USDT
    def fetch_open_interest(self, symbol):
        base_url = 'https://fapi.binance.com'
        link = "/fapi/v1/openInterest"
        fetch = base_url + link
        r = requests.get(fetch, params={'symbol': 'BTCUSDT'})
        return r.content

    def fetch_open_interest_history(self, symbol, period, startTime):
        base_url = 'https://fapi.binance.com'
        link = "/futures/data/openInterestHist"
        fetch = base_url + link
        endTime = dt_to_unix(unix_to_dt(startTime) + timedelta(days=1))
        r = requests.get(fetch, params={'symbol': symbol, 'period': period, 'limit': 500, 'startTime': startTime,
                                        'endTime': endTime})
        d = json.dumps(r.json())
        df = pd.read_json(d, convert_dates=False)
        df = df.set_index('timestamp')
        return df

    # fetch_all_open_interest_history('BTCUSDT', '5m', dt_to_unix(binance.close_datetime-timedelta(days=5)))
    def fetch_all_open_interest_history(self, symbol, period, startTime):
        datasets = {}
        initial_df = self.fetch_open_interest_history(symbol, period, startTime)
        i = initial_df.index[-1]
        datasets[i] = initial_df
        # Loop api
        while i < self.close_unix:
            try:
                df = pd.DataFrame(self.fetch_open_interest_history(symbol, period, int(i)))
                i = df.index[-1]
                datasets[i] = df
            except:
                continue
        try:
            result = pd.concat(datasets, join='inner')
            result.index = result.index.get_level_values(1)
        except:
            return initial_df
        result = result[~result.index.duplicated(keep='first')]
        return result

    def fetch_toplongshortpositionratio(self, symbol, period, startTime):
        base_url = 'https://fapi.binance.com'
        link = "/futures/data/topLongShortPositionRatio"
        fetch = base_url + link
        endTime = dt_to_unix(unix_to_dt(startTime) + timedelta(days=1))
        r = requests.get(fetch, params={'symbol': symbol, 'period': period, 'limit': 500, 'startTime': startTime,
                                        'endTime': endTime})
        d = json.dumps(r.json())
        df = pd.read_json(d, convert_dates=False)
        df = df.set_index('timestamp')
        return df

    def fetch_all_toplongshortpositionratio(self, symbol, period, startTime):
        datasets = {}
        initial_df = self.fetch_toplongshortpositionratio(symbol, period, startTime)
        i = initial_df.index[-1]
        datasets[i] = initial_df
        # Loop api
        while i < self.close_unix:
            try:
                df = pd.DataFrame(self.fetch_toplongshortpositionratio(symbol, period, int(i)))
                i = df.index[-1]
                datasets[i] = df
            except:
                continue
        try:
            result = pd.concat(datasets, join='inner')
            result.index = result.index.get_level_values(1)
        except:
            return initial_df
        result = result[~result.index.duplicated(keep='first')]
        return result

    def fetch_toplongshortaccountratio(self, symbol, period, startTime):
        base_url = 'https://fapi.binance.com'
        link = "/futures/data/topLongShortAccountRatio"
        fetch = base_url + link
        endTime = dt_to_unix(unix_to_dt(startTime) + timedelta(days=1))
        r = requests.get(fetch, params={'symbol': symbol, 'period': period, 'limit': 500, 'startTime': startTime,
                                        'endTime': endTime})
        d = json.dumps(r.json())
        df = pd.read_json(d, convert_dates=False)
        df = df.set_index('timestamp')
        return df

    def fetch_all_toplongshortaccountratio(self, symbol, period, startTime):
        datasets = {}
        initial_df = self.fetch_toplongshortaccountratio(symbol, period, startTime)
        i = initial_df.index[-1]
        datasets[i] = initial_df
        # Loop api
        while i < self.close_unix:
            try:
                df = pd.DataFrame(self.fetch_toplongshortaccountratio(symbol, period, int(i)))
                i = df.index[-1]
                datasets[i] = df
            except:
                continue
        try:
            result = pd.concat(datasets, join='inner')
            result.index = result.index.get_level_values(1)
        except:
            return initial_df
        result = result[~result.index.duplicated(keep='first')]
        return result

    def ticker_data(self, symbol, interval):
        pass


b_spot = binance('private', 'spot')
b_usd = binance('private', 'future')
b_coin = binance('private', 'delivery')
