import sqlite3
from sqlite3 import Error
from datetime import timedelta, datetime
from Utilities import dt_to_unix, unix_to_dt
import pandas as pd
import numpy as np
from binance_api import b_spot, b_usd, b_coin
import time

# Connect to sqlite3 binance database
conn = sqlite3.connect(r"D:\sqlite\databases\binance_scrypt.db", check_same_thread=False)
cursor = conn.cursor()

# Initial Tables
cursor.execute("""CREATE TABLE IF NOT EXISTS securities (ticker TEXT PRIMARY KEY, exchange TEXT,"
               " underlying TEXT)""")

cursor.execute("CREATE TABLE IF NOT EXISTS securities_prices (id TEXT PRIMARY KEY, ticker TEXT, "
               "time INTEGER, "
               " datetime TIMESTAMP, Open REAL, High REAL, Low REAL, Close REAL, Volume REAL, Number_of_trades REAL,"
               " openinterest REAL,"
               " openinterestnotional REAL, top_lsp_ratio REAL, top_long_account_position REAL,"
               " top_short_account_position REAL, top_lsa_ratio REAL, top_long_account REAL,"
               " top_short_account REAL,"
               " FOREIGN KEY (ticker) REFERENCES securities(ticker))")

cursor.execute("CREATE TABLE IF NOT EXISTS securities_kline (id TEXT PRIMARY KEY, ticker TEXT, "
               " time INTEGER, "
               "datetime TIMESTAMP, Open REAL, High REAL, Low REAL, Close REAL, Volume REAL, Number_of_trades REAL, "
               "d2m TIMESTAMP,"
               "FOREIGN KEY (ticker) REFERENCES securities(ticker))")

cursor.execute("CREATE TABLE IF NOT EXISTS securities_openinterest (id TEXT PRIMARY KEY, ticker TEXT, "
               "time INTEGER, "
               " datetime TIMESTAMP,"
               " openinterest REAL,"
               " openinterestnotional REAL, top_lsp_ratio REAL, top_long_account_position REAL,"
               " top_short_account_position REAL, top_lsa_ratio REAL, top_long_account REAL,"
               " top_short_account REAL,"
               " FOREIGN KEY (ticker) REFERENCES securities(ticker))")

cursor.execute("CREATE TABLE IF NOT EXISTS securities_funding (id TEXT PRIMARY KEY, ticker TEXT,"
               " time INTEGER, datetime TIMESTAMP, rate REAL, rate_24hr REAL, rate_24hr_annualised REAL,"
               " FOREIGN KEY (ticker) REFERENCES securities(ticker))")

# Updating every day for 5 minute intervals for now
coin_mode_tickers = ['BTC/USDT']
binance_expiries = ['210924', '211231']
exchange = 'Binance'
usd_mode_tickers = ['BTCUSDT', 'ETHUSDT', 'ETHUSDT_211231', 'BTCUSDT_211231', 'ETHUSDT_210924', 'BTCUSDT_210924',
                    'ETHUSDT_210625', 'BTCUSDT_210625', 'ETHUSDT_210326', 'BTCUSDT_210326']

funding_usd_mode_tickers = ['BTCUSDT', 'ETHUSDT']

spot_tickers = ['BTCUSDT_SPOT', 'ETHUSDT_SPOT']


def update_binance_usd_mode_tickers():
    exchange = 'Binance_usd'
    for t in usd_mode_tickers:
        underlying = t[:3]
        sql = "INSERT INTO securities (exchange, underlying, ticker) VALUES (?, ?, ?) ON CONFLICT (ticker) DO UPDATE" \
              " SET ticker=ticker"
        val = (exchange, underlying, t)
        cursor.execute(sql, val)
    return conn.commit()


# Updating Tickers below
# tickers have _spot string added onto them as they duplicate with USD_Mode tickers
def update_binance_spot_tickers():
    spot_tickers = ['BTCUSDT' + '_SPOT', 'ETHUSDT' + '_SPOT']
    exchange = 'Binance_SPOT'
    for t in spot_tickers:
        underlying = t[:3]
        sql = "INSERT INTO securities (exchange, underlying, ticker) VALUES (?, ?, ?) ON CONFLICT (ticker) DO UPDATE" \
              " SET ticker=ticker"
        val = (exchange, underlying, t)
        cursor.execute(sql, val)
    return conn.commit()


def update_binance_coin_mode_tickers():
    coin_mode_tickers = ['BTCUSD_PERP', 'ETHUSD_PERP', 'ETHUSD_211231', 'BTCUSD_211231', 'ETHUSD_210924',
                         'BTCUSD_210924', 'ETHUSD_210625', 'BTCUSD_210625', 'ETHUSD_210326', 'BTCUSD_210326',
                         'BTCUSD_220325', 'ETHUSD_220325']
    exchange = 'Binance_coin'
    for t in coin_mode_tickers:
        underlying = t[:3]
        sql = "INSERT INTO securities (exchange, underlying, ticker) VALUES (?, ?, ?) ON CONFLICT (ticker) DO UPDATE" \
              " SET ticker=ticker"
        val = (exchange, underlying, t)
        cursor.execute(sql, val)
    return conn.commit()


# Updating Prices below
def update_binance_usd_mode_prices(offset=1):
    # First update candlestick data
    binance = b_usd
    start_date = dt_to_unix(binance.close_datetime - timedelta(days=offset))
    close_unix = binance.now_unix

    for t in usd_mode_tickers:
        try:
            sec_data = binance.get_all_kline(t, interval='5m', startTime=start_date, endTime=close_unix)
            time.sleep(0.05)
            data_length = len(sec_data)
        except Exception as e:
            print(e, t)
            continue
        try:
            openinterestdata = binance.get_all_openinterestdata(t, '5m', start_date)
        except Exception as e:
            print(e, t)
            openinterestdata = pd.DataFrame(0, index=np.arange(data_length), columns=['sumOpenInterest',
                                                                                      'sumOpenInterestValue'])

        try:
            toplongshortpositionratio = binance.get_toplongshortpositionratio(t, '5m', start_date)
            toplongshortpositionratio.columns = toplongshortpositionratio.columns + 'Position'
        except Exception as e:
            print(e, t)
            toplongshortpositionratio = pd.DataFrame(0, index=np.arange(data_length), columns=['longShortRatioPosition',
                                                                                               'longAccountPosition',
                                                                                               'shortAccountPosition'])

        try:
            toplongshortaccountratio = binance.get_toplongshortaccountratio(t, '5m', start_date)
        except Exception as e:
            print(e, t)
            toplongshortaccountratio = pd.DataFrame(0, index=np.arange(data_length), columns=['longShortRatio',
                                                                                              'longAccount',
                                                                                              'shortAccount'])

        data = pd.concat([sec_data, openinterestdata, toplongshortpositionratio, toplongshortaccountratio], axis=1,
                         ignore_index=True)
        data = data[data.index < close_unix].dropna()
        for index, row in data.iterrows():
            sql = "INSERT INTO securities_prices (id, ticker, time, Open, High, Low, Close, Volume," \
                  " Number_of_trades, openinterest," \
                  " openinterestnotional, top_lsp_ratio, top_long_account_position, top_short_account_position," \
                  " top_lsa_ratio, top_long_account, top_short_account, datetime) VALUES " \
                  "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE id=id"
            id = t + "|" + str(index)
            val = (id, t, int(index), float(row.Open), float(row.High), float(row.Low), float(row.Close),
                   float(row.Volume), float(row.sumOpenInterest), float(row.sumOpenInterestValue),
                   float(row.longShortRatioPosition), float(row.longAccountPosition), float(row.shortAccountPosition),
                   float(row.longShortRatio), float(row.longAccount), float(row.shortAccount), str(row.datetime))
            cursor.execute(sql, val)
            pass
    pass


def update_binance_usd_mode_kline(offset=1):
    # First update candlestick data
    binance = b_usd
    start_date = dt_to_unix(binance.close_datetime - timedelta(days=offset))
    close_unix = binance.now_unix

    for t in usd_mode_tickers:
        try:
            sec_data = binance.get_all_kline(t, interval='5m', startTime=start_date, endTime=close_unix)
        except Exception as e:
            print(e, t)
            continue

        data = sec_data[sec_data.index < close_unix].dropna()
        for index, row in data.iterrows():
            sql = "INSERT INTO securities_kline (id, ticker, time, Open, High, Low, Close, Volume," \
                  " Number_of_trades, datetime, d2m) VALUES " \
                  "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET id=id"
            id = t + "|" + str(index)
            val = (id, t, int(index), float(row.Open), float(row.High), float(row.Low), float(row.Close),
                   float(row.Volume), float(row["Number of trades"]), str(row.Datetime), str(row.d2m))
            cursor.execute(sql, val)
        print('successfully snapped' + t + ' Data')
    return conn.commit()


def update_binance_spot_kline(offset=1):
    # First update candlestick data
    binance = b_spot
    start_date = dt_to_unix(binance.close_datetime - timedelta(days=offset))
    close_unix = binance.now_unix

    for t in spot_tickers:
        try:
            t_adj = t[:t.find('_')]
            sec_data = binance.get_all_kline(t_adj, interval='5m', startTime=start_date, endTime=close_unix)
        except Exception as e:
            print(e, t)
            continue

        data = sec_data[sec_data.index < close_unix].dropna()
        for index, row in data.iterrows():
            sql = "INSERT INTO securities_kline (id, ticker, time, Open, High, Low, Close, Volume," \
                  " Number_of_trades, datetime, d2m) VALUES " \
                  "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET id=id"
            id = t + "|" + str(index)
            val = (id, t, int(index), float(row.Open), float(row.High), float(row.Low), float(row.Close),
                   float(row.Volume), float(row["Number of trades"]), str(row.Datetime), str(row.d2m))
            cursor.execute(sql, val)
        print('successfully snapped' + t + ' Data')
    return conn.commit()


def update_binance_usd_mode_funding(offset=1):
    # First update candlestick data
    binance = b_usd
    start_date = dt_to_unix(binance.close_datetime - timedelta(days=offset) - timedelta(days=1))
    close_unix = binance.now_unix

    for t in funding_usd_mode_tickers:
        try:
            sec_data = binance.get_all_funding(t, startTime=start_date, endTime=close_unix)
        except Exception as e:
            print(e, t)
            continue

        data = sec_data[sec_data.index < close_unix].dropna()
        for index, row in data.iterrows():
            sql = "INSERT INTO securities_funding (id, ticker, time, rate," \
                  " rate_24hr, rate_24hr_annualised, datetime) VALUES " \
                  "(?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET id=id"
            id = t + "|" + str(index)
            val = (id, t, int(index), float(row.fundingRate), float(row['24hr_funding']),
                   float(row['24hr_funding_annualised']), str(row.Datetime))
            cursor.execute(sql, val)
        print('successfully snapped' + t + ' Data')
    return conn.commit()


# AGGREGATE FUNCTIONS FOR DASHBOARD
def update_all(offset=1):
    update_binance_spot_kline(offset=offset)
    update_binance_usd_mode_kline(offset=offset)
    update_binance_usd_mode_funding(offset=offset)
    return
