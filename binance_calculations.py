import binance_database
from binance_api import b_spot, b_usd, b_coin, ticker_date_match
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from Utilities import dt_to_unix, unix_to_dt

binance_conn = binance_database.conn
cursor = binance_conn.cursor()
binance_timeframes = ['5min', '15min', '30min', '1H', '2H', '4H', '6H', '12H', '1D']
binance_timeframes_rolling = {'5min': 288, '15min': 96, '30min': 48, '1H': 24, '2H': 12, '4H': 6, '6H': 4, '12H': 2,
                              '1D': 1}
list_of_strats = ['carry', 'timeadjcarry', 'spotvoladjcarry', 'spreadoverspotvoladjcarry']
kline_columns = ['Open', 'High', 'Low', 'Close', 'd2m']

# pull_datatable('securities_kline', 'BTCUSDT', binance_conn)
def pull_datatable(table, ticker, connection, timeframe=None):
    string = F"SELECT * FROM {table} WHERE ticker = '{ticker}'"
    df = pd.read_sql(string, connection, parse_dates=["datetime"])
    if timeframe is None:
        return df
    else:
        df = df.set_index('datetime', drop=False)
        df = df.groupby(pd.Grouper(freq=timeframe)).first()
        df = df.drop_duplicates().fillna(method='ffill')
        return df


def imm1(symbol, connection, timeframe):
    ticker_list = [symbol + '_' + s for s in b_usd.imm_expiries]
    datasets = {}
    for t in ticker_list:
        df = pull_datatable('securities_kline', t, connection, timeframe=timeframe)
        df['d2m'] = df['d2m'] / np.timedelta64(1, 'D')
        datasets[t] = df[df['d2m'] > 0]

    result = pd.concat(datasets, join='inner').reset_index(level=0)
    result = result[~result.index.duplicated(keep='first')]
    result = result[result['d2m'] > 2]
    return result


def imm_perp_basis(symbol, connection, timeframe, imm_type='imm1'):
    if imm_type == 'imm1':
        imm = imm1(symbol, connection, timeframe)
    else:
        return 'unavailable imm_type'

    perp = pull_datatable('securities_kline', symbol, connection, timeframe)

    # Calculate Spread
    selected_columns = ['Open', 'High', 'Low', 'Close', 'd2m']
    spread = (imm[selected_columns] - perp[selected_columns]).dropna()
    spread['Annualised_Open'] = spread['Open'] / perp['Open'] * (365 / spread['d2m'])
    spread['Annualised_High'] = spread['High'] / perp['High'] * (365 / spread['d2m'])
    spread['Annualised_Low'] = spread['Low'] / perp['Low'] * (365 / spread['d2m'])
    spread['Annualised_Close'] = spread['Close'] / perp['Close'] * (365 / spread['d2m'])
    return spread


def imm_spot_basis(symbol, connection, timeframe, imm_type='imm1'):
    if imm_type == 'imm1':
        imm = imm1(symbol, connection, timeframe)
    else:
        return 'unavailable imm_type'

    spot = pull_datatable('securities_kline', symbol + '_SPOT', connection, timeframe)

    # Calculate Spread
    selected_columns = ['Open', 'High', 'Low', 'Close', 'd2m']
    spread = (imm[selected_columns] - spot[selected_columns]).dropna()
    spread['Annualised_Open'] = spread['Open'] / spot['Open'] * (365 / spread['d2m'])
    spread['Annualised_High'] = spread['High'] / spot['High'] * (365 / spread['d2m'])
    spread['Annualised_Low'] = spread['Low'] / spot['Low'] * (365 / spread['d2m'])
    spread['Annualised_Close'] = spread['Close'] / spot['Close'] * (365 / spread['d2m'])
    return spread


def perp_spot_basis(symbol, connection, timeframe, binance_mode='usd'):
    if binance_mode == 'usd':
        perp = pull_datatable('securities_kline', symbol, connection, timeframe)
    else:
        return 'unavailable binance_mode'

    spot = pull_datatable('securities_kline', symbol + '_SPOT', connection, timeframe)

    # Calculate Spread
    selected_columns = ['Open', 'High', 'Low', 'Close', 'd2m']
    spread = (perp[selected_columns] - spot[selected_columns]).dropna()
    spread['Annualised_Open'] = spread['Open'] / spot['Open'] * 365
    spread['Annualised_High'] = spread['High'] / spot['High'] * 365
    spread['Annualised_Low'] = spread['Low'] / spot['Low'] * 365
    spread['Annualised_Close'] = spread['Close'] / spot['Close'] * 365
    return spread


x = 24 * 14 * 12


# Defined as SPREAD - FUNDING
def old_carry(symbol, connection, timeframe, window=x, carry_type='carry'):
    imm_spread = imm_perp_basis(symbol, connection, timeframe)
    funding = pull_datatable('securities_funding', symbol, connection).set_index('datetime')

    combined = pd.concat([imm_spread, funding], axis=1).fillna(method='ffill').dropna()
    selected_columns = ['Annualised_Open', 'Annualised_High', 'Annualised_Low', 'Annualised_Close']
    # Calculate Normal Carry
    carry = pd.DataFrame()
    carry['d2m'] = combined['d2m']
    for col in selected_columns:
        carry[col] = combined[col] - combined['rate_24hr_annualised']

    carry = carry.interpolate()

    if carry_type == 'carry':
        df = carry

    elif carry_type == 'timeadjcarry':
        for col in selected_columns:
            carry[col] = carry[col] / (np.sqrt(carry['d2m'] / 90))
        df = carry

    elif carry_type == 'spotvoladjcarry':
        spot = pull_datatable('securities_kline', symbol + '_SPOT', connection).set_index('datetime')[
            ['Open', 'High', 'Low', 'Close']]
        spot['Open_pct_chg'] = spot['Open'].pct_change()
        spot['Open_pct_chg_std'] = spot['Open_pct_chg'].rolling(window=window).std()
        for col in selected_columns:
            carry[col] = carry[col] / (np.sqrt(carry['d2m'] / 90))
            carry[col] = carry[col] / spot['Open_pct_chg_std']
        df = carry.interpolate()

    elif carry_type == 'spreadoverspotvoladjcarry':
        imm_spot = imm_spot_basis(symbol, connection, timeframe)
        imm_spot['Open_pct_chg'] = imm_spot['Annualised_Open'].pct_change()
        imm_spot['Open_pct_chg_std'] = imm_spot['Open_pct_chg'].rolling(window=window).std()
        for col in selected_columns:
            carry[col] = carry[col] / (np.sqrt(carry['d2m'] / 90))
            carry[col] = carry[col] / imm_spot['Open_pct_chg_std']
        df = carry.interpolate()
    else:
        return 'type not correct'

    df['rolling_mean'] = df['Annualised_Open'].rolling(window=window).mean()
    df['rolling_std'] = df['Annualised_Open'].rolling(window=window).std()
    df['rolling_z'] = (df['Annualised_Open'] - df['rolling_mean'])/df['rolling_std']
    return df


def new_carry(symbol, connection, timeframe, window=x, carry_type='carry'):
    imm_spread = imm_perp_basis(symbol, connection, timeframe)

    perp_spot = perp_spot_basis(symbol, connection, timeframe)

    selected_columns = ['Annualised_Open', 'Annualised_High', 'Annualised_Low', 'Annualised_Close']
    # Calculate Normal Carry
    carry = pd.DataFrame()
    carry['d2m'] = imm_spread['d2m']
    for col in selected_columns:
        carry[col] = imm_spread[col] - perp_spot[col]

    carry = carry.interpolate()

    if carry_type == 'carry':
        df = carry

    elif carry_type == 'timeadjcarry':
        for col in selected_columns:
            carry[col] = carry[col] / (np.sqrt(carry['d2m'] / 90))
        df = carry

    elif carry_type == 'spotvoladjcarry':
        spot = pull_datatable('securities_kline', symbol + '_SPOT', connection,
                              timeframe=timeframe).set_index('datetime')[['Open', 'High', 'Low', 'Close']]

        spot['Open_pct_chg'] = spot['Open'].pct_change()
        spot['Open_pct_chg_std'] = spot['Open_pct_chg'].rolling(window=window).std()

        # calc = pd.concat([carry[selected_columns], spot['Open_pct_chg_std']], axis=1, join='inner')

        for col in selected_columns:
            carry[col] = carry[col] / (np.sqrt(carry['d2m'] / 90))
            carry[col] = carry[col] / spot['Open_pct_chg_std'].dropna()
        df = carry.interpolate()

    elif carry_type == 'spreadoverspotvoladjcarry':
        imm_spot = imm_spot_basis(symbol, connection, timeframe)
        imm_spot['Open_pct_chg'] = imm_spot['Annualised_Open'].pct_change()
        imm_spot['Open_pct_chg_std'] = imm_spot['Open_pct_chg'].rolling(window=window).std()
        for col in selected_columns:
            carry[col] = carry[col] / (np.sqrt(carry['d2m'] / 90))
            carry[col] = carry[col] / imm_spot['Open_pct_chg_std']
        df = carry.interpolate()
    else:
        return 'type not correct'

    df['rolling_mean'] = df['Annualised_Open'].rolling(window=window).mean()
    df['rolling_std'] = df['Annualised_Open'].rolling(window=window).std()
    df['rolling_z'] = (df['Annualised_Open'] - df['rolling_mean'])/df['rolling_std']

    df = df.dropna()
    imm_spread.columns = ['imm_spread_' + i for i in imm_spread.columns]
    perp_spot.columns = ['perp_spot_' + i for i in perp_spot.columns]

    result = pd.concat([df, imm_spread, perp_spot], axis=1)

    return result




#carry.isnull().sum()