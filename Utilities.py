import pandas as pd
import sqlite3
from datetime import datetime
import streamlit as st

seconds_per_unit = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
import plotly.graph_objects as go


def df_plot(df, column):
    if column == 'all':
        return df.reset_index().plot(x='index')
    else:
        return df.reset_index().plot(x='index', y=column)


def sql_fetch(database):
    conn = sqlite3.connect(database + '_database.db')
    c = conn.cursor()
    c.execute('SELECT name from sqlite_master where type= "table"')
    print(c.fetchall())


def dt_to_unix(date):
    return int(date.timestamp() * 1000)


def unix_to_dt(int):
    return datetime.fromtimestamp(int / 1000)


def convert_to_seconds(s):
    return int(s[:-1]) * seconds_per_unit[s[-1]]


# database inputs are 'ftx' or 'binance' atm
def save_to_database(dict, database):
    if database == 'ftx':
        conn = sqlite3.connect('ftx_database.db')
        c = conn.cursor()
    elif database == 'binance':
        conn = sqlite3.connect('binance_database.db')
        c = conn.cursor()
    else:
        return 'invalid database selected'

    for key, df in dict.items(): df.to_sql(key, conn, schema=None, if_exists='replace', index=True, index_label=None,
                                           chunksize=None, dtype=None)
    conn.commit()
    return


def sql_query(name, conn):
    string = "select * from '" + name + "\' ;"
    query = pd.read_sql_query(string, conn, parse_dates=['index']).set_index('index')
    return query


def see_full_dfs():
    return pd.set_option("display.max_rows", None, "display.max_columns", None)


def _max_width_():
    max_width_str = f"max-width: 2000px;"
    st.markdown(
        f"""
    <style>
    .reportview-container .main .block-container{{
        {max_width_str}
    }}
    </style>    
    """,
        unsafe_allow_html=True,
    )


def highlight_max(x, row_index):
    return ['font-weight: bold' if v == x.loc[row_index] else ''
            for v in x]


def trace_candlestick_chart(df, name):
    return go.Candlestick(x=df.index,
                          open=df['Open'],
                          high=df['High'],
                          low=df['Low'],
                          close=df['Close'],
                          name=name,
                          )

# live.style.background_gradient(cmap=cm, axis=1, subset=df.index[-1])
