import binance_database
from binance_database import update_all
import binance_calculations
from binance_calculations import binance_timeframes, binance_conn, pull_datatable, perp_spot_basis, imm1, \
    imm_perp_basis, imm_spot_basis, binance_timeframes_rolling, old_carry, new_carry
from datetime import datetime, timedelta
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from Utilities import trace_candlestick_chart
import base64

kline_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
basis_columns = ['Annualised_Open', 'Annualised_High', 'Annualised_Low', 'Annualised_Close']


def get_table_download_link(df, filename):
    """Generates a link allowing the data in a given panda dataframe to be downloaded
    in:  dataframe
    out: href string
    """
    csv = df.to_csv(index=True)
    b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}.csv">Download csv file</a>'
    return href


# streamlit run C:/Users/ambam/PycharmProjects/Scrypt_2021/app.py
# START WEBPAGE HERE --------------------------------------------------------

st.set_page_config(layout="wide")
st.title("Scrypt Dashboard")
option = st.sidebar.selectbox("Which Dashboard?", ('Binance', 'FTX', 'BitMex'), 0)

if option == 'Binance':
    # Update data button
    update = st.sidebar.button('Update Data')
    if update:
        update_all(offset=5)

    # Parameters
    connection = binance_conn
    timeframe = st.sidebar.selectbox("Timeframe", binance_timeframes, 0, key='binance_timeframe')
    since = st.sidebar.date_input("Start Date", value=datetime(2021, 6, 1, 0, 0, 0))
    start_time = datetime.combine(since, datetime.min.time())
    crypto = st.sidebar.selectbox("Crypto", ('BTCUSDT', 'ETHUSDT'), 1)
    cols = st.columns(3)

    with cols[0]:
        st.header("Perp Data")
        # Load Perp Data
        perp = pull_datatable('securities_kline', crypto, connection, timeframe=timeframe)[kline_columns]
        perp = perp.loc[start_time:]
        # Load Funding Data
        funding = pull_datatable('securities_funding', crypto, connection, timeframe='1h').fillna(method='ffill')
        funding = funding.loc[start_time:]
        # Load Perp_Spot Basis
        perp_spot = perp_spot_basis(crypto, connection, timeframe)
        perp_spot = perp_spot.loc[start_time:]

        # Sidebar Widgets
        funding_type = st.sidebar.selectbox("Funding Type?", ('rate', 'rate_24hr', 'rate_24hr_annualised'), 2)
        window_adj = binance_timeframes_rolling[timeframe]
        lookback_window = st.sidebar.number_input('Lookback Window (Days)', min_value=1, value=14) * window_adj

        fig_0 = make_subplots(rows=4, cols=1,
                              subplot_titles=("Price", "Rolling Perp Std", "Funding", "Perp Spot Basis"))

        # fig_0.add_trace(trace_candlestick_chart(perp, 'perp'), row=1, col=1)

        fig_0.add_trace(go.Scatter(x=perp.index, y=perp['Open']),
                        row=1, col=1)

        fig_0.add_trace(go.Scatter(x=perp.index, y=perp.rolling(window=lookback_window).std()['Open']),
                        row=2, col=1)

        fig_0.add_trace(go.Scatter(x=funding.index, y=funding[funding_type]),
                        row=3, col=1)

        # fig_0.add_trace(go.Scatter(x=perp_spot.index, y=perp_spot['Open'], name='funding'),
        # row=3, col=1)

        fig_0.add_trace(go.Scatter(x=perp_spot.index, y=perp_spot['Annualised_Open']),
                        row=4, col=1)

        fig_0.update_layout(height=1200, width=600, xaxis_rangeslider_visible=False)
        st.plotly_chart(fig_0)

        # Load Data For Viewing
        st.subheader('Perp Data')
        st.write(perp)
        st.markdown(get_table_download_link(perp, crypto + '_perp_data'), unsafe_allow_html=True)
        st.subheader('Funding Data')
        st.write(funding)
        st.markdown(get_table_download_link(funding, crypto + '_funding_data'), unsafe_allow_html=True)
        st.subheader('Perp Spot Basis Data')
        st.write(perp_spot)
        st.markdown(get_table_download_link(perp_spot, crypto + '_perp_spot'), unsafe_allow_html=True)

    with cols[1]:
        st.header("Imm Data (usd mode)")
        # Load IMM Data
        imm = imm1(crypto, connection, timeframe)[kline_columns]
        imm = imm.loc[start_time:]
        # Load IMM_PERP
        imm_perp = imm_perp_basis(crypto, connection, timeframe)
        imm_perp = imm_perp.loc[start_time:]
        # Load IMM_SPOT
        imm_spot = imm_spot_basis(crypto, connection, timeframe)
        imm_spot = imm_spot.loc[start_time:]

        fig_1 = make_subplots(rows=4, cols=1,
                              subplot_titles=("IMM Price", "Rolling IMM Std", "IMM Perp Spread", "IMM PERP Basis"))

        # fig_1.add_trace(trace_candlestick_chart(imm, 'imm'), row=1, col=1)

        fig_1.add_trace(go.Scatter(x=imm.index, y=imm['Open']),
                        row=1, col=1)

        fig_1.add_trace(go.Scatter(x=imm.index, y=imm.rolling(window=lookback_window).std()['Open']),
                        row=2, col=1)

        fig_1.add_trace(go.Scatter(x=imm_perp.index, y=imm_perp['Open']),
                        row=3, col=1)

        # fig_0.add_trace(go.Scatter(x=perp_spot.index, y=perp_spot['Open'], name='funding'),
        # row=3, col=1)

        fig_1.add_trace(go.Scatter(x=imm_perp.index, y=imm_perp['Annualised_Open']),
                        row=4, col=1)

        fig_1.update_layout(height=1200, width=600, xaxis_rangeslider_visible=False)
        st.plotly_chart(fig_1)

        # Load Data For Viewing
        st.subheader('IMM Data')
        st.write(imm)
        st.markdown(get_table_download_link(imm, crypto + '_imm_data'), unsafe_allow_html=True)
        st.subheader('IMM Perp Data')
        st.write(imm_perp)
        st.markdown(get_table_download_link(imm_perp, crypto + '_imm_perp_data'), unsafe_allow_html=True)
        st.subheader('IMM Spot Data')
        st.write(imm_spot)
        st.markdown(get_table_download_link(imm_spot, crypto + '_imm_spot_data'), unsafe_allow_html=True)

    with cols[2]:
        # Sidebar Widgets
        carry_type = st.sidebar.selectbox("Carry Type?", ('carry', 'timeadjcarry',
                                                          'spotvoladjcarry', 'spreadoverspotvoladjcarry'), 3)

        st.header("Carry Stats")
        strat = st.selectbox('Strat?', ('New', 'Old'), 0)

        if strat == 'New':
            carry = new_carry(crypto, connection, timeframe, window=lookback_window, carry_type=carry_type)
            carry = carry[start_time:]
        elif strat == 'Old':
            carry = old_carry(crypto, connection, timeframe, window=lookback_window, carry_type=carry_type)
            carry = carry[start_time:]

        st.subheader('Latest Carry Metrics')
        st.write(carry.tail(1).T)

        fig_2 = make_subplots(rows=4, cols=1,
                              subplot_titles=("Carry Annualised", "Carry Mean", "Carry Std", "Carry Z"))

        fig_2.add_trace(go.Scatter(x=carry.index, y=carry['Annualised_Open']),
                        row=1, col=1)

        fig_2.add_trace(go.Scatter(x=carry.index, y=carry['rolling_mean']),
                        row=2, col=1)

        fig_2.add_trace(go.Scatter(x=carry.index, y=carry['rolling_std']),
                        row=3, col=1)

        fig_2.add_trace(go.Scatter(x=carry.index, y=carry['rolling_z']),
                        row=4, col=1)

        fig_2.update_layout(height=1600, width=600, xaxis_rangeslider_visible=False)
        st.plotly_chart(fig_2)

        st.subheader('Carry Data')
        st.write(carry)
        st.markdown(get_table_download_link(imm_perp, crypto + '_carry_data'), unsafe_allow_html=True)
