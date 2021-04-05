# frontend/main.py

import streamlit as st
from aiohttp import ClientSession
import asyncio
import time
import plotly.graph_objects as go
import pandas as pd

################################### VARS & FUNCTIONS
url = "http://api:8080/{}"

color_short = 'rgb(219, 90, 68)'
color_long = 'rgb(108, 156, 12)'
color_price = 'rgb(113, 124, 145)'

# ----------------------------------------------
def plot_history_and_trades(df_historical, df_emitter_trades):
    fig = go.Figure()
        
    df_long = df_emitter_trades.loc[df_emitter_trades['trade_type'] == 'Kauf'].copy()
    df_short = df_emitter_trades.loc[df_emitter_trades['trade_type'] == 'Verkauf'].copy()
    
    fig.add_trace(go.Scatter(
        x=df_historical['date'],
        y=df_historical['close'],
        marker_color=color_price,
        line_width=1,
        opacity=1,
        name='Daily Close'))
    
    fig.add_trace(go.Scatter(
        x=df_historical['date'],
        y=df_historical['high'],
        marker_color=color_price,
        line_width=1,
        opacity=0.3,
        name='Daily High'))

    fig.add_trace(go.Scatter(
        x=df_historical['date'],
        y=df_historical['low'],
        marker_color=color_price,
        line_width=1,
        opacity=0.3,
        name='Daily Low'))

    fig.add_trace(go.Scatter(
        x=df_long['date_trade'],
        y=df_long['average_price'],
        mode='markers',
        marker_color=color_long,
        marker_symbol='x',
        text=df_long['registrant'] + "<br>" + df_long['trade_type'],
        name='Buys'))
    
    fig.add_trace(go.Scatter(
        x=df_short['date_trade'],
        y=df_short['average_price'],
        mode='markers',
        marker_color=color_short,
        marker_symbol='x',
        text=df_short['registrant'] + "<br>" + df_short['trade_type'],
        name='Sells'))

    fig.update_layout(
        plot_bgcolor='white',
        title="Historical price data & Director Dealings",
        xaxis_title="Date",
        yaxis_title="Price")
    
    return fig


################################### API CALLS
async def get_result(query, params):
    async with ClientSession() as session:
        async with session.get(query, params=params) as response:
            response = await response.json()
            return response
        
async def get_trades(query, params):
    async with ClientSession() as session:
        async with session.get(query, params=params) as response:
            response = await response.json()
            return response
        
async def get_historical(query, params):
    async with ClientSession() as session:
        async with session.get(query, params=params) as response:
            response = await response.json()
            return response
        
################################### COMPANY SELECTION
st.title("Directors' Dealings for Germany")

companies_dict = asyncio.run(get_result(url.format('companies'), params=None))
df_unqiue_emitters = pd.DataFrame().from_dict(companies_dict)

label = f"Please select the company (download of data might take a few seconds)"

chosen_emitter = st.selectbox(options=df_unqiue_emitters['emitters'],
                              label=label)

params_isin = {'emitter': chosen_emitter}

################################### ISIN SELECTION
valid_isin = asyncio.run(get_result(url.format('isin'), params=params_isin))[0][0]
st.write(f'{chosen_emitter} has the following ISIN: {valid_isin}')

params_trades = {'emitter': chosen_emitter,
                 'isin': str(valid_isin)}

################################### PREPARE TRADES
df_trades = asyncio.run(get_trades(url.format('trades'), params=params_trades))
df_trades = pd.DataFrame().from_dict(df_trades, orient='index').T

################################### SHOW HISTORICAL
df_historical = asyncio.run(get_trades(url.format('historical'), params=params_trades))
df_historical = pd.DataFrame().from_dict(df_historical, orient='index').T

try:
    fig = plot_history_and_trades(df_historical, df_trades)
    st.plotly_chart(fig, use_container_width=True)
except Exception as error:
    st.write("Oops! An exception has occured:", error)
    st.write("Exception TYPE:", type(error))
    st.write('Historical data currently not available')
    
################################### SHOW RETURNS
# st.write(f"The current price of the ISIN is: {current_price}")
accounts = asyncio.run(get_trades(url.format('returns'), params=params_trades))
df_director_returns = pd.DataFrame().from_dict(accounts, orient='index').T.round(decimals=2)
st.write(df_director_returns)


################################### SHOW TRADES
show = st.checkbox('Show all individual trades')
if show:
    st.write(df_trades)