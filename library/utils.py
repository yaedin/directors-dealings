from datetime import datetime
from decimal import Decimal
from random import random
import os
import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import plotly.graph_objects as go

base_url = 'https://www.ariva.de/'

color_short = 'rgb(219, 90, 68)'
color_long = 'rgb(108, 156, 12)'
color_price = 'rgb(113, 124, 145)'

# ----------------------------------------------
def convert_to_float(series):
    """ 
    Function converts a column listed with a , decimal and a 3 symbol code (EUR, USD, etc.)
    into a float series.
    """
    series = series.str[:-4].str.replace('.', '').str.replace(',', '.').astype(float)
    return series


# ----------------------------------------------
def parse_datetime_columns(df):
    """
    Correctly parse datetime columns for df and return it
    """
    df['date_notification'] = pd.to_datetime(df['date_notification'], format='%d.%m.%Y')
    df['date_trade'] = pd.to_datetime(df['date_trade'], format='%d.%m.%Y')
    df['date_activation'] = pd.to_datetime(df['date_activation'], format='%d.%m.%Y %H:%M:%S')
    
    return df

# ----------------------------------------------
def anti_join(df_new, df_old):
    """
    Input: Recently scraped new data
    Output: Rows which are present in df_new but not in df_old"""
    columns_to_merge = df_new.columns.tolist()
    ans = pd.merge(left=df_new, right=df_old, how='left', indicator=True, on=columns_to_merge)
    ans = ans.loc[ans._merge == 'left_only', :].drop(columns='_merge')
    return ans


# ----------------------------------------------
def check_isin_is_valid(isin):
    """ 
    Input: ISIN
    Output: True if ISIN returns a stock prize, False otherwise
    """
    r = requests.get(base_url + isin)
    if r.status_code == 200:
        return True
    else:
        return False

    
# ----------------------------------------------
def get_unique_isins(df, emitter):
    """
    Input: dataframe, emitter
    Output: Returns either one isin as string, if only one is availabe in the dataframe or array of all isins available
    """
    df_emitter_isin = df.loc[df['emitter'] == emitter, 'isin']
    count_isins = df_emitter_isin.nunique()
    if  count_isins == 1:
        return df_emitter_isin.unique().item()
    else: 
        return df_emitter_isin.unique()
    
# ----------------------------------------------
def check_isin_is_valid(isin):
    """ 
    Input: ISIN
    Output: True if ISIN returns a stock prize, False otherwise
    """
    r = requests.get(base_url + isin)
    if r.status_code == 200:
        return True
    else:
        return False
    
    
# ----------------------------------------------
def get_current_price(isin):
    """
    Function which returns the current price of the asset, based on the ariva website
    """    
    try:
        r = requests.get(base_url + isin)

        soup = BeautifulSoup(r.text, 'html.parser')
        raw_string = soup.body.find('span', attrs={'itemprop': 'price'}).text
        value_string = re.search("[0-9,]+", raw_string).group()
        value_float = round(float(value_string.replace(',', '.')), 2)
        print(value_float)
    except:
        # is this a smart way of handling exceptions?
        value_float = 0
        
    return value_float


# ----------------------------------------------
def get_historical_data(isin):
    date = datetime.today().strftime("%Y_%m_%d")
    date_start = "01.01.2019"

    profile = webdriver.FirefoxProfile()
    profile.set_preference('browser.download.folderList', 2) # apparently needed for a custom download location
    profile.set_preference('browser.download.manager.showWhenStarting', False)
    profile.set_preference('browser.download.dir', os.getcwd())
    profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'text/csv')
    
    options = Options()
    options.headless = True
    
    try:
        browser = webdriver.Firefox(firefox_profile=profile, options=options)
        browser.get(f"https://www.ariva.de/{isin}/historische_kurse")
        browser.implicitly_wait(10)
        browser.switch_to.frame("sp_message_iframe_213940")
        WebDriverWait(browser, 20).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div/div[3]/div[3]/div[2]/button'))).click()
        browser.switch_to.default_content()
        browser.find_element(By.ID, "minTime").send_keys(date_start)
        browser.find_element(By.CSS_SELECTOR, ".submitButton").click()
        browser.close()
        
        file_to_rename = ''
        for file in os.listdir():
            if file.startswith('wkn'):
                file_to_rename = file
        
        os.rename(file_to_rename, f'../data/historical/{isin}_{date}.csv')
        
        return True
    
    except:
        return False
    
# ----------------------------------------------
def create_jitter_values(series):
    """
    This function creates very small random values, so 
    that some jitter is introduced for the visualization.
    """
    len_series = series.shape[0]
    jitter_values = []
    for i in range(len_series):
        jitter_values.append(Decimal(random() / 100))
    
    return jitter_values

# ----------------------------------------------
def plot_history_and_trades(df_historical, df_emitter_trades):
    fig = go.Figure()
    
    df_emitter_trades['rand'] = create_jitter_values(df_emitter_trades['average_price'])
    df_emitter_trades['average_price'] = df_emitter_trades['average_price'] + df_emitter_trades['rand']
    
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


# ----------------------------------------------
def initiate_account(director, accounts):
    accounts[director] = {}
    accounts[director]['current_stock'] = 0
    accounts[director]['current_balance'] = 0
    accounts[director]['average_price'] = 0
    
    accounts[director]['total_invest'] = 0
    accounts[director]['outstanding_earnings'] = 0
    accounts[director]['outstanding_roi'] = 0
    
    accounts[director]['realized_invest'] = 0
    accounts[director]['realized_earnings'] = 0
    accounts[director]['realized_roi'] = 0
    
    return accounts
    
# ----------------------------------------------
def buy(trade, director, accounts, current_price):
    count_stock = trade['aggregated_volume'] / trade['average_price']
    accounts[director]['current_stock'] += count_stock
    accounts[director]['current_balance'] = accounts[director]['current_stock'] * current_price
    
    # total money invested so far
    accounts[director]['total_invest'] += trade['aggregated_volume']
    accounts[director]['outstanding_earnings'] = accounts[director]['current_balance'] - accounts[director]['total_invest']
    accounts[director]['outstanding_roi'] = (accounts[director]['outstanding_earnings'] / accounts[director]['total_invest']) * 100
    
    accounts[director]['average_price'] = accounts[director]['total_invest'] / accounts[director]['current_stock']
    
    return accounts


# ----------------------------------------------
def sell(trade, director, accounts, current_price):
    count_stock = trade['aggregated_volume'] / trade['average_price']
    
    # if directors sells more than he "owns" according to my trade history
    # pretend that he does not sell
    if accounts[director]['current_stock'] - count_stock < 0:
        return accounts
        
    accounts[director]['current_stock'] -= count_stock
    accounts[director]['current_balance'] = accounts[director]['current_stock'] * current_price
    accounts[director]['total_invest'] -= trade['aggregated_volume']
    
    accounts[director]['average_price'] = accounts[director]['total_invest'] / accounts[director]['current_stock']
    accounts[director]['outstanding_earnings'] = accounts[director]['current_balance'] - accounts[director]['total_invest']
    accounts[director]['outstanding_roi'] = (accounts[director]['outstanding_earnings'] / accounts[director]['total_invest']) * 100
    
    accounts[director]['realized_invest'] += trade['aggregated_volume']
    accounts[director]['realized_earnings'] += count_stock * (trade['average_price'] - accounts[director]['average_price'])
    accounts[director]['realized_roi'] = accounts[director]['realized_earnings'] / accounts[director]['realized_invest'] * 100
    return accounts
