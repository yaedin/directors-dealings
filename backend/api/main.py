# backend/api/main.py

import asyncio
import time
import os
from datetime import datetime
import re

import requests
from bs4 import BeautifulSoup
import psycopg2
import pandas as pd
import uvicorn
from fastapi import FastAPI

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from utils_sql import *


############################## VARS & FUNCTIONS ###########################
app = FastAPI()

host_name = "db"
database_name = "directors"
user_name = ""
user_password = ""

connection = create_server_connection(host_name, database_name, user_name, user_password)

base_url = 'https://www.ariva.de/'
path = '/usr/src/docker_practice/data/historical/'
cwd = os.getcwd()
columns_historical = [
    'date',
    'open',
    'high',
    'low',
    'close',
    'count',
    'volume'
]

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
def parse_datetime_columns(df):
    """
    Correctly parse datetime columns for df and return it
    """
    df['date_notification'] = pd.to_datetime(df['date_notification'], format='%d.%m.%Y')
    df['date_trade'] = pd.to_datetime(df['date_trade'], format='%d.%m.%Y')
    df['date_activation'] = pd.to_datetime(df['date_activation'], format='%d.%m.%Y %H:%M:%S')
    
    return df

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
    except Exception as error:
        print ("Oops! An exception has occured:", error)
        print ("Exception TYPE:", type(error))
        value_float = 0
        
    return value_float

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

# ----------------------------------------------
def get_historical_data(isin):
    date = datetime.today().strftime("%Y_%m_%d")
    date_start = "01.01.2019"

    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_experimental_option("prefs", {
      "download.prompt_for_download": False,
      "safebrowsing.enabled": True,
      "download.default_directory" : cwd
    })
    
    try:
        print("TRY BLOCK SELENIUM CALL")
        browser = webdriver.Chrome(options=options)
        browser.get(f"https://www.ariva.de/{isin}/historische_kurse")
        browser.switch_to.frame("sp_message_iframe_440438")
        browser.find_element(By.CSS_SELECTOR, ".message-column:nth-child(2) > .message-component").click()
        browser.switch_to.default_content()
        browser.find_element(By.ID, "minTime").send_keys(date_start)
        browser.find_element(By.CSS_SELECTOR, ".submitButton").click()
        print(browser.title)
        time.sleep(5)
        browser.close()
         
        file_to_move = ''
        for file in os.listdir('/usr/src/'):
            if file.startswith('wkn'):
                file_to_move = file

        os.rename(f'/usr/src/{file_to_move}', f'/usr/src/docker_practice/data/historical/{isin}_{date}.csv')
        return True
    
    except Exception as error:
        print ("Oops! An exception has occured:", error)
        print ("Exception TYPE:", type(error))
        return False

############################## ROUTES ###########################
@app.get("/")
def read_root():
    return {"message": "Welcome to Directors' Dealings"}


# get all companies in database
@app.get("/companies")
def return_companies():
    
    query_unique_emitters = """
        SELECT DISTINCT emitter
        FROM trades
    """
    
    unique_emitters, columns = read_query(connection, query_unique_emitters)
    
    companies_dict = {}
    companies_dict['emitters'] = [unique_emitter[0] for unique_emitter in unique_emitters]
    
    return companies_dict

# get correct isin
@app.get("/isin")
async def return_isin(emitter: str = 'Deutsche Telekom AG'):
    
    query_get_isin = f"""
        SELECT DISTINCT isin
        FROM trades
        where emitter = '{emitter}'
        """
    isins_to_check, column = read_query(connection, query_get_isin)
    
    if len(isins_to_check) == 1:
        valid_isin = isins_to_check
    else:
        for isin in isins_to_check:
            response = check_isin_is_valid(isin)
            if response:
                valid_isin = isin
    
    return valid_isin

# api to return dataframe with all trades
@app.get("/trades")
async def return_trades(emitter: str = 'Deutsche Telekom AG', isin: str = 'DE0005557508'):
    
    query_trades = f"""
        SELECT 
            registrant, position_status, trade_type, average_price, aggregated_volume, date_trade, date_notification, date_activation
        FROM
            trades
        WHERE
            emitter = '{emitter}'
        AND isin = '{isin}'
    """

    emitter_trades, columns = read_query(connection, query_trades)
    df_emitter_trades = pd.DataFrame(data=emitter_trades, columns=columns)
    df_emitter_trades = parse_datetime_columns(df_emitter_trades)
        
    return df_emitter_trades

# api to return dataframe with all returns
@app.get("/returns")
async def return_returns(emitter: str = 'Deutsche Telekom AG', isin: str = 'DE0005557508'):
    query_trades = f"""
        SELECT 
            registrant, position_status, trade_type, average_price, aggregated_volume, date_trade, date_notification, date_activation
        FROM
            trades
        WHERE
            emitter = '{emitter}'
        AND isin = '{isin}'
    """

    emitter_trades, columns = read_query(connection, query_trades)
    df_emitter_trades = pd.DataFrame(data=emitter_trades, columns=columns)
    df_emitter_trades['date_trade'] = pd.to_datetime(df_emitter_trades['date_trade'])
    df_emitter_trades = df_emitter_trades.sort_values(by=['registrant', 'date_trade'])
    df_emitter_trades[['average_price', 'aggregated_volume']] = df_emitter_trades[['average_price', 'aggregated_volume']].astype(float)

    directors_list = df_emitter_trades['registrant'].unique()
    accounts = {}
    
    current_price = get_current_price(isin)

    for director in directors_list:
        accounts = initiate_account(director, accounts)

        df_account = df_emitter_trades.loc[df_emitter_trades['registrant'] == director].reset_index(drop=True)

        for index, trade in df_account.iterrows():
            if trade['trade_type'] == 'Kauf':
                accounts = buy(trade, director, accounts, current_price)

            if trade['trade_type'] == 'Verkauf':
                if accounts[director]['current_stock'] == 0:
                    continue
                accounts = sell(trade, director, accounts, current_price)
        accounts[director]['current_price'] = current_price

    return accounts

# api to list overview of all director dealings
@app.get("/historical")
async def return_historical(emitter: str = 'Deutsche Telekom AG', isin: str = 'DE0005557508'):
    
    try: 
        print("TRY BLOCK API CALL")
        file_to_read = ''
        for file in os.listdir(path):
            if file.startswith(isin):
                file_to_read = file

        df_historical = pd.read_csv(path + '/' + file_to_read, sep=';', nrows = 365 * 2)
        df_historical.columns = columns_historical
   
    except Exception as error:
        print("EXCEPT BLOCK API CALL")
        if get_historical_data(isin):
            file_to_read = ''
            for file in os.listdir(path):
                if file.startswith(isin):
                    file_to_read = file

            df_historical = pd.read_csv(path + '/' + file_to_read, sep=';', nrows = 365 * 2)
            df_historical.columns = columns_historical
        else:
            print ("Oops! An exception has occured:", error)
            print ("Exception TYPE:", type(error))

    df_historical = df_historical.dropna()

    for col in ['open', 'high', 'low', 'close']:
        if df_historical[col].dtype == float:
            continue
        df_historical[col] = df_historical[col].str.replace('.', '').str.replace(',', '.').astype(float)

    df_historical['date'] = pd.to_datetime(df_historical['date'])
    
    return df_historical    

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8080)
