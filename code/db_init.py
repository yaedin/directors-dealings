import os
import sys
if sys.path[0] != '..':
    sys.path.insert(0, '..')
from datetime import datetime
from library.utils_sql import *
import pandas as pd
import psycopg2


host_name = "db"
database_name = "directors"
user_name = ""
user_password = ""

date = datetime.today().strftime("%Y_%m_%d")

############################## QUERIES ###########################

query_table_trades = """
CREATE TABLE trades_raw (
    emitter VARCHAR(250) NOT NULL,
    bafin_id VARCHAR(250) NOT NULL,
    isin VARCHAR(250) NOT NULL,
    registrant VARCHAR(250) NOT NULL,
    position_status VARCHAR(250) NOT NULL,
    instrument_type VARCHAR(250) NOT NULL,
    trade_type VARCHAR(250) NOT NULL,
    average_price DECIMAL(15, 2),
    aggregated_volume DECIMAL(15, 2),
    date_notification VARCHAR(250) NOT NULL,
    date_trade VARCHAR(250) NOT NULL,
    place_trade VARCHAR(250), 
    date_activation VARCHAR(250) NOT NULL,
    price_currency VARCHAR(3),
    volume_currency VARCHAR(3)
    );
"""

query_insert_trades = '''
INSERT INTO trades_raw (emitter, bafin_id, isin, registrant, position_status, instrument_type, trade_type, average_price, aggregated_volume, date_notification, date_trade, place_trade, date_activation, price_currency, volume_currency) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''
    
############################## LOGIC ###########################
df = pd.read_csv(f'../data/{date}.csv')

connection = create_server_connection(host_name, database_name, user_name, user_password)
execute_query(connection, query_table_trades)

# converting my data into tuples to insert into database
values = list(df.itertuples(index=False, name=None))
execute_list_query(connection, query_insert_trades, values)
