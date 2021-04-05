import os
import sys
if sys.path[0] != '..':
    sys.path.insert(0, '..')
from datetime import datetime

import psycopg2
import pandas as pd

from library.utils_sql import *

date = datetime.today().strftime("%Y_%m_%d")

host_name = "db"
database_name = "directors"
user_name = ""
user_password = ""


# ----------------------------------------------
def anti_join(df_new, df_old):
    """
    Input: Recently scraped new data
    Output: Rows which are present in df_new but not in df_old"""
    columns_to_merge = df_new.columns.tolist()
    ans = pd.merge(left=df_new, right=df_old, how='left', indicator=True, on=columns_to_merge)
    ans = ans.loc[ans._merge == 'left_only', :].drop(columns='_merge')
    return ans


############################## QUERIES ###########################
query_existing_data = """
    SELECT *
    FROM trades_raw
"""

query_insert_trades = '''
INSERT INTO trades_raw (emitter, bafin_id, isin, registrant, position_status, instrument_type, trade_type, average_price, aggregated_volume, date_notification, date_trade, place_trade, date_activation, price_currency, volume_currency) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

query_drop_trades = """
DROP TABLE IF EXISTS trades
"""

query_create_table_trades = """
CREATE TABLE trades (LIKE trades_raw INCLUDING ALL)
"""

query_clean_trades = """
INSERT INTO trades SELECT *
FROM trades_raw
WHERE isin != 'unknown'
AND trade_type != 'Sonstiges'
AND aggregated_volume >= 5000
AND position_status IN ('Aufsichtsrat', 'Vorstand');
"""

############################## LOGIC ###########################
connection = create_server_connection(host_name, database_name, user_name, user_password)

# existing data in database
data, columns = read_query(connection, query_existing_data)
df_old = pd.DataFrame(data=data, columns=columns)

df_old['average_price'] = df_old['average_price'].astype(float)
df_old['aggregated_volume'] = df_old['aggregated_volume'].astype(float)

# scraped data in database
df_new = pd.read_csv(f'../data/{date}.csv')
df_new['bafin_id'] = df_new['bafin_id'].astype(str)

df_new['average_price'] = df_new['average_price'].astype(float)
df_new['aggregated_volume'] = df_new['aggregated_volume'].astype(float)

# new entries
rows_to_insert = anti_join(df_new, df_old)

# converting my data into tuples to insert into database
values = list(rows_to_insert.itertuples(index=False, name=None))
execute_list_query(connection, query_insert_trades, values)

# populate cleaned table
execute_query(connection, query_drop_trades)
execute_query(connection, query_create_table_trades)
execute_query(connection, query_clean_trades)









