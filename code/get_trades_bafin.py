from datetime import datetime
import pandas as pd
    
# ----------------------------------------------
def convert_to_float(series):
    """ 
    Function converts a column listed with a , decimal and a 3 symbol code (EUR, USD, etc.)
    into a float series.
    """
    series = series.str[:-4].str.replace('.', '').str.replace(',', '.').astype(float)
    return series
    
    
date = datetime.today().strftime("%Y_%m_%d")

raw_file = 'https://portal.mvp.bafin.de/database/DealingsInfo/sucheForm.do?meldepflichtigerName=&zeitraum=0&d-4000784-e=1&emittentButton=Suche+Emittent&emittentName=&zeitraumVon=&emittentIsin=&6578706f7274=1&zeitraumBis='

columns = [
           'emitter',
           'bafin_id',
           'isin',
           'registrant',
           'position_status',
           'instrument_type',
           'trade_type',
           'average_price',
           'aggregated_volume',
           'date_notification',
           'date_trade',
           'place_trade',
           'date_activation'
]

df_raw = pd.read_csv(raw_file, sep=';')

# rename columns
df_raw.columns = columns
# convert id column into str
df_raw['bafin_id'] = df_raw['bafin_id'].astype(str)
# fill empty values with unknown
df_raw['isin'] = df_raw['isin'].fillna('unknown')
df_raw['place_trade'] = df_raw['place_trade'].fillna('unknown')
# fill empty values with 0 as the value and N/A for currency
df_raw['average_price'] = df_raw['average_price'].fillna('0 NAP')
df_raw['aggregated_volume'] = df_raw['aggregated_volume'].fillna('0 NAP')
# clear edge case where value is present but no currency
helper_series = df_raw['average_price'].str.replace('.', '').str.replace(',', '.').str.split(' ').str.len()
df_raw.loc[helper_series == 1, ['average_price', 'aggregated_volume']] = '0 NAP'
df_raw['price_currency'] = df_raw['average_price'].str.replace('.', '').str.replace(',', '.').str.split(' ').apply(lambda x: x[-1])
df_raw['volume_currency'] = df_raw['aggregated_volume'].str.replace('.', '').str.replace(',', '.').str.split(' ').apply(lambda x: x[-1])
# convert pricing and volume columns into float
df_raw['average_price'] = convert_to_float(df_raw['average_price'])
df_raw['aggregated_volume'] = convert_to_float(df_raw['aggregated_volume'])

df_raw.to_csv(f'../data/{date}.csv', index=False)