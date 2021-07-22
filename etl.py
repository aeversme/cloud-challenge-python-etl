import pandas as pd
from urllib.request import urlretrieve

NYT_DATA = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
HOPKINS_DATA = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv'

# Download .csv files locally
urlretrieve(NYT_DATA, 'nyt_data.csv')
urlretrieve(HOPKINS_DATA, 'hopkins_data.csv')

# Read data from each .csv into a Pandas dataframe
nyt_df = pd.read_csv('nyt_data.csv')
hopkins_data = pd.read_csv('hopkins_data.csv')

# Transform Johns Hopkins data
hopkins_data.rename(columns=str.lower, inplace=True)
hopkins_data['date'] = pd.to_datetime(hopkins_data['date'])
hopkins_data_filtered = hopkins_data[hopkins_data['country/region'] == 'US']
hopkins_data_reduced = hopkins_data_filtered.drop(columns=['country/region',
                                                           'province/state',
                                                           'confirmed',
                                                           'deaths'])

#  Transform NYT dates to datetime objects
nyt_df['date'] = pd.to_datetime(nyt_df['date'])

# Merge the dataframes, dropping rows that don't share the same datetime
covid_data = nyt_df.merge(hopkins_data_reduced, on='date', how='inner')

print(covid_data)
