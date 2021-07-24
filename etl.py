import urllib.error

import pandas as pd
from urllib.request import urlretrieve
from transform import date_series_to_datetime, filter_dataframe, merge_dataframes

NYT_DATA = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
HOPKINS_DATA = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv'

# Download .csv files locally
try:
    urlretrieve(NYT_DATA, 'nyt_data.csv')
    urlretrieve(HOPKINS_DATA, 'hopkins_data.csv')
except urllib.error.HTTPError:
    # SNS notification
    print("There was an error retrieving a data set.")
else:
    # Read data from each .csv into a Pandas dataframe
    nyt_df = pd.read_csv('nyt_data.csv')
    hopkins_df = pd.read_csv('hopkins_data.csv')

    # Transform Johns Hopkins data
    hopkins_transformed = filter_dataframe(hopkins_df)

    #  Transform NYT dates to datetime objects
    nyt_df['date'] = date_series_to_datetime(nyt_df)

    # Merge the dataframes, dropping rows that don't share the same datetime
    covid_data = merge_dataframes(nyt_df, hopkins_transformed)

    print(covid_data)
    print(covid_data.dtypes)
