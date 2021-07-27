import urllib.error
import pandas as pd
from urllib.request import urlretrieve
from transform import date_series_to_datetime, filter_dataframe, merge_dataframes
from data_load import CovidDayStats, CovidDataContainer

NYT_DATA = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
HOPKINS_DATA = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv'


def download_read_csv_data(nyt_url, hopkins_url):
    # Download .csv files locally
    try:
        urlretrieve(nyt_url, 'nyt_data.csv')
        urlretrieve(hopkins_url, 'hopkins_data.csv')
    except urllib.error.HTTPError:
        # SNS notification
        print("There was an error retrieving a data set.")
        raise
    # Read data from each .csv into a Pandas dataframe
    nyt_df = pd.read_csv('nyt_data.csv')
    hopkins_df = pd.read_csv('hopkins_data.csv')
    return nyt_df, hopkins_df


def transform_dataframes(nyt_df, hopkins_df):
    #  Transform NYT dates to datetime objects
    nyt_df['date'] = date_series_to_datetime(nyt_df)
    # Transform Johns Hopkins data
    hopkins_transformed = filter_dataframe(hopkins_df)
    return nyt_df, hopkins_transformed


def covid_data_merge(nyt_df, hopkins_df):
    # Merge the dataframes, dropping rows that don't share the same datetime
    merged_covid_data = merge_dataframes(nyt_df, hopkins_df)
    return merged_covid_data


nyt_df, hopkins_df = download_read_csv_data(NYT_DATA, HOPKINS_DATA)
transformed_nyt_df, transformed_hopkins_df = transform_dataframes(nyt_df, hopkins_df)
covid_data = covid_data_merge(transformed_nyt_df, transformed_hopkins_df)

print(covid_data)

data_container = CovidDataContainer()

for index, row in covid_data.iterrows():
    data_container.add_day(CovidDayStats(str(row.date), row.cases, row.deaths, row.recovered))

print(data_container.get_day_with_timestamp(pd.Timestamp("2020-12-01")))
