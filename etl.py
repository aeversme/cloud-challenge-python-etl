import urllib.error
import pandas as pd
from urllib.request import urlretrieve
from data_transformer import date_series_to_datetime, filter_dataframe, merge_dataframes
from data_handler import CovidDayStats, CovidDataContainer

NYT_DATA = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
HOPKINS_DATA = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv'


def download_read_csv_data(nyt_url, hopkins_url):
    """
    Downloads COVID data sets and reads them into dataframes
    """
    try:
        urlretrieve(nyt_url, 'nyt_data.csv')
        urlretrieve(hopkins_url, 'hopkins_data.csv')
    except urllib.error.HTTPError:
        # TODO SNS notification
        print("There was an error retrieving a data set.")
        raise
    nyt_df = pd.read_csv('nyt_data.csv')
    hopkins_df = pd.read_csv('hopkins_data.csv')
    return nyt_df, hopkins_df


def transform_dataframes(nyt_df, hopkins_df):
    """
    Transforms data in the dataframes
    """
    nyt_df['date'] = date_series_to_datetime(nyt_df)
    hopkins_transformed = filter_dataframe(hopkins_df)
    return nyt_df, hopkins_transformed


def covid_data_merge(nyt_df, hopkins_df):
    """
    Merge the data sets into one dataframe
    """
    merged_covid_data = merge_dataframes(nyt_df, hopkins_df)
    return merged_covid_data


nyt_df, hopkins_df = download_read_csv_data(NYT_DATA, HOPKINS_DATA)
transformed_nyt_df, transformed_hopkins_df = transform_dataframes(nyt_df, hopkins_df)
covid_data = covid_data_merge(transformed_nyt_df, transformed_hopkins_df)

# print(covid_data)

data_container = CovidDataContainer()

for index, row in covid_data.iterrows():
    data_container.add_day(CovidDayStats(str(row.date), row.cases, row.deaths, row.recovered))
    # if index >= 548:
    #     break
# print(data_container.get_day_with_timestamp(pd.Timestamp("2020-12-01")))

most_recent_dataset_date = covid_data['date'][covid_data.index[-1]]
most_recent_database_date = data_container.get_most_recent_date()

if len(data_container) == 0:
    print("Loading initial data into database...")
    for index, row in covid_data.iterrows():
        data_container.add_day(CovidDayStats(str(row.date), row.cases, row.deaths, row.recovered))
    print(len(data_container))
elif most_recent_dataset_date > most_recent_database_date:
    print("Loading only new data into database...")

    # find dataset index of most recent database date
    dataset_index_of_database_most_recent_date = int(covid_data[covid_data['date'] ==
                                                                most_recent_database_date].index.values)
    # eliminate all dataset indices prior to most recent database date index
    new_covid_data = covid_data.drop(covid_data.index[range(0, dataset_index_of_database_most_recent_date + 1)])
    # print modified dataframe
    print(most_recent_database_date)
    print(new_covid_data)
    # use data_container.add_day to add only new data to database
    for index, row in new_covid_data.iterrows():
        data_container.add_day(CovidDayStats(str(row.date), row.cases, row.deaths, row.recovered))
    print(data_container.get_most_recent_date())
else:
    print("No new data to load")
