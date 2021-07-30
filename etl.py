import urllib.error
import pandas as pd
from urllib.request import urlretrieve
from data_transformer import date_series_to_datetime, filter_dataframe, merge_dataframes
from data_handler import CovidDayStats, CovidDataContainer

NYT_DATASET_URL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
HOPKINS_DATASET_URL = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv'


def download_covid_dataframes(nyt_url, hopkins_url):
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


def merge_covid_dataframes(nyt_df, hopkins_df):
    """
    Merge the data sets into one dataframe
    """
    merged_covid_data = merge_dataframes(nyt_df, hopkins_df)
    return merged_covid_data


def extract_transform(nyt_dataset_url, hopkins_dataset_url):
    try:
        nyt_df, hopkins_df = download_covid_dataframes(nyt_dataset_url, hopkins_dataset_url)
    except urllib.error.HTTPError:
        print("The import failed")
        # TODO SNS error notification
        raise
    try:
        transformed_nyt_df, transformed_hopkins_df = transform_dataframes(nyt_df, hopkins_df)
    except ValueError:
        print("Some data was invalid")
        # TODO SNS error notification
        raise
    try:
        covid_data = merge_covid_dataframes(transformed_nyt_df, transformed_hopkins_df)
    except:
        print("The merge failed")
        # TODO SNS error notification
        raise
    return covid_data


def load_initial_data(covid_dataset, container):
    print("Loading initial data into database...")
    add_rows_to_container(covid_dataset, container)
    message = "Initial data loaded"
    number_of_rows = len(container)
    send_notification(message, number_of_rows)


def load_recent_updates(covid_dataset, most_recent_database_date, container):
    print("Loading only new data into database...")
    # find dataset index of most recent database date
    dataset_index_of_database_most_recent_date = int(covid_dataset[covid_dataset['date'] ==
                                                                   most_recent_database_date].index.values)

    # eliminate all dataset indices prior to most recent database date index
    newest_covid_data = covid_dataset.drop(covid_dataset.index[range(0, dataset_index_of_database_most_recent_date +
                                                                     1)])

    # use data_container.add_day to add only new data to database
    add_rows_to_container(newest_covid_data, container)

    message = "Update successful"
    number_of_rows = len(newest_covid_data)
    send_notification(message, number_of_rows)


def no_update():
    message = "No new data to load"
    number_of_rows = 0
    send_notification(message, number_of_rows)


def add_rows_to_container(rows_to_add, container):
    for index, row in rows_to_add.iterrows():
        container.add_day(CovidDayStats(str(row.date), row.cases, row.deaths, row.recovered))
    return container


def send_notification(message, number_of_rows):
    notification = f"{message}, added {number_of_rows} rows to the database."
    print(notification)
    # TODO SNS notification goes here
    return notification


def load_to_database(nyt_dataset_url, hopkins_dataset_url):
    try:
        covid_data = extract_transform(nyt_dataset_url, hopkins_dataset_url)
        # print(type(covid_data))
        data_container = CovidDataContainer()

        # for index, row in covid_data.iterrows():
        #     data_container.add_day(CovidDayStats(str(row.date), row.cases, row.deaths, row.recovered))
        #     if index >= 547:
        #         break

        most_recent_dataset_date = covid_data['date'][covid_data.index[-1]]
        most_recent_database_date = data_container.get_most_recent_date()

        if len(data_container) == 0:
            load_initial_data(covid_data, data_container)
        elif most_recent_dataset_date > most_recent_database_date:
            load_recent_updates(covid_data, most_recent_database_date, data_container)
        else:
            no_update()
    except:
        print("Something went wrong!")
        return None
    return data_container


if __name__ == "__main__":
    load_to_database(NYT_DATASET_URL, HOPKINS_DATASET_URL)
