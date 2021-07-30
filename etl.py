import urllib.error
import pandas as pd
import pandas.errors
from urllib.request import urlretrieve
from data_transformer import date_series_to_datetime, filter_dataframe, merge_dataframes
from data_handler import CovidDayStats, CovidDataContainer

NYT_DATASET_URL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
HOPKINS_DATASET_URL = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv'

most_recent_error_message = "An unknown error occurred."


def download_covid_dataframes(nyt_url: str, hopkins_url: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Downloads COVID datasets and reads them into dataframes.
    """
    urlretrieve(nyt_url, 'nyt_data.csv')
    urlretrieve(hopkins_url, 'hopkins_data.csv')
    nyt_df = pd.read_csv('nyt_data.csv')
    hopkins_df = pd.read_csv('hopkins_data.csv')
    return nyt_df, hopkins_df


def transform_dataframes(nyt_df: pd.DataFrame, hopkins_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Transforms data in the dataframes.
    """
    nyt_df['date'] = date_series_to_datetime(nyt_df)
    hopkins_transformed = filter_dataframe(hopkins_df)
    return nyt_df, hopkins_transformed


def merge_covid_dataframes(nyt_df: pd.DataFrame, hopkins_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges two dataframes.
    """
    merged_covid_data = merge_dataframes(nyt_df, hopkins_df)
    return merged_covid_data


def extract_transform(nyt_dataset_url: str, hopkins_dataset_url: str) -> pd.DataFrame:
    """
    Downloads, extracts, transforms, and merges two sets of COVID data into one dataset.
    """
    global most_recent_error_message
    try:
        nyt_df, hopkins_df = download_covid_dataframes(nyt_dataset_url, hopkins_dataset_url)
    except urllib.error.HTTPError as error:
        most_recent_error_message = f"The import failed. {error}"
        # TODO SNS error notification
        raise
    try:
        transformed_nyt_df, transformed_hopkins_df = transform_dataframes(nyt_df, hopkins_df)
    except ValueError as error:
        most_recent_error_message = f"Some data was invalid. {error}"
        # TODO SNS error notification
        raise
    try:
        covid_data = merge_covid_dataframes(transformed_nyt_df, transformed_hopkins_df)
    except (pandas.errors.MergeError, ValueError) as error:
        most_recent_error_message = f"The merge failed. {error}"
        # TODO SNS error notification
        raise
    return covid_data


def load_initial_data(covid_dataset: pd.DataFrame, container: CovidDataContainer):
    """
    Loads all rows from the dataset into the database.
    """
    print("Loading initial data into database...")
    add_rows_to_container(covid_dataset, container)
    message = "Initial data loaded"
    number_of_rows = len(container)
    send_notification(message, number_of_rows)


def load_recent_updates(covid_dataset: pd.DataFrame,
                        most_recent_database_date: pd.Timestamp,
                        container: CovidDataContainer):
    """
    Drops any row from the dataset that is already stored in the database, and loads only new rows into the database.
    """
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
    """
    Sends a 'no update' message.
    """
    message = "No new data to load"
    number_of_rows = 0
    send_notification(message, number_of_rows)


def add_rows_to_container(rows_to_add: pd.DataFrame, container: CovidDataContainer):
    """
    Adds dataset rows to a container.
    """
    for index, row in rows_to_add.iterrows():
        container.add_day(CovidDayStats(str(row.date), row.cases, row.deaths, row.recovered))
    return container


def send_notification(message: str, number_of_rows: int):
    """
    Prints an informational message.
    """
    notification = f"{message}, added {number_of_rows} rows to the database."
    print(notification)
    # TODO SNS notification goes here
    return notification


def load_to_database(nyt_dataset_url: str, hopkins_dataset_url: str):
    """
    Extracts and transforms two datasets and loads the merged dataset into a database.
    """
    global most_recent_error_message
    try:
        # extract and transform datasets
        covid_dataset = extract_transform(nyt_dataset_url, hopkins_dataset_url)
        data_container = CovidDataContainer()

        if False:
            # test code for local container
            for index, row in covid_dataset.iterrows():
                data_container.add_day(CovidDayStats(str(row.date), row.cases, row.deaths, row.recovered))
                if index >= 547:
                    break

        # retrieve most recent date from database and extracted covid dataset
        most_recent_dataset_date = covid_dataset['date'][covid_dataset.index[-1]]
        most_recent_database_date = data_container.get_most_recent_date()

        if len(data_container) == 0:
            # loads entire dataset into the database
            load_initial_data(covid_dataset, data_container)
        elif most_recent_dataset_date > most_recent_database_date:
            # updates the database with new data
            load_recent_updates(covid_dataset, most_recent_database_date, data_container)
        else:
            # no new data loaded into the database
            no_update()
    except:
        # something went wrong
        print(most_recent_error_message)
        return None
    return data_container


if __name__ == "__main__":
    load_to_database(NYT_DATASET_URL, HOPKINS_DATASET_URL)
