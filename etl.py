import json
import sys
import boto3
import urllib.error
import pandas as pd
import pandas.errors
from urllib.request import urlretrieve
import data_transformer as dt
import data_handler as dh
import message_handler as mh

NYT_DATASET_URL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
HOPKINS_DATASET_URL = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv'
IS_RUNNING_LOCALLY = False

if IS_RUNNING_LOCALLY:
    NYT_CSV_PATH = './tmp/nyt_data.csv'
    HOPKINS_CSV_PATH = './tmp/hopkins_data.csv'
else:
    NYT_CSV_PATH = '/tmp/nyt_data.csv'
    HOPKINS_CSV_PATH = '/tmp/hopkins_data.csv'

most_recent_error_message = 'An unknown error occurred.'
subject = ''
message = ''

sns_client = boto3.client('sns', region_name='us-east-1')
sns_topics_list = sns_client.list_topics()
sns_topic_arn = sns_topics_list['Topics'][1]['TopicArn']
sns = boto3.resource('sns')
topic = sns.Topic(sns_topic_arn)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('covid-data')


def download_covid_dataframes(nyt_url: str, hopkins_url: str):
    """
    Downloads COVID datasets and reads them into dataframes.
    """
    urlretrieve(nyt_url, NYT_CSV_PATH)
    urlretrieve(hopkins_url, HOPKINS_CSV_PATH)
    nyt_df = pd.read_csv(NYT_CSV_PATH)
    hopkins_df = pd.read_csv(HOPKINS_CSV_PATH)
    return nyt_df, hopkins_df


def transform_dataframes(nyt_df: pd.DataFrame, hopkins_df: pd.DataFrame):
    """
    Transforms data in the dataframes.
    """
    nyt_df['date'] = dt.date_series_to_datetime(nyt_df)
    hopkins_transformed = dt.filter_dataframe(hopkins_df)
    return nyt_df, hopkins_transformed


def merge_covid_dataframes(nyt_df: pd.DataFrame, hopkins_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges two dataframes.
    """
    merged_covid_data = dt.merge_dataframes(nyt_df, hopkins_df)
    return merged_covid_data


def extract_transform(nyt_dataset_url: str, hopkins_dataset_url: str) -> pd.DataFrame:
    """
    Downloads, extracts, transforms, and merges two sets of COVID data into one dataset.
    """
    global most_recent_error_message
    try:
        nyt_df, hopkins_df = download_covid_dataframes(nyt_dataset_url, hopkins_dataset_url)
    except urllib.error.HTTPError as error:
        most_recent_error_message = f'The import failed. {error}'
        raise
    except FileNotFoundError as error:
        most_recent_error_message = f'Incorrect file or directory path. {error.filename}: {error.strerror}'
        raise
    try:
        transformed_nyt_df, transformed_hopkins_df = transform_dataframes(nyt_df, hopkins_df)
    except ValueError as error:
        most_recent_error_message = f'Some data was invalid. {error}'
        raise
    try:
        covid_data = merge_covid_dataframes(transformed_nyt_df, transformed_hopkins_df)
    except (pandas.errors.MergeError, ValueError) as error:
        most_recent_error_message = f'The merge failed. {error}'
        raise
    return covid_data


def load_initial_data(covid_dataset: pd.DataFrame):
    """
    Loads all rows from the dataset into the database and sends a success message.
    """
    global subject, message
    subject = 'Initial COVID database load was successful'
    number_of_rows = dh.add_rows_to_database(covid_dataset, table)
    message = f'Initial data loaded: {number_of_rows} rows added to the database.'
    print(message)


def load_recent_updates(covid_dataset: pd.DataFrame, most_recent_database_date: pd.Timestamp):
    """
    Drops any row from the dataset that is already stored in the database, loads only new rows into the database,
    and sends a success message.
    """
    global subject, message
    # find index in dataset of most recent database date
    dataset_index_of_database_most_recent_date = int(covid_dataset[covid_dataset['date'] ==
                                                                   most_recent_database_date].index.values)

    # find new covid data by eliminating all dataset indices prior to index of most recent database date
    new_covid_data = covid_dataset.drop(covid_dataset.index[range(1, dataset_index_of_database_most_recent_date + 1)])

    # add only new data to database
    subject = 'COVID database update successful'
    number_of_rows = dh.add_rows_to_database(new_covid_data, table)
    message = f'Today\'s update was successful: {number_of_rows} row(s) added to the database.'
    print(message)


def load_to_database(nyt_dataset_url: str, hopkins_dataset_url: str):
    """
    Extracts and transforms two datasets and loads the merged dataset into a database.
    """
    global most_recent_error_message, subject, message
    try:
        # extract and transform datasets
        covid_dataset = extract_transform(nyt_dataset_url, hopkins_dataset_url)

        # if IS_RUNNING_LOCALLY:
        #     # test code for local container
        #     for index, row in covid_dataset.iterrows():
        #         data_container.add_day(CovidDayStats(str(row.date), row.cases, row.deaths, row.recovered))
        #         if index >= 547:
        #             break

        # retrieve most recent date from database and extracted covid dataset
        most_recent_dataset_date = covid_dataset['date'][covid_dataset.index[-1]]
        most_recent_database_date = dh.get_most_recent_date(table)

        if most_recent_database_date is None:
            # loads entire dataset into the database
            load_initial_data(covid_dataset)
        elif most_recent_dataset_date > most_recent_database_date:
            # updates the database with new data
            load_recent_updates(covid_dataset, most_recent_database_date)
        else:
            # no new data loaded into the database
            subject = 'No Python ETL update'
            message = 'There was no new data to load: 0 rows added to the database.'
            print(message)
    except Exception as error:
        print(error)
        print(sys.exc_info())
        print(most_recent_error_message)
        subject = 'The Python ETL function encountered an error today'
        message = f'An error occurred. See log stream for more details.' \
                  f'\n\n{most_recent_error_message}' \
                  f'\n\n{sys.exc_info()}'
        raise


def lambda_handler(event, context):
    global subject, message
    try:
        load_to_database(NYT_DATASET_URL, HOPKINS_DATASET_URL)
        mh.publish_message(topic, subject, message)
    except Exception as error:
        print(error)
        mh.publish_message(topic, subject, message)
        return {
            'statusCode': 500,
            'body': json.dumps('Something went wrong. See logs for details.')
        }
    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }


# if __name__ == '__main__' and IS_RUNNING_LOCALLY:
#     load_to_database(NYT_DATASET_URL, HOPKINS_DATASET_URL)
