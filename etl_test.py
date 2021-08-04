import pytest
import urllib.error
from etl import download_covid_dataframes, load_to_database, publish_to_sns
from data_handler import CovidDataContainer
import pandas as pd

test_url1 = 'http://www.google.com/nonexistantfile'
test_url2 = 'http://www.amazon.com/yousillygoose'

test_good_hopkins_data_url = 'https://raw.githubusercontent.com/aeversme/cloud-challenge-python-etl/main/test_data/sample_good_hopkins_data.csv'
test_good_nyt_data_url = 'https://raw.githubusercontent.com/aeversme/cloud-challenge-python-etl/main/test_data/sample_good_nyt_data.csv'


def test_http_error():
    with pytest.raises(urllib.error.HTTPError):
        download_covid_dataframes(test_url1, test_url2)


# def test_load_to_database_failure():
#     database = load_to_database(test_url1, test_url2)
#     assert database is None


def test_load_to_database_success():
    database = load_to_database(test_good_nyt_data_url, test_good_hopkins_data_url)
    assert isinstance(database, CovidDataContainer)
    assert len(database) == 2
    assert database.get_most_recent_date() == pd.Timestamp("2021-01-03")


# def test_publish_to_sns():
#     message = "Success"
#     number_of_rows = 5
#     assert publish_to_sns(message) == "Success, added 5 rows to the database."
