import pandas as pd
import pytest
from transform import date_series_to_datetime


def set_up(csv):
    test_df = pd.read_csv(csv)
    return test_df


def test_malformed_data():
    string_in_field = set_up('test_data/test_date_as_string.csv')
    with pytest.raises(ValueError):
        date_series_to_datetime(string_in_field)


def test_good_data():
    good_data = set_up('test_data/test_sample_good_nyt_data.csv')
    good_data['date'] = date_series_to_datetime(good_data)
    assert len(good_data) == 1
    assert good_data['date'].dtype == 'datetime64[ns]'
    assert good_data.date[0] == pd.Timestamp(year=2021, month=1, day=1)
    assert good_data.cases[0] == 123
    assert good_data.deaths[0] == 456
