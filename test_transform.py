import pandas as pd
import pytest
from transform import date_series_to_datetime, filter_dataframe, merge_dataframes


def set_up(csv):
    test_df = pd.read_csv(csv)
    return test_df


def test_wrong_datatype_in_date_series_field():
    wrong_datatype = set_up('test_data/wrong_datatype_in_date_series_field.csv')
    with pytest.raises(ValueError):
        date_series_to_datetime(wrong_datatype)


def test_sample_good_nyt_data():
    good_data = set_up('test_data/sample_good_nyt_data.csv')
    good_data['date'] = date_series_to_datetime(good_data)
    assert len(good_data) == 3
    assert good_data['date'].dtype == 'datetime64[ns]'
    assert good_data.date[0] == pd.Timestamp(year=2021, month=1, day=1)
    assert good_data.cases[1] == 234
    assert good_data.deaths[2] == 678


def test_sample_good_hopkins_data():
    good_data = set_up('test_data/sample_good_hopkins_data.csv')
    good_data = filter_dataframe(good_data)
    assert len(good_data) == 3
    assert list(good_data.columns) == ['date', 'recovered']
    assert good_data['date'].dtype == 'datetime64[ns]'
    assert good_data.date[0] == pd.Timestamp(year=2021, month=1, day=2)
    assert good_data.recovered[1] == 567


def test_merge_dataframes():
    nyt_test_data = set_up('test_data/sample_good_nyt_data.csv')
    hopkins_test_data = set_up('test_data/sample_good_hopkins_data.csv')
    nyt_test_data['date'] = date_series_to_datetime(nyt_test_data)
    hopkins_test_data = filter_dataframe(hopkins_test_data)
    merged_data = merge_dataframes(nyt_test_data, hopkins_test_data)
    assert len(merged_data) == 2
    assert list(merged_data.columns) == ['date', 'cases', 'deaths', 'recovered']
    assert merged_data['date'].dtype == 'datetime64[ns]'
    assert merged_data.date[0] == pd.Timestamp(year=2021, month=1, day=2)
    assert merged_data.cases[0] == 234
    assert merged_data.deaths[1] == 678
    assert merged_data.recovered[0] == 456
