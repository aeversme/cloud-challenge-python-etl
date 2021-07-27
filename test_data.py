from data_load import CovidDayStats
import pandas as pd


def test_data_row_creation():
    row_test = CovidDayStats("2021-7-26")
    assert row_test.datestring == "2021-7-26"
    assert row_test.cases == 0
    assert row_test.deaths == 0
    assert row_test.recovered == 0


def test_date_as_timestamp():
    row_test = CovidDayStats("2021-7-26")
    assert row_test.date_as_timestamp() == pd.Timestamp(year=2021, month=7, day=26)
