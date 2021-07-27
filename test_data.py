import pytest

from data_load import CovidDayStats, CovidDataContainer
import pandas as pd


def setup():
    row_test = CovidDayStats("2021-7-26", 0, 0, 0)
    return row_test


def test_data_row_creation():
    row_test = CovidDayStats("2021-7-26", 0, 0, 0)
    assert row_test.datestring == "2021-7-26"
    assert row_test.cases == 0
    assert row_test.deaths == 0
    assert row_test.recovered == 0


def test_date_as_timestamp():
    row_test = CovidDayStats("2021-7-26", 0, 0, 0)
    assert row_test.date_as_timestamp() == pd.Timestamp(year=2021, month=7, day=26)


def test_add_day():
    container_test = CovidDataContainer()
    row_test_1 = CovidDayStats("2021-7-24", 0, 0, 0)
    row_test_2 = CovidDayStats("2021-7-25", 2, 1, 0)
    row_test_3 = CovidDayStats("2021-7-26", 4, 2, 1)
    container_test.add_day(row_test_1)
    container_test.add_day(row_test_2)
    container_test.add_day(row_test_3)
    assert len(container_test.data_dict) == 3
    query_day = container_test.get_day_with_timestamp(pd.Timestamp("2021-7-25"))
    assert query_day.cases == 2
    with pytest.raises(KeyError):
        bad_query_day = container_test.get_day_with_timestamp(pd.Timestamp("2021-7-23"))
