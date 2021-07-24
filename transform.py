import pandas as pd


def date_series_to_datetime(dataframe):
    """
    Transform date object series to datetime dtype
    """
    return pd.to_datetime(dataframe['date'])


def filter_dataframe(dataframe):
    """
    Transform a dataframe by filtering on a value and dropping unneeded columns
    """
    dataframe.rename(columns=str.lower, inplace=True)
    dataframe['date'] = date_series_to_datetime(dataframe)
    dataframe_filtered = dataframe[dataframe['country/region'] == 'US']
    dataframe_reduced = dataframe_filtered.drop(columns=['country/region', 'province/state', 'confirmed', 'deaths'])
    return dataframe_reduced


def merge_dataframes(dataframe_1, dataframe_2):
    """
    Merge two dataframes
    """
    return dataframe_1.merge(dataframe_2, on='date', how='inner')
