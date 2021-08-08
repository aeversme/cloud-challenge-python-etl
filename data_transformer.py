import pandas as pd


def date_series_to_datetime(dataframe):
    """
    Transforms a series of datestring objects to datetime objects.
    """
    return pd.to_datetime(dataframe['date'])


def filter_dataframe(dataframe):
    """
    Transforms a dataframe by filtering on a value and dropping unneeded columns.
    """
    dataframe.rename(columns=str.lower, inplace=True)
    dataframe['date'] = date_series_to_datetime(dataframe)
    dataframe['recovered'] = dataframe['recovered'].fillna(0)
    dataframe['recovered'] = dataframe['recovered'].astype(int)
    dataframe_filtered = dataframe[dataframe['country/region'] == 'US']
    dataframe_reduced = dataframe_filtered.drop(columns=['country/region', 'province/state', 'confirmed', 'deaths'])
    return dataframe_reduced


def merge_dataframes(dataframe_1, dataframe_2):
    """
    Merges two dataframes using the inner method.
    """
    return dataframe_1.merge(dataframe_2, on='date', how='inner')
