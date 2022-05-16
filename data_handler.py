import pandas as pd
from botocore.exceptions import ClientError


def get_most_recent_date(table):
    """
    If the table contains data, returns the most recent date; else, returns None.
    """
    response = table.scan()

    data = response['Items']
    most_recent_date = pd.Timestamp("2000-01-01")

    if data:
        data_dates = [item['date'] for item in data]
        for date in data_dates:
            date_timestamp = pd.Timestamp(date)
            if date_timestamp > most_recent_date:
                most_recent_date = date_timestamp
        return most_recent_date
    else:
        return None


def add_rows_to_database(rows_to_add: pd.DataFrame, table):
    """
    Adds dataset rows to the database; returns number of rows added to the database.
    """
    batch_size = 100
    batch = []
    rows_added = 0

    for row in rows_to_add.itertuples():
        if len(batch) >= batch_size:
            write_to_database(batch, table)
            batch.clear()
        batch.append(row)
        rows_added += 1

    if batch:
        write_to_database(batch, table)

    return rows_added


def write_to_database(rows, table):
    """
    Writes one batch of up to 100 items to the database.
    """
    try:
        with table.batch_writer() as batch:
            for row in rows:
                batch.put_item(
                    Item={
                        'date': str(row.date),
                        'cases': row.cases,
                        'deaths': row.deaths,
                        'recovered': row.recovered,
                    }
                )
    except ClientError as e:
        print(e)
        print("Error executing batch writer.")


# class CovidDayStats:
#     def __init__(self, datestring, cases, deaths, recovered):
#         self.datestring = datestring
#         self.cases = cases
#         self.deaths = deaths
#         self.recovered = recovered
#
#     def date_as_timestamp(self):
#         date = pd.Timestamp(self.datestring)
#         return date
#
#     def __str__(self):
#         return f"date: {self.datestring} / cases: {self.cases} / deaths: {self.deaths} / recovered: {self.recovered}"
#
#
# class CovidDataContainer:
#     def __init__(self):
#         self.data_dict = {}
#
#     def add_day(self, day):
#         self.data_dict[day.date_as_timestamp()] = day
#
#     def get_day_with_timestamp(self, timestamp):
#         return self.data_dict[timestamp]
#
#     def get_most_recent_date(self):
#         most_recent_date = pd.Timestamp("2010-01-01")
#         for key in self.data_dict:
#             key_timestamp = pd.Timestamp(key)
#             if key_timestamp > most_recent_date:
#                 most_recent_date = key_timestamp
#         return most_recent_date
#
#     def __len__(self):
#         return len(self.data_dict)
#
#     def __str__(self):
#         return f"A Covid data container with {len(self.data_dict)} rows."
