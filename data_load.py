import pandas as pd


class CovidDayStats:
    def __init__(self, datestring, cases, deaths, recovered):
        self.datestring = datestring
        self.cases = cases
        self.deaths = deaths
        self.recovered = recovered

    def date_as_timestamp(self):
        date = pd.Timestamp(self.datestring)
        return date

    def __str__(self):
        return f"date: {self.datestring} / cases: {self.cases} / deaths: {self.deaths} / recovered: {self.recovered}"


class CovidDataContainer:
    def __init__(self):
        self.data_dict = {}

    def add_day(self, day):
        self.data_dict[day.date_as_timestamp()] = day

    def get_day_with_timestamp(self, timestamp):
        return self.data_dict[timestamp]

    def __str__(self):
        return f"A Covid data container with {len(self.data_dict)} rows."

#
#     dictionary of rows
#         primary key = date


