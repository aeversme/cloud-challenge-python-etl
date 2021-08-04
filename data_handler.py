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

    def get_most_recent_date(self):
        most_recent_date = pd.Timestamp("2010-01-01")
        for key in self.data_dict:
            key_timestamp = pd.Timestamp(key)
            if key_timestamp > most_recent_date:
                most_recent_date = key_timestamp
        return most_recent_date

    def __len__(self):
        return len(self.data_dict)

    def __str__(self):
        return f"A Covid data container with {len(self.data_dict)} rows."
