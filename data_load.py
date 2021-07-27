import pandas as pd
import datetime


class CovidDayStats:
    def __init__(self, datestring, cases, deaths, recovered):
        self.datestring = datestring
        self.casesstring = str(cases)
        self.deaths = deaths
        self.recovered = recovered

    def date_as_timestamp(self):
        date = pd.Timestamp(self.datestring)
        return date

    # def date_as_datetime(self):
    #     date = datetime.datetime(self.datestring)
    #     return date

    def __str__(self):
        return f"date: {self.datestring} / cases: {self.casesstring} / deaths: {self.deaths} / recovered: {self.recovered}"

# CovidDataContainer class
#
#     dictionary of rows
#         primary key = date


