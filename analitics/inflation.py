import pandas as pd
from functools import lru_cache
from storage import FStorage, INFLATION


class InflationLambda:
    def __init__(self):
        self._sum = 0

    def __call__(self, value : float):
        self._sum = (value/100 + 1) * self._sum + value
        return self._sum


class Inflation:
    def __init__(self, root_dir : str):
        self._data = FStorage(root_dir).open_data(INFLATION, index_col="month").sort_index(ascending=True)

    # include start and final dates
    @lru_cache(maxsize=1)
    def get_inflation(self, start_year : int, start_month : int, final_year : int, final_month : int) -> float:
        func = InflationLambda()
        start_date = pd.Timestamp(start_year, start_month, 1)
        final_date = pd.Timestamp(final_year, final_month, 1)
        return self._data.loc[(self._data.index >= start_date) & (self._data.index <= final_date), "value"].apply(func).tail(1).values[0]

    # include start and final dates
    def get_inflation_date(self, start_date : pd.Timestamp, final_date : pd.Timestamp) -> float:
        return self.get_inflation(start_date.year, start_date.month, final_date.year, final_date.month)

    # include start and final dates
    @lru_cache(maxsize=1)
    def get_year_inflation(self, start_year : int, start_month : int, final_year : int, final_month : int) -> float:
        value = self.get_inflation(start_year, start_month, final_year, final_month)
        final_date = pd.Timestamp(final_year, final_month, 1)
        days = (pd.Timestamp(final_year, final_month, final_date.days_in_month) - pd.Timestamp(start_year, start_month, 1)).days + 1
        return value * 365.25 / days

    # include start and final dates
    def get_year_inflation_date(self, start_date : pd.Timestamp, final_date : pd.Timestamp) -> float:
        return self.get_year_inflation(start_date.year, start_date.month, final_date.year, final_date.month)
