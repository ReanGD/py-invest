import pandas as pd
from storage import INFLATION
from website_api.base_data_loader import BaseDataLoader


class InflationLoader(BaseDataLoader):
    name_id = INFLATION

    def __init__(self, country : str ="russia"):
        super(InflationLoader, self).__init__("InflationLoader")
        self.country = country

    def load_data(self, save_path : str):
        # doc: https://www.statbureau.org/ru/inflation-api
        # example: https://www.statbureau.org/get-data-json?country=russia
        url_params = {
            "country": self.country,
        }
        data = self._load_url("https://www.statbureau.org/get-data-json", url_params)

        df = pd.read_json(data, convert_dates=["MonthFormatted"], precise_float=True).sort_values(by="MonthFormatted", ascending=True)
        df = df.drop(columns=["InflationRateFormatted", "InflationRateRounded", "Country", "Month"]).rename(columns={"InflationRate": "value", "MonthFormatted": "month"}).set_index("month")
        df.to_csv(save_path, sep=";", encoding="utf-8")
