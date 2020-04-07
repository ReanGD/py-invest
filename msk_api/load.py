import os
import logging
import pandas as pd
from msk_api.loaders import LoaderParams, SecuritiesListLoader, MarketdataLoader, DividendsLoader, TradeHistory

# https://iss.moex.com/iss/engines
# engine = "stock"
# https://iss.moex.com/iss/engines/stock/markets
# market = "shares"
# https://iss.moex.com/iss/engines/stock/markets/shares/boards
# board = "TQBR"
# https://iss.moex.com/iss/securitygroups
# group = "stock_shares"


class Loader:
    def __init__(self, engine, market, board, root_dir):
        self.engine = engine
        self.market = market
        self.board = board

        self.data_dir = os.path.join(root_dir, "data")
        if not os.path.exists(self.data_dir):
            os.mkdir(self.data_dir)

        self.meta_dir = os.path.join(self.data_dir, "meta")
        if not os.path.exists(self.meta_dir):
            os.mkdir(self.meta_dir)

    def get_meta_file_path(self, name) -> str:
        file_name = ""
        if name == "securities":
            file_name = "securities_msk_column.json"
        elif name == "dividends":
            file_name = "dividends_msk_column.json"
        elif name == "trade_history":
            file_name = "trade_history_msk_column.json"
        elif name == "marketdata":
            file_name = "marketdata_msk_column.json"
        else:
            return ""

        return os.path.join(self.meta_dir, file_name)

    def get_base_file_path(self, name) -> str:
        file_name = ""
        if name == "securities":
            file_name = "securities_msk_data.csv"
        elif name == "marketdata":
            file_name = "marketdata_msk_data.csv"
        else:
            return ""

        return os.path.join(self.data_dir, file_name)

    def get_data_file_path(self, name, secid) -> str:
        file_name = ""
        if name == "dividends":
            file_name = "dividends_msk_data.csv"
        elif name == "dividends_processed":
            file_name = "dividends_msk_processed_data.csv"
        elif name == "trade_history":
            file_name = "trade_history_msk_data.csv"
        else:
            return ""

        return os.path.join(self.data_dir, secid, file_name)

    def _call_meta_loader(self, loader, name) -> bool:
        full_path = self.get_meta_file_path(name)
        if full_path == "":
            logging.error("Loader: failed name %s for meta_load", name)
            return False
        if not os.path.exists(full_path):
            loader_params = LoaderParams(self.engine, self.market, self.board, "")
            loader_obj = loader(loader_params)
            return loader_obj.load_meta(full_path)

        return True

    def load_meta(self) -> bool:
        if not self._call_meta_loader(SecuritiesListLoader, "securities"):
            return False

        if not self._call_meta_loader(DividendsLoader, "dividends"):
            return False

        if not self._call_meta_loader(TradeHistory, "trade_history"):
            return False

        if not self._call_meta_loader(MarketdataLoader, "marketdata"):
            return False

        return True

    def _call_data_loader(self, loader, name, secid) -> bool:
        if secid != "":
            full_path = self.get_data_file_path(name, secid)
        else:
            full_path = self.get_base_file_path(name)

        if full_path == "":
            logging.error("Loader: failed name %s for base_load or data_load", name)
            return False
        if not os.path.exists(full_path):
            loader_params = LoaderParams(self.engine, self.market, self.board, secid)
            loader_obj = loader(loader_params)
            return loader_obj.load_data(full_path)

        return True

    def load_base(self) -> bool:
        if not self._call_data_loader(SecuritiesListLoader, "securities", ""):
            return False

        if not self._call_data_loader(MarketdataLoader, "marketdata", ""):
            return False

        return True

    def _data_preprocess(self, secid) -> bool:
        file_path_out = self.get_data_file_path("dividends_processed", secid)
        if file_path_out == "":
            logging.error("Loader: failed name %s for data process", "dividends_processed")
            return False
        if os.path.exists(file_path_out):
            return True

        file_path = self.get_data_file_path("dividends", secid)
        divs = pd.read_csv(file_path, sep=";", parse_dates=['registryclosedate'], infer_datetime_format=True)
        divs = divs.sort_values(by="registryclosedate", ascending=True)

        file_path = self.get_data_file_path("trade_history", secid)
        hist = pd.read_csv(file_path, sep=";", parse_dates=["TRADEDATE"], infer_datetime_format=True)
        hist["t2date"] = hist["TRADEDATE"].shift(-2, fill_value=pd.Timestamp(2099, 1, 1, 12))
        hist = hist.sort_values(by="TRADEDATE", ascending=True)

        column_names = ["secid", "TRADEDATE", "registryclosedate", "value", "LEGALCLOSEPRICE", "interest_income", "currencyid"]
        if divs.empty:
            divs_full = pd.DataFrame(columns = column_names)
        else:
            divs_full = pd.merge_asof(divs, hist, left_on="registryclosedate", right_on="t2date")
            divs_full["interest_income"] = divs_full["value"]*100.0/divs_full["LEGALCLOSEPRICE"]
        divs_full = divs_full[column_names].rename(columns={"TRADEDATE": "t2date", "LEGALCLOSEPRICE": "close_price"})
        divs_full.to_csv(file_path_out, sep=";", encoding="utf-8")

        return True

    def load_data(self, securities_list) -> bool:
        for secid in securities_list:
            security_dir = os.path.join(self.data_dir, secid)
            if not os.path.exists(security_dir):
                os.mkdir(security_dir)

            if not self._call_data_loader(DividendsLoader, "dividends", secid):
                return False

            if not self._call_data_loader(TradeHistory, "trade_history", secid):
                return False

            if not self._data_preprocess(secid):
                return False

        return True
