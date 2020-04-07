import os
import logging
import pandas as pd
from storage import FStruct, SECURITIES, DIVIDENDS, TRADE_HISTORY, MARKETDATA, DIVIDENDS_PROCESSED
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
    def __init__(self, engine : str, market : str, board : str, fstruct : FStruct):
        self.engine = engine
        self.market = market
        self.board = board
        self.fstruct = fstruct

    def _call_meta_loader(self, loader, name : str) -> bool:
        full_path = self.fstruct.meta_file_path(name)
        if full_path is None:
            return False

        if not os.path.exists(full_path):
            loader_params = LoaderParams(self.engine, self.market, self.board, "")
            loader_obj = loader(loader_params)
            return loader_obj.load_meta(full_path)

        return True

    def load_meta(self) -> bool:
        if not self._call_meta_loader(SecuritiesListLoader, SECURITIES):
            return False

        if not self._call_meta_loader(DividendsLoader, DIVIDENDS):
            return False

        if not self._call_meta_loader(TradeHistory, TRADE_HISTORY):
            return False

        if not self._call_meta_loader(MarketdataLoader, MARKETDATA):
            return False

        return True

    def _call_data_loader(self, loader, name : str, secid : str = None) -> bool:
        full_path = self.fstruct.data_file_path(name, secid)
        if full_path is None:
            return False

        if not os.path.exists(full_path):
            loader_params = LoaderParams(self.engine, self.market, self.board, secid)
            loader_obj = loader(loader_params)
            return loader_obj.load_data(full_path)

        return True

    def load_base(self) -> bool:
        if not self._call_data_loader(SecuritiesListLoader, SECURITIES):
            return False

        if not self._call_data_loader(MarketdataLoader, MARKETDATA):
            return False

        return True

    def _data_preprocess(self, secid : str) -> bool:
        file_path_out = self.fstruct.data_file_path(DIVIDENDS_PROCESSED, secid)
        if file_path_out is None:
            return False

        if os.path.exists(file_path_out):
            return True

        file_path = self.fstruct.data_file_path(DIVIDENDS, secid)
        if file_path is None:
            return False
        divs = pd.read_csv(file_path, sep=";", parse_dates=["registryclosedate"], infer_datetime_format=True)
        divs = divs.sort_values(by="registryclosedate", ascending=True)

        file_path = self.fstruct.data_file_path(TRADE_HISTORY, secid)
        if file_path is None:
            return False
        hist = pd.read_csv(file_path, sep=";", parse_dates=["TRADEDATE"], infer_datetime_format=True)
        hist["t2date"] = hist["TRADEDATE"].shift(-2, fill_value=pd.Timestamp(2099, 1, 1))
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
            self.fstruct.make_sec_dir(secid)

            if not self._call_data_loader(DividendsLoader, DIVIDENDS, secid):
                return False

            if not self._call_data_loader(TradeHistory, TRADE_HISTORY, secid):
                return False

            if not self._data_preprocess(secid):
                return False

        return True
