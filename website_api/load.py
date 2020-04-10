import os
import logging
import pandas as pd
from storage import FStruct, FStorage, DIVIDENDS, TRADE_HISTORY, DIVIDENDS_PROCESSED
from website_api.moex_loaders import LoaderParams, SecuritiesListLoader, MarketdataLoader, DividendsLoader, TradeHistory

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
        self.fstorage = FStorage(fstruct.get_root_dir())
        self.fstruct.make_base_dir()

    def _call_meta_loader(self, loader):
        full_path = self.fstruct.meta_file_path(loader.name_id)
        if not os.path.exists(full_path):
            loader_params = LoaderParams(self.engine, self.market, self.board, "")
            loader_obj = loader(loader_params)
            loader_obj.load_meta(full_path)

    def load_meta(self):
        self._call_meta_loader(SecuritiesListLoader)
        self._call_meta_loader(DividendsLoader)
        self._call_meta_loader(TradeHistory)
        self._call_meta_loader(MarketdataLoader)
        logging.info("Finish loading meta")

    def _call_data_loader(self, loader, sec_id : str = None):
        full_path = self.fstruct.data_file_path(loader.name_id, sec_id)
        if not os.path.exists(full_path):
            loader_params = LoaderParams(self.engine, self.market, self.board, sec_id)
            loader_obj = loader(loader_params)
            loader_obj.load_data(full_path)

    def load_base(self):
        self._call_data_loader(SecuritiesListLoader)
        self._call_data_loader(MarketdataLoader)
        logging.info("Finish loading base")

    def _data_preprocess(self, sec_id : str):
        file_path_out = self.fstruct.data_file_path(DIVIDENDS_PROCESSED, sec_id)
        if os.path.exists(file_path_out):
            return

        divs = self.fstorage.open_data(DIVIDENDS, sec_id).sort_values(by="registryclosedate", ascending=True)
        hist = self.fstorage.open_data(TRADE_HISTORY, sec_id).sort_values(by="TRADEDATE", ascending=True)
        hist["t2date"] = hist["TRADEDATE"].shift(-2, fill_value=pd.Timestamp(2099, 1, 1))

        column_names = ["secid", "TRADEDATE", "registryclosedate", "value", "LEGALCLOSEPRICE", "interest_income", "currencyid"]
        if divs.empty:
            divs_full = pd.DataFrame(columns = column_names)
        else:
            divs_full = pd.merge_asof(divs, hist, left_on="registryclosedate", right_on="t2date")
            divs_full["interest_income"] = divs_full["value"] * 100.0 / divs_full["LEGALCLOSEPRICE"]
        divs_full = divs_full[column_names].rename(columns={"TRADEDATE": "t2date", "LEGALCLOSEPRICE": "close_price"})
        divs_full.to_csv(file_path_out, sep=";", encoding="utf-8")

    def load_data(self, securities_list):
        for sec_id in securities_list:
            self.fstruct.make_sec_dir(sec_id)
            self._call_data_loader(DividendsLoader, sec_id)
            self._call_data_loader(TradeHistory, sec_id)
            self._data_preprocess(sec_id)
            logging.info("Finish loading security: %s", sec_id)
        logging.info("Finish loading securities")
