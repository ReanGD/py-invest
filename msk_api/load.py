import os
import logging
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

    def get_data_file_path(self, name, security_name) -> str:
        file_name = ""
        if name == "dividends":
            file_name = "dividends_msk_data.csv"
        elif name == "trade_history":
            file_name = "trade_history_msk_data.csv"
        else:
            return ""

        return os.path.join(self.data_dir, security_name, file_name)

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

        if not self._call_meta_loader(DividendsLoader, "trade_history"):
            return False

        if not self._call_meta_loader(MarketdataLoader, "marketdata"):
            return False

        return True

    def _call_data_loader(self, loader, name, security_name) -> bool:
        if security_name != "":
            full_path = self.get_data_file_path(name, security_name)
        else:
            full_path = self.get_base_file_path(name)

        if full_path == "":
            logging.error("Loader: failed name %s for base_load or data_load", name)
            return False
        if not os.path.exists(full_path):
            loader_params = LoaderParams(self.engine, self.market, self.board, security_name)
            loader_obj = loader(loader_params)
            return loader_obj.load_data(full_path)

        return True

    def load_base(self) -> bool:
        if not self._call_data_loader(SecuritiesListLoader, "securities", ""):
            return False

        if not self._call_data_loader(MarketdataLoader, "marketdata", ""):
            return False

        return True

    def load_data(self, securities_list) -> bool:
        for security_name in securities_list:
            security_dir = os.path.join(self.data_dir, security_name)
            if not os.path.exists(security_dir):
                os.mkdir(security_dir)

            if not self._call_data_loader(DividendsLoader, "dividends", security_name):
                return False

            if not self._call_data_loader(TradeHistory, "trade_history", security_name):
                return False

        return True
