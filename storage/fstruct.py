import os
from storage import SECURITIES, DIVIDENDS, TRADE_HISTORY, MARKETDATA, DIVIDENDS_PROCESSED


class FStruct:
    def __init__(self, root_dir : str):
        self.data_dir = os.path.join(root_dir, "data")
        if not os.path.exists(self.data_dir):
            os.mkdir(self.data_dir)

        self.meta_dir = os.path.join(self.data_dir, "meta")
        if not os.path.exists(self.meta_dir):
            os.mkdir(self.meta_dir)

    def make_secid_dir(self, secid : str):
        security_dir = os.path.join(self.data_dir, secid)
        if not os.path.exists(security_dir):
            os.mkdir(security_dir)

    def meta_file_path(self, name : str) -> str:
        file_name = ""
        if name == SECURITIES:
            file_name = "moex_securities_column.json"
        elif name == DIVIDENDS:
            file_name = "moex_dividends_column.json"
        elif name == TRADE_HISTORY:
            file_name = "moex_trade_history_column.json"
        elif name == MARKETDATA:
            file_name = "moex_marketdata_column.json"
        else:
            return ""

        return os.path.join(self.meta_dir, file_name)

    def data_file_path(self, name : str, secid : str = None) -> str:
        if secid is not None:
            return self._data_file_path_secid(name, secid)

        file_name = ""
        if name == SECURITIES:
            file_name = "moex_securities_data.csv"
        elif name == MARKETDATA:
            file_name = "moex_marketdata_data.csv"
        else:
            return ""

        return os.path.join(self.data_dir, file_name)

    def _data_file_path_secid(self, name : str, secid : str) -> str:
        file_name = ""
        if name == DIVIDENDS:
            file_name = "moex_dividends_data.csv"
        elif name == DIVIDENDS_PROCESSED:
            file_name = "moex_dividends_data_processed.csv"
        elif name == TRADE_HISTORY:
            file_name = "moex_trade_history_data.csv"
        else:
            return ""

        return os.path.join(self.data_dir, secid, file_name)
