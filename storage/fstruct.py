import os
from storage import INFLATION, SECURITIES, DIVIDENDS, TRADE_HISTORY, MARKETDATA, DIVIDENDS_PROCESSED


class FStruct:
    def __init__(self, root_dir : str):
        self._root_dir = root_dir
        self._data_dir = os.path.join(root_dir, "data")
        self._meta_dir = os.path.join(self._data_dir, "meta")

    def get_root_dir(self):
        return self._root_dir

    def make_base_dir(self):
        if not os.path.exists(self._data_dir):
            os.mkdir(self._data_dir)
        if not os.path.exists(self._meta_dir):
            os.mkdir(self._meta_dir)

    def make_sec_dir(self, sec_id : str):
        security_dir = os.path.join(self._data_dir, sec_id)
        if not os.path.exists(security_dir):
            os.mkdir(security_dir)

    def meta_file_path(self, name_id : str) -> str:
        file_name = ""
        if name_id == SECURITIES:
            file_name = "moex_securities_column.json"
        elif name_id == DIVIDENDS:
            file_name = "moex_dividends_column.json"
        elif name_id == TRADE_HISTORY:
            file_name = "moex_trade_history_column.json"
        elif name_id == MARKETDATA:
            file_name = "moex_marketdata_column.json"
        else:
            raise Exception("not found name id {} for find meta path".format(name_id))

        return os.path.join(self._meta_dir, file_name)

    def data_file_path(self, name_id : str, sec_id : str = None) -> str:
        if sec_id is not None:
            return self._data_file_path_by_sec_id(name_id, sec_id)

        file_name = ""
        if name_id == INFLATION:
            file_name = "statbureau_inflation_data.csv"
        elif name_id == SECURITIES:
            file_name = "moex_securities_data.csv"
        elif name_id == MARKETDATA:
            file_name = "moex_marketdata_data.csv"
        else:
            raise Exception("not found name id {} for find data path".format(name_id))

        return os.path.join(self._data_dir, file_name)

    def _data_file_path_by_sec_id(self, name_id : str, sec_id : str) -> str:
        file_name = ""
        if name_id == DIVIDENDS:
            file_name = "moex_dividends_data.csv"
        elif name_id == DIVIDENDS_PROCESSED:
            file_name = "moex_dividends_data_processed.csv"
        elif name_id == TRADE_HISTORY:
            file_name = "moex_trade_history_data.csv"
        else:
            raise Exception("not found name id {} for find data path (sec id = {})".format(name_id, sec_id))

        return os.path.join(self._data_dir, sec_id, file_name)
