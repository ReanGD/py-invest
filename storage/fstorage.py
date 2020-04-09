import os
import pandas as pd
from storage import FStruct, INFLATION, SECURITIES, DIVIDENDS, TRADE_HISTORY, MARKETDATA, DIVIDENDS_PROCESSED


class FStorage:
    def __init__(self, root_dir : str):
        self.fstruct = FStruct(root_dir)

    def open_data(self, name_id : str, sec_id : str = None, index_col = None) -> pd.DataFrame:
        if sec_id is not None:
            return self._open_data_by_sec_id(name_id, sec_id, index_col)

        file_path = self.fstruct.data_file_path(name_id, sec_id)
        if not os.path.exists(file_path):
            raise Exception("not found file for load name id {}".format(name_id))

        if name_id == INFLATION:
            return pd.read_csv(file_path, sep=";", parse_dates=["month"], infer_datetime_format=True, index_col=index_col)
        elif name_id == SECURITIES:
            return pd.read_csv(file_path, sep=";", index_col=index_col)
        elif name_id == MARKETDATA:
            return pd.read_csv(file_path, sep=";", parse_dates=["UPDATETIME"], infer_datetime_format=True, index_col=index_col)
        else:
            raise Exception("not found name id {} for load data file".format(name_id))

    def _open_data_by_sec_id(self, name_id : str, sec_id : str, index_col) -> pd.DataFrame:
        file_path = self.fstruct.data_file_path(name_id, sec_id)
        if not os.path.exists(file_path):
            raise Exception("not found file for load name id {} (sec id = {})".format(name_id, sec_id))

        if name_id == DIVIDENDS:
            return pd.read_csv(file_path, sep=";", parse_dates=["registryclosedate"], infer_datetime_format=True, index_col=index_col)
        elif name_id == DIVIDENDS_PROCESSED:
            return pd.read_csv(file_path, sep=";", parse_dates=["t2date", "registryclosedate"], infer_datetime_format=True, index_col=index_col)
        elif name_id == TRADE_HISTORY:
            return pd.read_csv(file_path, sep=";", parse_dates=["TRADEDATE"], infer_datetime_format=True, index_col=index_col)
        else:
            raise Exception("not found name id {} for load data file (sec id = {})".format(name_id, sec_id))
