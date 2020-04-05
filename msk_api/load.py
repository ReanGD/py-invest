import os
import logging
import requests
from msk_api.base_data_loader import BaseDataLoader

# https://iss.moex.com/iss/engines
# engine = "stock"
# https://iss.moex.com/iss/engines/stock/markets
# market = "shares"
# https://iss.moex.com/iss/engines/stock/markets/shares/boards
# board = "TQBR"
# https://iss.moex.com/iss/securitygroups
# group = "stock_shares"


class LoaderParams:
    def __init__(self, engine, market, board, security_name):
        self.engine = engine
        self.market = market
        self.board = board
        self.security_name = security_name


class SecuritiesListLoader(BaseDataLoader):
    def __init__(self, params):
        super(SecuritiesListLoader, self).__init__("SecuritiesListLoader")
        self.params = params

    def load_data(self, save_path) -> bool:
        # doc: http://iss.moex.com/iss/reference/5
        # example: http://iss.moex.com/iss/securities.json?lang=ru&start=0&limit=100
        start = 0
        count = 100
        while not self.is_finish:
            url_params = {
                "lang": "ru",
                "start": start,
                "limit": count,
            }
            if not self._load_data_page("http://iss.moex.com/iss/securities.csv", url_params, "securities"):
                return False
            start += count

        return self.save_data(save_path)

    def load_meta(self, save_path) -> bool:
        # example: http://iss.moex.com/iss/securities/column.json?iss.only=boards
        url_params = {
            "iss.only": "boards",
        }
        return self._load_meta("http://iss.moex.com/iss/securities/column.json", url_params, save_path)


class MarketdataLoader(BaseDataLoader):
    def __init__(self, params):
        super(MarketdataLoader, self).__init__("MarketdataLoader")
        self.params = params

    def load_data(self, save_path) -> bool:
        # doc: https://iss.moex.com/iss/reference/32
        # example: http://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities?iss.only=marketdata
        url_params = {
            "iss.only": "marketdata",
        }
        url = "http://iss.moex.com/iss/engines/{}/markets/{}/boards/{}/securities.csv"
        url = url.format(self.params.engine, self.params.market, self.params.board)
        if not self._load_data_page(url, url_params, "marketdata"):
            return False

        return self.save_data(save_path)

    def load_meta(self, save_path) -> bool:
        # example: http://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/columns?iss.only=marketdata
        url_params = {
            "iss.only": "marketdata",
        }
        url = "http://iss.moex.com/iss/engines/{}/markets/{}/boards/{}/securities/columns.json"
        url = url.format(self.params.engine, self.params.market, self.params.board)
        return self._load_meta(url, url_params, save_path)


class DividendsLoader(BaseDataLoader):
    def __init__(self, params):
        super(DividendsLoader, self).__init__("DividendsLoader")
        self.params = params

    def load_data(self, save_path) -> bool:
        # example: http://iss.moex.com/iss/securities/ROSN/dividends.json
        url_params = {}
        url = "http://iss.moex.com/iss/securities/{}/dividends.csv".format(self.params.security_name)
        if not self._load_data_page(url, url_params, "dividends"):
            return False

        return self.save_data(save_path)

    def load_meta(self, save_path) -> bool:
        # example: http://iss.moex.com/iss/securities/TATN/dividends.json?iss.data=off
        url_params = {
            "iss.data": "off",
        }
        return self._load_meta("http://iss.moex.com/iss/securities/TATN/dividends.json", url_params, save_path)


class TradeHistory(BaseDataLoader):
    def __init__(self, params):
        super(TradeHistory, self).__init__("TradeHistory")
        self.params = params

    def load_data(self, save_path) -> bool:
        # doc: http://iss.moex.com/iss/reference/65
        # example: http://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/TATN?from=2020-01-01&lang=ru&start=0&limit=100
        start = 0
        count = 100
        url = "http://iss.moex.com/iss/history/engines/{}/markets/{}/boards/{}/securities/{}.csv"
        url = url.format(self.params.engine, self.params.market, self.params.board, self.params.security_name)
        while not self.is_finish:
            url_params = {
                "from": "2010-01-01",
                "lang": "ru",
                "start": start,
                "limit": count,
            }
            if not self._load_data_page(url, url_params, "history"):
                return False
            start += count

        return self.save_data(save_path)

    def load_meta(self, save_path) -> bool:
        # doc: https://iss.moex.com/iss/reference/101
        # example: iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/columns.json?iss.only=boards
        url_params = {
            "iss.only": "boards",
        }
        url = "http://iss.moex.com/iss/history/engines/{}/markets/{}/boards/{}/securities/columns.json"
        url = url.format(self.params.engine, self.params.market, self.params.board)
        return self._load_meta(url, url_params, save_path)


class Loader:
    def __init__(self, engine, market, board, root_dir):
        self.engine = engine
        self.market = market
        self.board = board

        self.data_dir = os.path.join(root_dir, "data")
        if not os.path.exists(self.data_dir):
            os.mkdir(self.data_dir)

    def _call_meta_loader(self, loader, file_path) -> bool:
        full_path = os.path.join(self.meta_dir, file_path)
        if not os.path.exists(full_path):
            loader_params = LoaderParams(self.engine, self.market, self.board, "")
            loader_obj = loader(loader_params)
            return loader_obj.load_meta(full_path)

        return True

    def load_meta(self) -> bool:
        self.meta_dir = os.path.join(self.data_dir, "meta")
        if not os.path.exists(self.meta_dir):
            os.mkdir(self.meta_dir)

        if not self._call_meta_loader(SecuritiesListLoader, "securities_msk_column.json"):
            return False

        if not self._call_meta_loader(DividendsLoader, "dividends_msk_column.json"):
            return False

        if not self._call_meta_loader(DividendsLoader, "trade_history_msk_column.json"):
            return False

        if not self._call_meta_loader(MarketdataLoader, "marketdata_msk_column.json"):
            return False

        return True

    def _call_data_loader(self, loader, file_path, security_name) -> bool:
        if security_name != "":
            full_path = os.path.join(self.data_dir, security_name, file_path)
        else:
            full_path = os.path.join(self.data_dir, file_path)

        if not os.path.exists(full_path):
            loader_params = LoaderParams(self.engine, self.market, self.board, security_name)
            loader_obj = loader(loader_params)
            return loader_obj.load_data(full_path)

        return True

    def load_base(self) -> bool:
        if not self._call_data_loader(SecuritiesListLoader, "securities_msk_data.csv", ""):
            return False

        if not self._call_data_loader(MarketdataLoader, "marketdata_msk_data.csv", ""):
            return False

        return True

    def load_data(self, securities_list) -> bool:
        for security_name in securities_list:
            security_dir = os.path.join(self.data_dir, security_name)
            if not os.path.exists(security_dir):
                os.mkdir(security_dir)

            if not self._call_data_loader(DividendsLoader, "dividends_msk_data.csv", security_name):
                return False

            if not self._call_data_loader(TradeHistory, "trade_history_msk_data.csv", security_name):
                return False

        return True
