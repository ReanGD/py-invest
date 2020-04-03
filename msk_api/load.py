import os
import logging
import requests
from msk_api.base_data_loader import BaseDataLoader

# https://iss.moex.com/iss/securitygroups
# group = "stock_shares"

class SecuritiesListLoader(BaseDataLoader):
    def __init__(self):
        super(SecuritiesListLoader, self).__init__("SecuritiesListLoader")

    def load_data(self, save_path) -> bool:
        # doc: http://iss.moex.com/iss/reference/5
        # example: http://iss.moex.com/iss/securities.json?lang=ru&start=0&limit=100
        start = 0
        count = 100
        while not self.is_finish:
            params = {
                "lang": "ru",
                "start": start,
                "limit": count,
            }
            if not self.load_page("http://iss.moex.com/iss/securities.csv", params, "securities"):
                return False
            start += count

        return self.save_data(save_path)

    def load_meta(self, save_path) -> bool:
        # example: http://iss.moex.com/iss/securities/column.json?iss.only=boards
        params = {
            "iss.only": "boards",
        }
        r = requests.get("http://iss.moex.com/iss/securities/column.json", params=params)
        if r.status_code != 200:
            logging.error("SecuritiesListLoader: failed load page: %s, status code = %d, reason: %s", r.url, r.status_code, r.reason)
            return False

        with open(save_path, "w") as f:
            f.write(r.text)

        return True


class DividendsLoader(BaseDataLoader):
    def __init__(self):
        super(DividendsLoader, self).__init__("DividendsLoader")

    def load_data(self, security_name, save_path) -> bool:
        # example: http://iss.moex.com/iss/securities/ROSN/dividends.json
        params = {}
        url = "http://iss.moex.com/iss/securities/{}/dividends.csv".format(security_name)
        if not self.load_page(url, params, "dividends"):
            return False

        return self.save_data(save_path)

    def load_meta(self, save_path) -> bool:
        # example: http://iss.moex.com/iss/securities/TATN/dividends.json?iss.data=off
        params = {
            "iss.data": "off",
        }
        r = requests.get("http://iss.moex.com/iss/securities/TATN/dividends.json", params=params)
        if r.status_code != 200:
            logging.error("DividendsLoader: failed load page: %s, status code = %d, reason: %s", r.url, r.status_code, r.reason)
            return False

        with open(save_path, "w") as f:
            f.write(r.text)

        return True


class TradeHistory(BaseDataLoader):
    def __init__(self):
        super(TradeHistory, self).__init__("TradeHistory")

    def load_data(self, engine, market, board, security_name, save_path) -> bool:
        # doc: http://iss.moex.com/iss/reference/65
        # example: http://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/TATN?from=2020-01-01&lang=ru&start=0&limit=100
        start = 0
        count = 100
        url = "http://iss.moex.com/iss/history/engines/{}/markets/{}/boards/{}/securities/{}.csv".format(engine, market, board, security_name)
        while not self.is_finish:
            params = {
                "from": "2010-01-01",
                "lang": "ru",
                "start": start,
                "limit": count,
            }
            if not self.load_page(url, params, "history"):
                return False
            start += count

        return self.save_data(save_path)

class Loader:
    def __init__(self, root_dir):
        self.data_dir = os.path.join(root_dir, "data")
        if not os.path.exists(self.data_dir):
            os.mkdir(self.data_dir)

        self.meta_dir = os.path.join(self.data_dir, "meta")
        if not os.path.exists(self.meta_dir):
            os.mkdir(self.meta_dir)

    def load_meta(self):
        securities_meta_file = os.path.join(self.meta_dir, "securities_msk_column.json")
        if not os.path.exists(securities_meta_file):
            loader = SecuritiesListLoader()
            if not loader.load_meta(securities_meta_file):
                return False

        dividends_meta_file = os.path.join(self.meta_dir, "dividends_msk_column.json")
        if not os.path.exists(dividends_meta_file):
            loader = DividendsLoader()
            if not loader.load_meta(dividends_meta_file):
                return False

        return True

    def load(self, securities_list) -> bool:
        # https://iss.moex.com/iss/engines
        engine = "stock"
        # https://iss.moex.com/iss/engines/stock/markets
        market = "shares"
        # https://iss.moex.com/iss/engines/stock/markets/shares/boards
        board = "TQBR"

        if not self.load_meta():
            return False

        securities_data_file = os.path.join(self.data_dir, "securities_msk_data.csv")
        if not os.path.exists(securities_data_file):
            loader = SecuritiesListLoader()
            if not loader.load_data(securities_data_file):
                return False

        for security_name in securities_list:
            security_dir = os.path.join(self.data_dir, security_name)
            if not os.path.exists(security_dir):
                os.mkdir(security_dir)

            security_dividends_file = os.path.join(security_dir, "dividends_msk_data.csv")
            if not os.path.exists(security_dividends_file):
                loader = DividendsLoader()
                if not loader.load_data(security_name, security_dividends_file):
                    return False

            security_trade_history_file = os.path.join(security_dir, "trade_history_msk_data.csv")
            if not os.path.exists(security_trade_history_file):
                loader = TradeHistory()
                if not loader.load_data(engine, market, board, security_name, security_trade_history_file):
                    return False

        return True
