from website_api.base_data_loader import BaseDataLoader
from storage import SECURITIES, DIVIDENDS, TRADE_HISTORY, MARKETDATA


class LoaderParams:
    def __init__(self, engine, market, board, sec_id):
        self.engine = engine
        self.market = market
        self.board = board
        self.sec_id = sec_id


class SecuritiesListLoader(BaseDataLoader):
    name_id = SECURITIES

    def __init__(self, params):
        super(SecuritiesListLoader, self).__init__("SecuritiesListLoader")
        self.params = params

    def load_data(self, save_path):
        # doc: http://iss.moex.com/iss/reference/5
        # example: http://iss.moex.com/iss/securities.json?lang=ru&start=0&limit=100
        start = 0
        count = 100
        while True:
            url_params = {
                "lang": "ru",
                "start": start,
                "limit": count,
            }
            start += count
            if self._load_data_page("http://iss.moex.com/iss/securities.csv", url_params, "securities"):
                break

        self._save_data(save_path)

    def load_meta(self, save_path):
        # example: http://iss.moex.com/iss/securities/column.json?iss.only=boards
        url_params = {
            "iss.only": "boards",
        }
        self._load_meta("http://iss.moex.com/iss/securities/column.json", url_params, save_path)


class MarketdataLoader(BaseDataLoader):
    name_id = MARKETDATA

    def __init__(self, params):
        super(MarketdataLoader, self).__init__("MarketdataLoader")
        self.params = params

    def load_data(self, save_path):
        # doc: https://iss.moex.com/iss/reference/32
        # example: http://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities?iss.only=marketdata
        url_params = {
            "iss.only": "marketdata",
        }
        url = "http://iss.moex.com/iss/engines/{}/markets/{}/boards/{}/securities.csv"
        url = url.format(self.params.engine, self.params.market, self.params.board)
        self._load_data_page(url, url_params, "marketdata")
        self._save_data(save_path)

    def load_meta(self, save_path):
        # example: http://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/columns?iss.only=marketdata
        url_params = {
            "iss.only": "marketdata",
        }
        url = "http://iss.moex.com/iss/engines/{}/markets/{}/boards/{}/securities/columns.json"
        url = url.format(self.params.engine, self.params.market, self.params.board)
        self._load_meta(url, url_params, save_path)


class DividendsLoader(BaseDataLoader):
    name_id = DIVIDENDS

    def __init__(self, params):
        super(DividendsLoader, self).__init__("DividendsLoader")
        self.params = params

    def load_data(self, save_path):
        # example: http://iss.moex.com/iss/securities/ROSN/dividends.json
        url_params = {}
        url = "http://iss.moex.com/iss/securities/{}/dividends.csv".format(self.params.sec_id)
        self._load_data_page(url, url_params, "dividends")
        self._save_data(save_path)

    def load_meta(self, save_path):
        # example: http://iss.moex.com/iss/securities/TATN/dividends.json?iss.data=off
        url_params = {
            "iss.data": "off",
        }
        self._load_meta("http://iss.moex.com/iss/securities/TATN/dividends.json", url_params, save_path)


class TradeHistory(BaseDataLoader):
    name_id = TRADE_HISTORY

    def __init__(self, params):
        super(TradeHistory, self).__init__("TradeHistory")
        self.params = params

    def load_data(self, save_path):
        # doc: http://iss.moex.com/iss/reference/65
        # example: http://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/TATN?from=2020-01-01&lang=ru&start=0&limit=100
        start = 0
        count = 100
        url = "http://iss.moex.com/iss/history/engines/{}/markets/{}/boards/{}/securities/{}.csv"
        url = url.format(self.params.engine, self.params.market, self.params.board, self.params.sec_id)
        while True:
            url_params = {
                "from": "2010-01-01",
                "lang": "ru",
                "start": start,
                "limit": count,
            }
            if self._load_data_page(url, url_params, "history"):
                break
            start += count

        self._save_data(save_path)

    def load_meta(self, save_path):
        # doc: https://iss.moex.com/iss/reference/101
        # example: http://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/columns.json?iss.meta=off
        url_params = {
            "iss.meta": "off",
        }
        url = "http://iss.moex.com/iss/history/engines/{}/markets/{}/boards/{}/securities/columns.json"
        url = url.format(self.params.engine, self.params.market, self.params.board)
        self._load_meta(url, url_params, save_path)
