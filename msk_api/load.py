import os
import logging
import requests

# https://iss.moex.com/iss/engines
# engine = "stock"
# https://iss.moex.com/iss/engines/stock/markets
# market = "shares",
# https://iss.moex.com/iss/securitygroups
# group = "stock_shares",

class SecuritiesLoader:
    def __init__(self):
        self.header = ""
        self.data = []
        self.is_finish = False

    def parse_page(self, r, start, count) -> bool:
        lines = r.text.splitlines()

        if lines[0] != "securities":
            logging.error("SecuritiesLoader: failed parse page: %s, reason: header (%s) is wrong", r.url, lines[0])
            return False

        if lines[1] != "":
            logging.error("SecuritiesLoader: failed parse page: %s, reason: second line (%s) not empty", r.url, lines[1])
            return False

        if start == 0:
            self.header = lines[2]

        if lines[3] == "":
            self.is_finish = True
            return True

        for line in lines[3:]:
            if line != "":
                self.data.append(line)

        return True

    def load_page(self, start, count) -> bool:
        # doc: http://iss.moex.com/iss/reference/5
        # example: http://iss.moex.com/iss/securities.json?lang=ru&start=0&limit=100
        params = {
            "lang": "ru",
            "start": start,
            "limit": count,
        }
        r = requests.get("http://iss.moex.com/iss/securities.csv", params=params)
        if r.status_code != 200:
            logging.error("SecuritiesLoader: failed load page: %s, status code = %d, reason: %s", r.url, r.status_code, r.reason)
            return False

        return self.parse_page(r, start, count)

    def load_data(self, save_path) -> bool:
        self.header = ""
        self.data = []
        self.is_finish = False
        start = 0
        count = 100
        while not self.is_finish:
            if not self.load_page(start, count):
                return False
            start += count

        with open(save_path, "w") as f:
            f.write(self.header + "\n")
            f.writelines("%s\n" % line for line in self.data)

        return True

    def load_meta(self, save_path) -> bool:
        # example: http://iss.moex.com/iss/securities/column.json?iss.only=boards
        params = {
            "iss.only": "boards",
        }
        r = requests.get("http://iss.moex.com/iss/securities/column.json", params=params)
        if r.status_code != 200:
            logging.error("SecuritiesLoader: failed load page: %s, status code = %d, reason: %s", r.url, r.status_code, r.reason)
            return False

        with open(save_path, "w") as f:
            f.write(r.text)

        return True


class Loader:
    def __init__(self, root_dir):
        self.data_dir = os.path.join(root_dir, "data")
        if not os.path.exists(self.data_dir):
            os.mkdir(self.data_dir)

        self.meta_dir = os.path.join(self.data_dir, "meta")
        if not os.path.exists(self.meta_dir):
            os.mkdir(self.meta_dir)

    def load(self) -> bool:
        securities_data_file = os.path.join(self.data_dir, "securities_msk_data.csv")
        if not os.path.exists(securities_data_file):
            loader = SecuritiesLoader()
            if not loader.load_data(securities_data_file):
                return False

        securities_meta_file = os.path.join(self.meta_dir, "securities_msk_column.json")
        if not os.path.exists(securities_meta_file):
            loader = SecuritiesLoader()
            if not loader.load_meta(securities_meta_file):
                return False

        return True
