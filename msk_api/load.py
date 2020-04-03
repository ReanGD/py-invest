import os
import logging
import requests

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
        # https://iss.moex.com/iss/securities.json?engine=stock&market=shares&group_by=group&group_by_filter=stock_shares&lang=ru&start=0&limit=100
        params = {
            # https://iss.moex.com/iss/engines
            # "engine": "stock",
            # https://iss.moex.com/iss/engines/stock/markets
            # "market": "shares",
            # "group_by": "group",
            # https://iss.moex.com/iss/securitygroups
            # "group_by_filter": "stock_shares",
            "lang": "ru",
            "start": start,
            "limit": count,
        }
        # https://iss.moex.com/iss/reference/5
        r = requests.get("https://iss.moex.com/iss/securities.csv", params=params)
        if r.status_code != 200:
            logging.error("SecuritiesLoader: failed load page: %s, status code = %d, reason: %s", r.url, r.status_code, r.reason)
            return False

        return self.parse_page(r, start, count)

    def load(self, save_path):
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


def load_securities(save_path):
    loader = SecuritiesLoader()
    if not loader.load(save_path):
        print("fail load")
    else:
        print("load success")
