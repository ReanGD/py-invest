import logging
import requests


class BaseDataLoader:
    def __init__(self, class_name):
        self.class_name = class_name
        self.header = ""
        self.data = []

    def _load_data_page(self, url, params, header_name) -> bool:
        r = requests.get(url, params=params)
        if r.status_code != 200:
            raise Exception("{}: failed load page: {}, status code = {}, reason: {}".format(self.class_name, r.url, r.status_code, r.reason))

        lines = r.text.splitlines()

        if lines[0] != header_name:
            raise Exception("{}: failed parse page: {}, reason: header ({}) is wrong".format(self.class_name, r.url, lines[0]))

        if lines[1] != "":
            raise Exception("{}: failed parse page: {}, reason: second line ({}) not empty".format(self.class_name, r.url, lines[1]))

        if self.header == "":
            self.header = lines[2]

        if lines[3] == "":
            return True

        for line in lines[3:]:
            if line != "":
                self.data.append(line)

        return False

    def _save_data(self, save_path):
        with open(save_path, "w") as f:
            f.write(self.header + "\n")
            f.writelines("%s\n" % line for line in self.data)

    def _load_meta(self, url, params, save_path):
        r = requests.get(url, params=params)
        if r.status_code != 200:
            raise Exception("{}: failed load page: {}, status code = {}, reason: {}".format(self.class_name, r.url, r.status_code, r.reason))

        with open(save_path, "w") as f:
            f.write(r.text)
