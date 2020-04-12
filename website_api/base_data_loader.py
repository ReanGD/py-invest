import requests


class BaseDataLoader:
    def __init__(self, class_name : str):
        self.class_name = class_name
        self.header = ""
        self.data = []

    def _load_moex_csv(self, url : str, params : dict, header_name : str) -> bool:
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

    def _save_csv(self, save_path : str):
        with open(save_path, "w") as f:
            f.write(self.header + "\n")
            f.writelines("%s\n" % line for line in self.data)

    def _load_url(self, url : str, params : dict) -> str:
        r = requests.get(url, params=params)
        if r.status_code != 200:
            raise Exception("{}: failed load page: {}, status code = {}, reason: {}".format(self.class_name, r.url, r.status_code, r.reason))

        return r.text

    def _load_and_save_url(self, url : str, params : dict, save_path : str):
        text = self._load_url(url, params)
        with open(save_path, "w") as f:
            f.write(text)
