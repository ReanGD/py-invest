import os
import logging
import requests


class BaseDataLoader:
    def __init__(self, class_name):
        self.class_name = class_name
        self.header = ""
        self.data = []
        self.is_finish = False

    def parse_page(self, r, header_name) -> bool:
        lines = r.text.splitlines()

        if lines[0] != header_name:
            logging.error("%s: failed parse page: %s, reason: header (%s) is wrong", self.class_name, r.url, lines[0])
            return False

        if lines[1] != "":
            logging.error("%s: failed parse page: %s, reason: second line (%s) not empty", self.class_name, r.url, lines[1])
            return False

        if self.header == "":
            self.header = lines[2]

        if lines[3] == "":
            self.is_finish = True
            return True

        for line in lines[3:]:
            if line != "":
                self.data.append(line)

        return True

    def save_data(self, save_path) -> bool:
        with open(save_path, "w") as f:
            f.write(self.header + "\n")
            f.writelines("%s\n" % line for line in self.data)
