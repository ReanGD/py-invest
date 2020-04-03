import os
from msk_api.load import load_securities


def root():
    return os.path.dirname(os.path.abspath(__file__))

def run():
    load_securities(os.path.join(root(), "data", "securities.scv"))

if __name__ == "__main__":
    run()
