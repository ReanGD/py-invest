import os
from msk_api.load import Loader


def root():
    return os.path.dirname(os.path.abspath(__file__))

def run():
    loader = Loader(root())
    if not loader.load_meta():
        print("Meta load finished with error")
        return

    if not loader.load_base():
        print("Base load finished with error")
        return

    if not loader.load_data(["ROSN", "TATN", "TATNP"]):
        print("Data load finished with error")
        return

    print("Finished success")


if __name__ == "__main__":
    run()
