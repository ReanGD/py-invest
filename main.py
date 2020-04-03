import os
from msk_api.load import Loader


def root():
    return os.path.dirname(os.path.abspath(__file__))

def run():
    loader = Loader(root())
    if loader.load():
        print("Finished success")
    else:
        print("Finished with error")


if __name__ == "__main__":
    run()
