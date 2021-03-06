import os
import logging
import pandas as pd
from storage import FStruct, FStorage, MARKETDATA
from website_api.load import Loader


def load():
    engine = "stock"
    market = "shares"
    board = "TQBR"
    root_dir = os.path.dirname(os.path.abspath(__file__))
    fstruct = FStruct(root_dir)
    fstorage = FStorage(root_dir)
    loader = Loader(engine, market, board, fstruct)

    loader.load_meta()
    loader.load_base()

    top = fstorage.open_data(MARKETDATA).sort_values("VALTODAY_RUR", ascending = False).head(50)["SECID"]
    loader.load_data([name for name in top])

def run():
    logging.basicConfig(level=logging.INFO, format='%(name)s:[%(levelname)s]: %(message)s')

    try:
        load()
        logging.info("Finish success")
    except Exception:
        logging.exception("")


if __name__ == "__main__":
    run()
