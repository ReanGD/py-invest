import os
import logging
import pandas as pd
from msk_api.load import Loader


def root():
    return os.path.dirname(os.path.abspath(__file__))

def run():
    logging.basicConfig(level=logging.INFO, format='%(name)s:[%(levelname)s]: %(message)s')

    engine = "stock"
    market = "shares"
    board = "TQBR"
    loader = Loader(engine, market, board, root())
    if not loader.load_meta():
        logging.error("Meta load finished with error")
        return

    if not loader.load_base():
        logging.error("Base load finished with error")
        return

    top = pd.read_csv(loader.get_base_file_path("marketdata"), sep=";").sort_values("VALTODAY_RUR", ascending = False).head(50)["SECID"]
    if not loader.load_data([name for name in top]):
        logging.error("Data load finished with error")
        return

    logging.info("Finished success")


if __name__ == "__main__":
    run()
