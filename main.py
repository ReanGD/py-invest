import os
import logging
import pandas as pd
from storage import FStruct, MARKETDATA
from msk_api.load import Loader


def run():
    logging.basicConfig(level=logging.INFO, format='%(name)s:[%(levelname)s]: %(message)s')

    engine = "stock"
    market = "shares"
    board = "TQBR"
    root_dir = os.path.dirname(os.path.abspath(__file__))
    fstruct = FStruct(root_dir)
    loader = Loader(engine, market, board, fstruct)
    if not loader.load_meta():
        logging.error("Meta load finished with error")
        return

    if not loader.load_base():
        logging.error("Base load finished with error")
        return

    file_path = fstruct.data_file_path(MARKETDATA)
    if file_path is None:
        return False
    top = pd.read_csv(file_path, sep=";").sort_values("VALTODAY_RUR", ascending = False).head(5)["SECID"]
    if not loader.load_data([name for name in top]):
        logging.error("Data load finished with error")
        return

    logging.info("Finished success")


if __name__ == "__main__":
    run()
