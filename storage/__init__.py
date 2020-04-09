INFLATION = "inflation"
SECURITIES = "securities"
DIVIDENDS = "dividends"
TRADE_HISTORY = "trade_history"
MARKETDATA = "marketdata"
DIVIDENDS_PROCESSED = "dividends_processed"

from storage.fstruct import FStruct
from storage.fstorage import FStorage

__all__ = [
    "INFLATION",
    "SECURITIES",
    "DIVIDENDS",
    "TRADE_HISTORY",
    "MARKETDATA",
    "DIVIDENDS_PROCESSED",
    "FStruct",
    "FStorage",
]
