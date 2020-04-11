import pandas as pd
from analitics.inflation import Inflation


class Profit:
    def __init__(self, tax : float, broker_commission : float, inflation : Inflation):
        self._tax = tax
        self._broker_commission = broker_commission
        self._inflation = inflation

    # return tax + broker commission
    def costs(self, buy_date : pd.Timestamp, buy_price : float, sale_date : pd.Timestamp, sale_price : float, dividends : float) -> float:
        commission_value = (buy_price + sale_price) * self._broker_commission
        sale_revenue = sale_price - buy_price - commission_value
        tax_base = dividends if (sale_revenue <= 0.0) or (buy_date + pd.DateOffset(years=3) < sale_date) else sale_revenue + dividends
        return tax_base * self._tax + commission_value

    # include: dividends
    # exclude: tax, broker commission
    # not adjusted for inflation
    # return: percent per year
    def nominal_rate(self, buy_date : pd.Timestamp, buy_price : float, sale_date : pd.Timestamp, sale_price : float, dividends : float) -> float:
        return (dividends + sale_price - buy_price) * 365.25 * 100.0 / (buy_price * (sale_date - buy_date).days)

    # include: dividends, tax, broker commission
    # not adjusted for inflation
    # return: percent per year
    def effective_rate(self, buy_date : pd.Timestamp, buy_price : float, sale_date : pd.Timestamp, sale_price : float, dividends : float) -> float:
        costs = self.costs(buy_date, buy_price, sale_date, sale_price, dividends)
        return (dividends + sale_price - buy_price - costs) * 365.25 * 100.0 / (buy_price * (sale_date - buy_date).days)

    # include: dividends, tax, broker commission
    # return: percent per year (above inflation)
    def real_rate(self, buy_date : pd.Timestamp, buy_price : float, sale_date : pd.Timestamp, sale_price : float, dividends : float) -> float:
        effective_rate = self.effective_rate(buy_date, buy_price, sale_date, sale_price, dividends)
        return self._inflation.get_rate_without_inflation(effective_rate, buy_date, sale_date)
