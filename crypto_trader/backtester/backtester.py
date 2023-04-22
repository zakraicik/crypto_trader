import logging
import pandas as pd
import numpy as np
from crypto_trader.backtester.strategies import *

logger = logging.getLogger(__name__)


def run_backtest(prices, strategy):
    # Run the strategy on the price data
    signals = strategy(prices)

    # Merge the signals with the price data
    trades = pd.merge(prices, signals, on="timestamp")

    # Calculate the trade sizes based on the position and the available capital
    capital = 10000  # Starting capital
    trades["trade_size"] = np.where(
        trades["position"] != 0, capital * trades["position"], 0
    )

    # Calculate the trade entry and exit prices
    trades["entry_price"] = np.where(
        trades["position"].diff() != 0, trades["close"], np.nan
    )
    trades["entry_price"].fillna(method="ffill", inplace=True)
    trades["exit_price"] = trades["close"]

    # Calculate the trade profit and loss
    trades["pnl"] = (
        trades["trade_size"]
        * (trades["exit_price"] - trades["entry_price"])
        * np.where(trades["position"] == -1, -1, 1)
    )
    trades["cumulative_pnl"] = trades["pnl"].cumsum()

    # Create a DataFrame of trades
    trades = trades[trades["entry_price"].notnull()].reset_index()
    trades.rename(columns={"index": "timestamp"}, inplace=True)
    trades["trade_type"] = np.where(trades["position"] == 1, "buy", "sell")

    return trades[
        [
            "timestamp",
            "trade_type",
            "trade_size",
            "entry_price",
            "exit_price",
            "pnl",
            "cumulative_pnl",
        ]
    ]
