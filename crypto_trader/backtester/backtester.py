import pandas as pd
import numpy as np
from crypto_trader.backtester.strategies import *


def run_backtest(prices, strategy):
    """
    Run a backtest on a trading strategy using historical price data.

    Parameters:
    - prices: A pandas DataFrame containing historical price data with the following columns:
        - timestamp: The timestamp of each data point.
        - open: The opening price of the asset.
        - high: The highest price of the asset.
        - low: The lowest price of the asset.
        - close: The closing price of the asset.
        - volume: The trading volume of the asset.
    - strategy: A function that takes the historical price data as input and returns a pandas DataFrame with the following columns:
        - timestamp: The timestamp of each trading signal.
        - position: The position to take in the asset at each trading signal (-1 for short, 0 for neutral, 1 for long).

    Returns:
    A pandas DataFrame with the following columns:
    - timestamp: The timestamp of each trade.
    - trade_type: The type of trade ('buy' or 'sell').
    - trade_size: The size of the trade in units of the asset.
    - entry_price: The price at which the trade was entered.
    - exit_price: The price at which the trade was exited.
    - pnl: The profit or loss of the trade in units of the quote currency.
    - cumulative_pnl: The cumulative profit or loss of the strategy over time in units of the quote currency.
    """
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
