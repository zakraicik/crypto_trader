import pandas as pd
import numpy as np


def calculate_sma(data: pd.DataFrame, window: int) -> pd.Series:
    return data["close"].rolling(window=window).mean()


def sma_crossover_strategy(
    data: pd.DataFrame, short_window: int, long_window: int
) -> pd.DataFrame:
    short_sma = calculate_sma(data, short_window)
    long_sma = calculate_sma(data, long_window)

    signals = pd.DataFrame(index=data.index)
    signals["timestamp"] = data.index
    signals["position"] = 0

    signals["position"][short_window:] = np.where(
        short_sma[short_window:] > long_sma[short_window:], 1.0, -1.0
    )
    signals["position"] = signals["position"].diff().replace(0, np.nan).dropna()

    return signals.reset_index(drop=True)
