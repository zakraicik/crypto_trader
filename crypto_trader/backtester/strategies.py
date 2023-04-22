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

    signals["timestamp"] = data["open_time"]

    signals["position"] = 0

    signals["position"][long_window:] = np.where(
        short_sma[long_window:] > long_sma[long_window:], 1.0, -1.0
    )

    signals["position"] = signals["position"].diff()

    signals["short_sma"] = short_sma

    signals["long_sma"] = long_sma

    signals["position"] = np.where(
        signals["position"] < 0, -1, np.where(signals["position"] > 0, 1, 0)
    )

    return signals.reset_index(drop=True)
