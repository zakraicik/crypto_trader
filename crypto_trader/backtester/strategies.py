import pandas as pd


def moving_average_crossover(prices):
    """
    Generate trading signals based on a moving average crossover strategy.

    Parameters:
    - prices: A pandas DataFrame containing historical price data.

    Returns:
    A pandas Series of trading signals, where a positive signal indicates a buy order and a negative signal indicates a sell order.
    """
    # Calculate the 50-day moving average
    ma50 = prices["close"].rolling(window=50).mean()

    # Generate buy signals whenever the price crosses above the moving average
    signals = pd.Series(0, index=prices.index)
    signals[prices["close"] > ma50] = 1

    return signals
