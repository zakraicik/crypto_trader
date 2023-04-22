import logging
from crypto_trader.backtester.backtester import run_backtest
from crypto_trader.backtester.strategies import sma_crossover_strategy
from crypto_trader.helper import (
    BUCKET,
    from_s3,
    plot_signals,
)

from crypto_trader.keys import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

logger = logging.getLogger(__name__)


def main():
    s3_key = "data/ETHUSDT/2017_04_15_2023_04_10_1d.json"

    data = from_s3(BUCKET, s3_key, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

    short_window = 50
    long_window = 200

    signals = sma_crossover_strategy(data, short_window, long_window)

    plot_signals(data, signals)


if __name__ == "__main__":
    main()
