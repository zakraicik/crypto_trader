import argparse
import logging
from crypto_trader.backtester.backtester import run_backtest
from crypto_trader.backtester.strategies import sma_crossover_strategy
from crypto_trader.helper import (
    BUCKET,
    S3_KEY,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    from_s3,
    plot_trades,
)

logger = logging.getLogger(__name__)


def main():
    data = from_s3(BUCKET, S3_KEY, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

    short_window = 50
    long_window = 200

    signals = sma_crossover_strategy(data, short_window, long_window)

    trades = run_backtest(data.reset_index(), sma_crossover_strategy)

    plot_trades(data, trades)
