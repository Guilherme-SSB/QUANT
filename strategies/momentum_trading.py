import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def fetch_historical_data(tickers, start_date, end_date):
    """
    Fetch historical data for the provided tickers.
    """
    data = {}
    for ticker in tickers:
        stock_data = yf.download(ticker, start=start_date, end=end_date)
        data[ticker] = stock_data
    return data


def momentum_strategy(prices, short_window=20, long_window=50):
    """
    Implements a momentum strategy using moving averages.

    Args:
        prices (pd.Series): Closing prices of the stock.
        short_window (int): Window for the short-term moving average.
        long_window (int): Window for the long-term moving average.

    Returns:
        pd.DataFrame: Contains signals for buying and selling.
    """
    signals = pd.DataFrame(index=prices.index)
    signals['price'] = prices
    signals['short_mavg'] = prices.rolling(window=short_window, min_periods=1).mean()
    signals['long_mavg'] = prices.rolling(window=long_window, min_periods=1).mean()

    # Generate signals
    signals['signal'] = 0
    signals.loc[signals['short_mavg'] > signals['long_mavg'], 'signal'] = 1
    signals.loc[signals['short_mavg'] <= signals['long_mavg'], 'signal'] = -1

    # Create buy/sell positions
    signals['position'] = signals['signal'].diff()

    return signals


def backtest_strategy(signals, initial_capital=100000):
    """
    Backtest the momentum strategy.

    Args:
        signals (pd.DataFrame): DataFrame with price and signals.
        initial_capital (float): Starting capital for backtesting.

    Returns:
        pd.DataFrame: Portfolio performance over time.
    """
    portfolio = pd.DataFrame(index=signals.index)
    portfolio['price'] = signals['price']
    portfolio['holdings'] = signals['position'].cumsum() * signals['price']
    portfolio['cash'] = initial_capital - (signals['position'] * signals['price']).cumsum()
    portfolio['total'] = portfolio['holdings'] + portfolio['cash']
    portfolio['returns'] = portfolio['total'].pct_change()
    return portfolio


def generate_report(portfolio):
    """
    Generate a performance report for the backtest.

    Args:
        portfolio (pd.DataFrame): Portfolio performance data.

    Returns:
        dict: Summary of the strategy performance.
    """
    portfolio = portfolio.iloc[2:]
    total_return = (portfolio['total'].iloc[-1] / portfolio['total'].iloc[0]) - 1
    annualized_return = (1 + total_return) ** (252 / len(portfolio)) - 1
    annualized_volatility = portfolio['returns'].std() * np.sqrt(252)
    sharpe_ratio = annualized_return / annualized_volatility if annualized_volatility != 0 else np.nan

    return {
        'Total Return': f"{total_return:.2%}",
        'Annualized Return': f"{annualized_return:.2%}",
        'Annualized Volatility': f"{annualized_volatility:.2%}",
        'Sharpe Ratio': f"{sharpe_ratio:.2f}"
    }


def plot_momentum_strategy(signals, ticker):
    """
    Plot the momentum strategy with buy and sell signals.

    Args:
        signals (pd.DataFrame): DataFrame containing price, moving averages, and signals.
        ticker (str): The stock ticker for the plot title.
    """
    plt.figure(figsize=(14, 8))

    # Plot the price and moving averages
    plt.plot(signals.index, signals['price'], label='Price', color='black', alpha=0.7)
    plt.plot(signals.index, signals['short_mavg'], label='Short Moving Average', color='blue', linestyle='--')
    plt.plot(signals.index, signals['long_mavg'], label='Long Moving Average', color='red', linestyle='--')

    # Highlight buy and sell signals
    buy_signals = signals[signals['signal'] == 1]
    sell_signals = signals[signals['signal'] == -1]
    plt.scatter(buy_signals.index, buy_signals['price'], label='Buy Signal', marker='^', color='green', alpha=1)
    plt.scatter(sell_signals.index, sell_signals['price'], label='Sell Signal', marker='v', color='red', alpha=1)

    plt.title(f"Momentum Strategy for {ticker}")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.show()


def run_momentum_model(tickers, start_date, end_date, short_window=20, long_window=50):
    """
    Main function to execute the momentum strategy.

    Args:
        tickers (list): List of stock tickers.
        start_date (str): Start date for historical data.
        end_date (str): End date for historical data.
        short_window (int): Short window for moving average.
        long_window (int): Long window for moving average.

    Returns:
        dict: Performance reports for each ticker.
    """
    data = fetch_historical_data(tickers, start_date, end_date)
    reports = {}

    for ticker, df in data.items():
        if 'Close' not in df:
            continue

        signals = momentum_strategy(df['Close'], short_window, long_window)
        portfolio = backtest_strategy(signals)
        report = generate_report(portfolio)
        reports[ticker] = report

        # Plot the strategy
        plot_momentum_strategy(signals, ticker)

    return reports


# Example Usage
tickers = [
    'PETR4.SA', 'VALE3.SA', 'ITUB4.SA', 'BBDC4.SA', 'BBAS3.SA',
    'ABEV3.SA', 'MGLU3.SA', 'WEGE3.SA', 'ELET3.SA', 'ELET6.SA'
]
date_start = '2020-01-01'
date_end = '2023-12-31'
reports = run_momentum_model(tickers, date_start, date_end)

for ticker, report in reports.items():
    print(f"Report for {ticker}:")
    for key, value in report.items():
        print(f"  {key}: {value}")
    print()
