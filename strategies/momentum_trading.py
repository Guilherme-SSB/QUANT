import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from itertools import product


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

    # Create buy/sell positions only when signal changes
    signals['position'] = signals['signal'].diff()
    signals['position'] = signals['position'].fillna(0)  # Handle NaN for the first row

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

    # Highlight buy and sell signals only when there is a change in signal
    buy_signals = signals[signals['position'] == 2]  # Change from -1 to 1
    sell_signals = signals[signals['position'] == -2]  # Change from 1 to -1
    plt.scatter(buy_signals.index, buy_signals['price'], label='Buy Signal', marker='^', color='green', alpha=1,
                edgecolor='k')
    plt.scatter(sell_signals.index, sell_signals['price'], label='Sell Signal', marker='v', color='red', alpha=1,
                edgecolor='k')

    plt.title(f"Momentum Strategy for {ticker}")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.show()


def grid_search(tickers, start_date, end_date, short_window_range, long_window_range):
    """
    Perform a grid search to optimize short and long window parameters.

    Args:
        tickers (list): List of stock tickers.
        start_date (str): Start date for historical data.
        end_date (str): End date for historical data.
        short_window_range (list): Range of values for short window.
        long_window_range (list): Range of values for long window.

    Returns:
        dict: Best parameters and performance metrics for each ticker.
    """
    data = fetch_historical_data(tickers, start_date, end_date)
    best_params = {}

    for ticker, df in data.items():
        if 'Close' not in df:
            continue

        best_sharpe = -np.inf
        best_combination = None

        for short_window, long_window in product(short_window_range, long_window_range):
            if short_window >= long_window:
                continue  # Skip invalid combinations

            signals = momentum_strategy(df['Close'], short_window, long_window)
            portfolio = backtest_strategy(signals)
            report = generate_report(portfolio)

            sharpe_ratio = float(report['Sharpe Ratio'])

            if sharpe_ratio > best_sharpe:
                best_sharpe = sharpe_ratio
                best_combination = (short_window, long_window)

        best_params[ticker] = {
            'Best Short Window': best_combination[0],
            'Best Long Window': best_combination[1],
            'Best Sharpe Ratio': best_sharpe
        }

    return best_params


def consolidate_portfolio(portfolios):
    """
    Consolidate multiple portfolios into a single portfolio with equal weight allocation.

    Args:
        portfolios (dict): Dictionary of portfolios, where keys are tickers and values are DataFrames.

    Returns:
        pd.DataFrame: Consolidated portfolio performance.
    """
    consolidated = pd.DataFrame()
    for ticker, portfolio in portfolios.items():
        if consolidated.empty:
            # Inicializa o portfólio consolidado com a contribuição do primeiro ativo
            consolidated['total'] = portfolio['total'] / len(portfolios)
        else:
            # Soma a contribuição proporcional de cada ativo
            consolidated['total'] += portfolio['total'] / len(portfolios)

    # Calcula os retornos do portfólio consolidado
    consolidated['returns'] = consolidated['total'].pct_change()
    return consolidated


def run_momentum_model(tickers, start_date, end_date, best_params):
    """
    Execute the momentum strategy using the best parameters from grid search.

    Args:
        tickers (list): List of stock tickers.
        start_date (str): Start date for historical data.
        end_date (str): End date for historical data.
        best_params (dict): Dictionary containing the best parameters for each ticker.

    Returns:
        dict: Performance reports for each ticker and the consolidated portfolio.
    """
    data = fetch_historical_data(tickers, start_date, end_date)
    reports = {}
    portfolios = {}

    for ticker, df in data.items():
        if 'Close' not in df or ticker not in best_params:
            continue

        # Retrieve best parameters for the ticker
        short_window = best_params[ticker]['Best Short Window']
        long_window = best_params[ticker]['Best Long Window']

        # Run the strategy with the best parameters
        signals = momentum_strategy(df['Close'], short_window, long_window)
        portfolio = backtest_strategy(signals)
        portfolios[ticker] = portfolio
        report = generate_report(portfolio)
        reports[ticker] = report

        # Plot the strategy
        # plot_momentum_strategy(signals, ticker)

    # Consolidate the portfolios and generate a consolidated report
    consolidated_portfolio = consolidate_portfolio(portfolios)
    consolidated_report = generate_report(consolidated_portfolio)

    print("\nConsolidated Portfolio Report:")
    for key, value in consolidated_report.items():
        print(f"{key}: {value}")

    reports['Consolidated'] = consolidated_report
    return reports


# Example Usage
tickers = [
    'PETR4.SA', 'VALE3.SA', 'ITUB4.SA', 'BBDC4.SA', 'BBAS3.SA',
    'ABEV3.SA', 'MGLU3.SA', 'WEGE3.SA', 'ELET3.SA', 'ELET6.SA'
]
date_start = '2020-01-01'
date_end = '2024-12-31'
short_window_range = range(5, 51, 5)
long_window_range = range(10, 101, 10)

best_params = grid_search(tickers, date_start, date_end, short_window_range, long_window_range)
for ticker, params in best_params.items():
    print(f"Best parameters for {ticker}: {params}")

reports = run_momentum_model(tickers, date_start, date_end, best_params)
for ticker, report in reports.items():
    print(f"Report for {ticker}:")
    for key, value in report.items():
        print(f"  {key}: {value}")
    print()