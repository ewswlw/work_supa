# Expert-Level Backtesting System for Market-Timing Strategies

You are an expert algorithmic trader and data scientist with deep knowledge of financial time series, quantitative trading strategies, and the Python library `vectorbt`, Bloomberg liibrary 'XBBG'. Your task is to create very complex market-timing strategies.

- **File path:** `data_pipelines/backtest_data.csv`
- **Primary column of interest:** `cad_ig_er_index` (the asset to be timed)

Each strategy must seek to outperform a buy-and-hold benchmark of `cad_ig_er_index` based on annualized returns and Sharpe ratio. Assume:

- No transaction costs
- No leverage
- No short-selling

### Core Requirements:

1. **Data Exploration**
    - Infer and utilize the statistical properties of all columns
    - Inspect the data for missing values and outliers, and handle them appropriately
    - Construct your strategies based on your knowledge of the column names and their economic meaning and statistical properties. If you need clarification on meaning just ask me.

2. **Strategy Design**
    - Develop unique and complex timing strategies.
    - Each strategy should derive signals (entries/exits) from any combination of the available columns.
    - Strategies may include, but are not limited to:
        - Technical Indicators (e.g., Moving Averages, RSI)
        - Statistical Features
        - Momentum/Mean Reversion Indicators
        - Regime Detection
        - Advanced Data Transformations
        - Time series features and transformations 
    - Clearly articulate the logic behind each strategy, including signal generation and position management (long only).
    - based on what you know about the column names and the data structure and stats, feel free to create new transformations to use as rules (example yoy growth rates, percentiles, z scores etc)

3. **Backtesting with Vectorbt**
    - For each of the strategies, provide a clear, step-by-step approach to implement it in `vectorbt`.
    - Explain how to run the backtest, specifying input parameters such as time range, columns used, and signal definitions.
    - Include code snippets or pseudocode demonstrating strategy setup.
    - Be an expert in this API and refer to documentation when needed. In particular, handling data not in daily frequency tends to be an issue for this library around certain calculations. Make sure you understand how to handle returns and frequencies and their respective classes. 

4. **Performance Evaluation**
    - Compare each strategy’s performance against the buy-and-hold result of `cad_ig_er_index`.
    - focus on pf.stats() as primary metrics for every strategy evaluation and always comepare vs buy and hold
    - Also interested in pf.stats() from the returns accessor (refer to https://vectorbt.dev/api/returns/accessors/)
    - also there tends to be issues with vector bt in handling frequency so below is a code example, you can follow the general logic when dealing with how to handle different frequencies
    
    def create_portfolio(strategy, price, signals, config: BacktestConfig):
    # Filter price and signals to config date range if dates are specified
    if config.start_date is not None:
        price = price[price.index >= config.start_date]
        signals = signals[signals.index >= config.start_date]
    if config.end_date is not None:
        price = price[price.index <= config.end_date]
        signals = signals[signals.index <= config.end_date]
    
    # Convert signals to boolean if they're not already
    signals = signals.astype(bool)
    
    # Resample signals based on rebalance frequency
    if config.rebalance_freq != '1D':
        monthly_signals = signals.resample('M').last()
        signals = monthly_signals.reindex(price.index, method='ffill')
        signals = signals.astype(bool)
    
    # Generate entries and exits
    entries = signals & ~signals.shift(1).fillna(False)
    exits = ~signals & signals.shift(1).fillna(False)
    
    return vbt.Portfolio.from_signals(
        price,
        entries,
        exits,
        freq='1D',
        init_cash=config.initial_capital,
        size=config.size,
        size_type=config.size_type,
        accumulate=False
    )

5. **Results Presentation**
    - Present all final results in tables (e.g., pandas DataFrames).
    - pf.stats() and ret_acc.stats() from the returns accessor (refer to https://vectorbt.dev/api/returns/accessors/)
    - make sure output is nicely formatted and easy to read
    - add as much details as possible, the more the better 

6. **Explanations**
    - Provide commentary on why each strategy might succeed or fail under various market conditions.
    - Note potential pitfalls (e.g., data-mining bias, overfitting) and how to mitigate them.

### Output Should Include:

- A concise but complete step-by-step explanation of each of the strategies.
- Code snippets or pseudocode demonstrating how each strategy is set up using `vectorbt`.
- Tables summarizing the performance of each strategy against the buy-and-hold benchmark.

### Additional Instructions:

- **Data Granularity:** Should always infer based on the frequency of the data. Adjust all parts of the analysis accordingly (example sanity check parameters etc)
- **Performance Metrics:** Annualized return and Sharpe ratio are mandatory; include others as needed (sanity check this vs manual calculations)
- **Output Format:** Always present results in tables.
- **Assumptions:** 
    - The language model will infer the meaning of column names, data structures, and statistical properties.

Before proceeding, ensure all strategies and backtests are reproducible with the provided data and instructions. Address any potential biases and emphasize scalability for future enhancements.

## Returns Statistics Implementation Best Practices

When working with vectorbt's returns statistics, proper implementation is crucial for accurate performance metrics. The following section details best practices and common pitfalls to avoid.

### Accessing Returns Statistics Correctly

To properly access and configure returns statistics from a portfolio object:

```python
# Get portfolio returns
returns = portfolio.returns()

# CORRECT WAY: Configure the returns accessor with frequency
returns_accessor = returns.vbt.returns(freq='D')  # 'D' for daily data

# Display statistics
print(returns_accessor.stats())
```

### Setting Benchmark Returns for Comparative Metrics

To enable comparative metrics like Alpha, Beta, and Information Ratio:

```python
# Get benchmark returns first
benchmark_returns = benchmark_portfolio.returns()

# Create returns accessor with benchmark
strategy_returns = strategy_portfolio.returns()
strategy_ret_acc = strategy_returns.vbt.returns(
    freq='D',                           # Set frequency
    benchmark_rets=benchmark_returns    # Configure benchmark in constructor
)

# Now all comparative metrics will be available
print(strategy_ret_acc.stats())
```

**IMPORTANT**: Benchmark returns must be set in the accessor constructor, not in the stats() method. The following will NOT work:
```python
# INCORRECT - Will raise TypeError
strategy_ret_acc = strategy_returns.vbt.returns(freq='D')
print(strategy_ret_acc.stats(benchmark_rets=benchmark_returns))  # Error!
```

### Available Returns Metrics

The returns accessor provides several metrics not available in portfolio.stats():

| Metric | Description |
| ------ | ----------- |
| Total Return [%] | Cumulative return over the entire period |
| Annualized Return [%] | Return annualized based on the specified frequency |
| Annualized Volatility [%] | Annualized standard deviation of returns |
| Sharpe Ratio | Risk-adjusted return (excess return / volatility) |
| Sortino Ratio | Downside risk-adjusted return |
| Calmar Ratio | Return / Maximum drawdown |
| Max Drawdown [%] | Maximum peak-to-trough decline |
| Alpha | Excess return compared to benchmark (risk-adjusted) |
| Beta | Sensitivity to benchmark movements |
| Skew | Asymmetry of returns distribution |
| Kurtosis | "Tailedness" of returns distribution |
| Tail Ratio | Ratio of right to left tail |
| Value at Risk | Potential loss at given confidence level |

### Handling Frequency

Frequency setting is critical for annualized metrics calculation:

- For daily data, use `freq='D'`
- For monthly data, use `freq='M'`
- For intraday data, specify the appropriate frequency (e.g., `freq='1H'` for hourly)

If frequency is not set correctly, the following warning appears:
```
UserWarning: Metric 'ann_return' requires frequency to be set
```

### Troubleshooting Common Issues

#### Missing Benchmark Returns

If you see this warning:
```
UserWarning: Metric 'benchmark_return' requires benchmark_rets to be set
```

Solution: Provide benchmark returns in the accessor constructor as shown above.

#### Unexpected Keyword Arguments

If you encounter:
```
TypeError: StatsBuilderMixin.stats() got an unexpected keyword argument 'benchmark_rets'
```

Solution: Move the benchmark_rets parameter from the stats() method to the returns accessor constructor.

### Complete Implementation Example

Here's a complete example demonstrating proper implementation of returns statistics:

```python
def compare_strategies(strategy1_portfolio, strategy2_portfolio, benchmark_portfolio):
    """
    Compare strategies using both portfolio stats and returns statistics.
    
    Args:
        strategy1_portfolio (vbt.Portfolio): Strategy 1 portfolio
        strategy2_portfolio (vbt.Portfolio): Strategy 2 portfolio
        benchmark_portfolio (vbt.Portfolio): Benchmark portfolio
    """
    # Get benchmark returns for comparison
    benchmark_returns = benchmark_portfolio.returns()
    
    # Display Buy and Hold portfolio stats
    print("\nBUY AND HOLD PORTFOLIO STATS:")
    print(benchmark_portfolio.stats())
    
    # Configure returns accessor with frequency
    benchmark_ret_acc = benchmark_returns.vbt.returns(freq='D')
    print("\nBUY AND HOLD RETURNS STATS:")
    print(benchmark_ret_acc.stats())
    
    # Display Strategy 1 portfolio stats
    print("\nSTRATEGY 1 PORTFOLIO STATS:")
    print(strategy1_portfolio.stats())
    
    # Configure Strategy 1 returns accessor with frequency and benchmark
    strategy1_returns = strategy1_portfolio.returns()
    strategy1_ret_acc = strategy1_returns.vbt.returns(
        freq='D', 
        benchmark_rets=benchmark_returns
    )
    print("\nSTRATEGY 1 RETURNS STATS:")
    print(strategy1_ret_acc.stats())
    
    # Display Strategy 2 portfolio stats
    print("\nSTRATEGY 2 PORTFOLIO STATS:")
    print(strategy2_portfolio.stats())
    
    # Configure Strategy 2 returns accessor with frequency and benchmark
    strategy2_returns = strategy2_portfolio.returns()
    strategy2_ret_acc = strategy2_returns.vbt.returns(
        freq='D', 
        benchmark_rets=benchmark_returns
    )
    print("\nSTRATEGY 2 RETURNS STATS:")
    print(strategy2_ret_acc.stats())
```

By following these best practices, you can ensure accurate and comprehensive performance evaluation of your trading strategies in vectorbt.