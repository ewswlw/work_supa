# AI Agent Instructions: Academic Research Extraction and Replication Preparation

## 1. Read and Summarize the Paper in Great Detail

- Parse the entire PDF, extracting all text, tables, and figures.
- Identify the title, authors, publication venue, and year.
- Write a detailed summary of the paper, including:
  - The research question(s) and motivation.
  - Theoretical background and literature context.
  - Main hypotheses or objectives.
  - Overview of methodology.
  - Key findings and conclusions.
- For each section (Introduction, Data, Methodology, Results, Discussion, Conclusion), provide a paragraph-level summary, preserving technical nuance and any caveats.

## 2. Extract and Detail All Data Used

- List every dataset, database, or data vendor referenced (e.g., CRSP, Compustat, Bloomberg, Datastream, FRED, etc.).
- For each data source:
  - Specify the exact variables/fields used (e.g., daily close price, volume, earnings, macroeconomic indicators).
  - Note the data frequency (e.g., daily, monthly, quarterly).
  - State the data range (start and end dates).
  - Record any filters or sample selection criteria (e.g., only S&P 500 stocks, only US equities, only data after 1990).
  - Extract any ticker lists, index constituents, or asset universes.
  - Note any data cleaning, winsorization, or outlier removal steps.
- If the paper uses proprietary or hand-collected data, describe the collection process and any limitations.

## 3. xbbg Replicability and Ticker Mapping

- For each data element, assess if it is available via xbbg (the Bloomberg Python API):
  - Consult the [xbbg documentation](https://xbbg.readthedocs.io/) and Bloomberg’s own field/ticker guides.
  - For each security, provide the Bloomberg ticker format (e.g., AAPL US Equity).
  - For indices, commodities, FX, and macro data, provide the correct Bloomberg tickers (e.g., SPX Index, EURUSD Curncy, GDP CQOQ Index).
  - If the paper uses custom or non-Bloomberg data, note what is not replicable and suggest the closest available proxy.
  - For each variable, specify the Bloomberg field (e.g., PX_LAST, PX_VOLUME, EARNINGS_PER_SHARE).
  - If mapping is ambiguous, provide multiple plausible options and explain the differences.
- Create a table mapping each data item in the paper to its xbbg/Bloomberg equivalent, with notes on any caveats or required transformations.

## 4. Extract and Formalize the Strategy Logic

- Parse the methodology section for all rules, formulas, and algorithms.
- Write out the exact logic of the strategy, including:
  - Signal construction (e.g., moving averages, factor scores, ranking procedures).
  - Portfolio formation rules (e.g., top/bottom decile, equal-weighted, value-weighted).
  - Rebalancing frequency and timing conventions.
  - Transaction cost assumptions, if any.
  - Any risk management or position sizing rules.
- Where formulas are given, transcribe them in LaTeX and explain each variable.
- If code snippets or pseudocode are present, extract and clarify them.
- If any steps are ambiguous, flag them and suggest plausible interpretations.

## 5. Summarize Detailed Results

- Extract all tables and figures showing results (e.g., performance metrics, risk statistics, regression outputs).
- For each result:
  - Note the sample period, universe, and any sub-sample analyses.
  - Record all reported statistics (e.g., mean return, Sharpe ratio, t-stats, alpha, beta, drawdown).
  - If results are stratified (e.g., by size, industry, time period), summarize each stratum.
  - Note any robustness checks, alternative specifications, or sensitivity analyses.
- If possible, reconstruct the main result tables in a structured format (e.g., CSV or Excel).

## 6. Identify Weaknesses and Limitations

- List all limitations acknowledged by the authors.
- Critically assess additional weaknesses, such as:
  - Data-snooping or lookahead bias.
  - Survivorship bias or selection bias.
  - Overfitting or lack of out-of-sample validation.
  - Incomplete or ambiguous methodology.
  - Data availability or replicability issues.
  - Unrealistic transaction cost assumptions.
- Suggest further robustness checks or extensions that would strengthen the study.

## 7. Additional Steps for Full Replication

- List any software, libraries, or code dependencies required.
- Note any data licensing or access requirements (e.g., Bloomberg Terminal).
- Suggest a step-by-step plan for full replication, including:
  - Data download and cleaning.
  - Signal and portfolio construction.
  - Backtesting and performance evaluation.
  - Result comparison and validation.
- If the paper references supplementary materials or code, extract and summarize their contents.

## 8. Output Format

- Provide all extracted information in a structured, human-readable format (e.g., Markdown, Word, or Excel).
- Include tables for data mapping, strategy rules, and results.
- Where possible, provide code templates or pseudocode for key steps.
- Flag any ambiguities or missing information for human review.

---