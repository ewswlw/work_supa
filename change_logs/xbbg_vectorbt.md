# Change Log for xbbg and vectorbt Usage
# Timestamp: 2025-04-18T20:32:51-04:00

## Purpose
This log documents mistakes, corrections, and key learnings from using the xbbg and vectorbt libraries for market timing strategy replication. Refer to this before future projects to avoid common pitfalls and speed up iteration.

---

### xbbg (Bloomberg Data)

- **MultiIndex Columns:**
  - Mistake: Assumed single-level columns from `blp.bdh`. Some tickers/fields return MultiIndex columns (e.g., `('SPX Index', 'PX_LAST')`).
  - Correction: Always check column structure and access with both levels if needed.
  - Lesson: Use `print(df.columns)` after every `xbbg` call to inspect structure.

- **Field/Ticker Availability:**
  - Mistake: Some tickers/fields (e.g., USGG3M Index, YLD_YTM_MID) return empty DataFrames.
  - Correction: Implement fallback logic with alternative tickers/fields (e.g., US0003M Index, PX_LAST as 3M LIBOR proxy).
  - Lesson: Always check `.empty` and log errors. Have a prioritized list of alternatives.

- **Date Handling:**
  - Mistake: Used `.date()` on `datetime.date` index, causing AttributeError.
  - Correction: Use index values directly if already `datetime.date`.
  - Lesson: Inspect index type before applying date methods.

---

### vectorbt (Backtesting)

- **Portfolio.from_orders() Arguments:**
  - Mistake: Used unsupported arguments like `accumulate` and `freq` in older vectorbt versions.
  - Correction: Check installed version's documentation. Remove or replace unsupported args.
  - Lesson: Version compatibility matters—review docs or use `help()` in Python shell.

- **Frequency for Annualized Metrics:**
  - Mistake: Tried to set `freq` in `.stats()` for annualization, but not supported in older versions.
  - Correction: Remove `freq` argument. Accept warning or annualize metrics manually if needed.
  - Lesson: For annualized Sharpe/Sortino, upgrade vectorbt or post-process metrics.

- **Buy-and-Hold Baseline:**
  - Learning: Always include a buy-and-hold comparison for context.

- **Drawdown Calculation:**
  - Learning: Max drawdown can be similar across strategies even if returns differ.

---

### General Debugging & Reproducibility

- **Logging:**
  - Always log raw data, column structures, and fallback events.
- **Error Handling:**
  - Use try/except blocks and print/log all errors for rapid troubleshooting.
- **Documentation:**
  - Comment all nontrivial logic and fallback mechanisms.
- **Save Outputs:**
  - Save stats and main results to CSV for future review.

---

# Add new entries below with a new timestamp for each session/iteration.
