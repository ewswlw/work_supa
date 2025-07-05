# Bond Z-Score Dashboard

An Excel-like dashboard application built with Plotly Dash for analyzing bond spread data and Z-scores.

## Features

### 📊 Excel-like Table Interface
- **Interactive Data Table**: Browse through 13,530+ bond records with 41 columns
- **Multi-column Sorting**: Click headers to sort by multiple columns
- **Native Filtering**: Built-in column filters for quick data exploration
- **Row Selection**: Select multiple rows for analysis
- **Export Functionality**: Export filtered data to CSV

### 🔍 Advanced Filtering
- **Sector Filter**: Filter by Custom Sector (e.g., Financial, Industrial, etc.)
- **Rating Filter**: Filter by bond ratings (AAA, AA, A, BBB, etc.)
- **Currency Filter**: Filter by currency (USD, EUR, etc.)
- **Z-Score Range**: Interactive slider to filter by Z-score ranges
- **Global Search**: Search across all columns with a single text input

### 📈 Real-time Statistics
- **Live Summary Cards**: Automatically updated statistics as you filter
- **Key Metrics**:
  - Total filtered records
  - Average Z-Score
  - Average Spread
  - Min/Max Z-Score
  - Unique sectors count

### 🎨 Visual Enhancements
- **Color-coded Z-Scores**: 
  - 🟢 Green: Z-Score < -2 (potentially undervalued)
  - 🔴 Red: Z-Score > 2 (potentially overvalued)
- **Alternating Row Colors**: Better readability
- **Responsive Design**: Works on different screen sizes

## Data Structure

The dashboard displays bond pair analysis with the following key columns:

### Bond Information
- **Security_1 / Security_2**: Bond identifiers
- **Ticker_1 / Ticker_2**: Bond tickers
- **Rating_1 / Rating_2**: Credit ratings
- **Currency_1 / Currency_2**: Bond currencies
- **Custom_Sector_1 / Custom_Sector_2**: Sector classifications

### Spread Analysis
- **Last_Spread**: Current spread between bond pairs
- **Z_Score**: Statistical Z-score of the spread
- **Max / Min**: Historical max/min spreads
- **Percentile**: Percentile ranking of current spread

### Market Data
- **Best Bid / Best Offer**: Current market prices
- **Bid/Offer Spread**: Bid-offer spreads
- **Size**: Trade sizes
- **G Spread**: Government spread

## Getting Started

### Prerequisites
Make sure you have the required Python packages installed:
```bash
pip3 install pandas pyarrow dash dash-bootstrap-components plotly
```

### Running the Dashboard

#### Option 1: Direct Launch
```bash
python3 bond_dashboard.py
```

#### Option 2: Using the Startup Script
```bash
python3 run_dashboard.py
```

### Accessing the Dashboard
Once started, open your web browser and navigate to:
```
http://localhost:8050
```

## Usage Guide

### 1. Basic Navigation
- **Table Interaction**: Click column headers to sort, use built-in filters
- **Pagination**: Navigate through pages using the pagination controls
- **Row Selection**: Click checkboxes to select rows

### 2. Filtering Data
- **Dropdown Filters**: Use the sector, rating, and currency dropdowns
- **Z-Score Range**: Drag the range slider to filter by Z-score
- **Search**: Type in the search box to find specific bonds or characteristics
- **Reset**: Click "Reset Filters" to clear all filters

### 3. Analyzing Results
- **Summary Cards**: Monitor key statistics in real-time
- **Color Coding**: Identify extreme Z-scores visually
- **Export**: Use the built-in export functionality to save filtered results

### 4. Advanced Features
- **Multi-column Sorting**: Hold Shift while clicking headers
- **Column Resizing**: Drag column borders to resize
- **Fixed Headers**: Headers stay visible while scrolling

## Key Metrics Explanation

### Z-Score
- **Above +2**: Spread is unusually wide (potential mean reversion opportunity)
- **Below -2**: Spread is unusually narrow (potential widening opportunity)
- **Between -1 and +1**: Spread is within normal range

### Spread Analysis
- **Last_Spread**: Current spread in basis points
- **Percentile**: Where current spread ranks historically (0-100)
- **Last_vs_Max/Min**: How current spread compares to historical extremes

## Performance Notes

- **Data Loading**: Initial load processes 13,530 records
- **Filtering**: Real-time filtering with immediate updates
- **Responsiveness**: Optimized for datasets with 10K+ rows
- **Memory Usage**: Efficient pandas operations for large datasets

## Troubleshooting

### Common Issues

1. **Application won't start**
   - Check that all required packages are installed
   - Ensure the `bond_z.parquet` file exists in the `historical g spread/` directory

2. **Slow performance**
   - Use filters to reduce the dataset size
   - Consider limiting the number of visible columns

3. **Port already in use**
   - The dashboard runs on port 8050 by default
   - If port is busy, modify the port in `bond_dashboard.py`

### Data Issues
- Missing values are displayed as empty strings
- Categorical columns are automatically converted to strings
- Numeric formatting is applied to financial columns

## Technical Details

### Architecture
- **Framework**: Plotly Dash with Bootstrap components
- **Data Processing**: Pandas for data manipulation
- **Styling**: Bootstrap CSS framework
- **Interactivity**: Dash callbacks for real-time updates

### File Structure
```
bond_dashboard.py       # Main dashboard application
run_dashboard.py        # Startup script
DASHBOARD_README.md     # This documentation
historical g spread/    # Data directory
├── bond_z.parquet     # Main data file
└── ...
```

## Customization

### Adding New Filters
To add new filters, modify the filter section in `bond_dashboard.py`:
1. Add the filter component to the layout
2. Update the callback function to handle the new filter
3. Apply the filter logic in the `update_table` function

### Styling Changes
- Modify the `style_*` dictionaries in the DataTable component
- Update Bootstrap classes in the layout components
- Add custom CSS by modifying the external stylesheets

### Performance Optimization
- Use `page_size` parameter to control rows per page
- Implement server-side filtering for very large datasets
- Consider using Dash's built-in caching mechanisms

## Support

For issues or questions about the dashboard:
1. Check the troubleshooting section above
2. Review the console output for error messages
3. Ensure all dependencies are correctly installed

---

**Dashboard Version**: 1.0  
**Last Updated**: 2024  
**Compatible with**: Python 3.8+, Dash 3.0+