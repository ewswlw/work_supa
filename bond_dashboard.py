#!/usr/bin/env python3
"""
Excel-like Dashboard App for Bond Z Data using Plotly Dash
"""

import pandas as pd
import dash
from dash import dcc, html, dash_table, callback, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import numpy as np

# Load data
print("Loading bond data...")
df = pd.read_parquet('historical g spread/bond_z.parquet')

# Handle categorical columns properly
categorical_columns = df.select_dtypes(include=['category']).columns
for col in categorical_columns:
    # Convert categorical to string to avoid fillna issues
    df[col] = df[col].astype(str)

# Convert NaN to empty string for better display
df = df.fillna('')

# Convert numeric columns to formatted strings for display
numeric_columns = ['Last_Spread', 'Z_Score', 'Max', 'Min', 'Last_vs_Max', 'Last_vs_Min', 
                   'Percentile', 'XCCY', 'Best Bid_1', 'Best Offer_1', 'Bid/Offer_1', 
                   'Size @ Best Bid_1', 'Size @ Best Offer_1', 'G Spread_1',
                   'Best Bid_2', 'Best Offer_2', 'Bid/Offer_2', 'Size @ Best Bid_2', 
                   'Size @ Best Offer_2', 'G Spread_2']

# Create formatted versions for display
df_display = df.copy()
for col in numeric_columns:
    if col in df.columns:
        df_display[col] = df[col].apply(lambda x: f"{x:.2f}" if pd.notna(x) and x != '' and str(x) != 'nan' else '')

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Define the app layout
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("Bond Z-Score Dashboard", className="text-center mb-4", 
                   style={'color': '#2c3e50', 'font-weight': 'bold'}),
            html.H5(f"Total Records: {len(df):,}", className="text-center mb-4", 
                   style={'color': '#7f8c8d'})
        ])
    ]),
    
    # Filters Row
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H5("Filters", className="card-title"),
                    dbc.Row([
                        dbc.Col([
                            html.Label("Sector 1:"),
                            dcc.Dropdown(
                                id='sector1-filter',
                                options=[{'label': 'All', 'value': 'all'}] + 
                                       [{'label': val, 'value': val} for val in sorted(df['Custom_Sector_1'].unique()) if val != '' and val != 'nan'],
                                value='all',
                                multi=False
                            )
                        ], md=3),
                        dbc.Col([
                            html.Label("Rating 1:"),
                            dcc.Dropdown(
                                id='rating1-filter',
                                options=[{'label': 'All', 'value': 'all'}] + 
                                       [{'label': val, 'value': val} for val in sorted(df['Rating_1'].unique()) if val != '' and val != 'nan'],
                                value='all',
                                multi=False
                            )
                        ], md=3),
                        dbc.Col([
                            html.Label("Currency 1:"),
                            dcc.Dropdown(
                                id='currency1-filter',
                                options=[{'label': 'All', 'value': 'all'}] + 
                                       [{'label': val, 'value': val} for val in sorted(df['Currency_1'].unique()) if val != '' and val != 'nan'],
                                value='all',
                                multi=False
                            )
                        ], md=3),
                        dbc.Col([
                            html.Label("Z-Score Range:"),
                            dcc.RangeSlider(
                                id='zscore-range',
                                min=df['Z_Score'].min(),
                                max=df['Z_Score'].max(),
                                step=0.1,
                                value=[df['Z_Score'].min(), df['Z_Score'].max()],
                                marks={
                                    -2: '-2',
                                    -1: '-1',
                                    0: '0',
                                    1: '1',
                                    2: '2'
                                },
                                tooltip={"placement": "bottom", "always_visible": True}
                            )
                        ], md=3)
                    ])
                ])
            ])
        ], width=12)
    ], className="mb-4"),
    
    # Summary Statistics Row
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H5("Summary Statistics", className="card-title"),
                    html.Div(id='summary-stats')
                ])
            ])
        ], width=12)
    ], className="mb-4"),
    
    # Search Row
    dbc.Row([
        dbc.Col([
            dbc.InputGroup([
                dbc.InputGroupText("🔍"),
                dbc.Input(id="search-input", placeholder="Search across all columns...", type="text")
            ])
        ], width=8),
        dbc.Col([
            dbc.Button("Reset Filters", id="reset-btn", color="secondary", className="w-100")
        ], width=2),
        dbc.Col([
            dbc.Button("Export CSV", id="export-btn", color="primary", className="w-100")
        ], width=2)
    ], className="mb-4"),
    
    # Data Table
    dbc.Row([
        dbc.Col([
            dash_table.DataTable(
                id='data-table',
                columns=[
                    {
                        'name': col,
                        'id': col,
                        'type': 'numeric' if col in numeric_columns else 'text',
                        'format': {'specifier': '.2f'} if col in numeric_columns else None
                    }
                    for col in df.columns
                ],
                data=df_display.to_dict('records'),
                page_size=25,
                sort_action="native",
                sort_mode="multi",
                filter_action="native",
                row_selectable="multi",
                selected_rows=[],
                style_cell={
                    'textAlign': 'left',
                    'padding': '10px',
                    'fontFamily': 'Arial',
                    'fontSize': '12px',
                    'border': '1px solid #ddd',
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'minWidth': '100px',
                    'maxWidth': '200px'
                },
                style_header={
                    'backgroundColor': '#3498db',
                    'color': 'white',
                    'fontWeight': 'bold',
                    'textAlign': 'center',
                    'border': '1px solid #2980b9'
                },
                style_data={
                    'backgroundColor': '#f8f9fa',
                    'color': '#2c3e50',
                    'border': '1px solid #e9ecef'
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': '#ffffff'
                    },
                    {
                        'if': {'column_id': 'Z_Score', 'filter_query': '{Z_Score} > 2'},
                        'backgroundColor': '#e74c3c',
                        'color': 'white'
                    },
                    {
                        'if': {'column_id': 'Z_Score', 'filter_query': '{Z_Score} < -2'},
                        'backgroundColor': '#27ae60',
                        'color': 'white'
                    }
                ],
                fixed_rows={'headers': True},
                style_table={'overflowX': 'auto', 'height': '600px'},
                export_format="csv",
                export_headers="display"
            )
        ], width=12)
    ]),
    
    # Footer
    dbc.Row([
        dbc.Col([
            html.Hr(),
            html.P(f"Dashboard generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
                   className="text-muted text-center")
        ])
    ], className="mt-4")
], fluid=True)

# Callbacks
@app.callback(
    [Output('data-table', 'data'),
     Output('summary-stats', 'children')],
    [Input('sector1-filter', 'value'),
     Input('rating1-filter', 'value'),
     Input('currency1-filter', 'value'),
     Input('zscore-range', 'value'),
     Input('search-input', 'value')]
)
def update_table(sector1, rating1, currency1, zscore_range, search_term):
    # Start with the full dataset
    filtered_df = df.copy()
    
    # Apply filters
    if sector1 != 'all':
        filtered_df = filtered_df[filtered_df['Custom_Sector_1'] == sector1]
    
    if rating1 != 'all':
        filtered_df = filtered_df[filtered_df['Rating_1'] == rating1]
    
    if currency1 != 'all':
        filtered_df = filtered_df[filtered_df['Currency_1'] == currency1]
    
    # Z-score range filter
    if zscore_range:
        filtered_df = filtered_df[
            (filtered_df['Z_Score'] >= zscore_range[0]) & 
            (filtered_df['Z_Score'] <= zscore_range[1])
        ]
    
    # Search filter
    if search_term:
        search_term = search_term.lower()
        mask = filtered_df.astype(str).apply(
            lambda x: x.str.lower().str.contains(search_term, na=False)
        ).any(axis=1)
        filtered_df = filtered_df[mask]
    
    # Create display version
    filtered_df_display = filtered_df.copy()
    for col in numeric_columns:
        if col in filtered_df.columns:
            filtered_df_display[col] = filtered_df[col].apply(
                lambda x: f"{x:.2f}" if pd.notna(x) and x != '' and str(x) != 'nan' else ''
            )
    
    # Summary statistics
    if len(filtered_df) > 0:
        summary_stats = dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H6("Filtered Records", className="card-title"),
                        html.H4(f"{len(filtered_df):,}", className="text-primary")
                    ])
                ])
            ], md=2),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H6("Avg Z-Score", className="card-title"),
                        html.H4(f"{filtered_df['Z_Score'].mean():.2f}", className="text-info")
                    ])
                ])
            ], md=2),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H6("Avg Spread", className="card-title"),
                        html.H4(f"{filtered_df['Last_Spread'].mean():.2f}", className="text-success")
                    ])
                ])
            ], md=2),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H6("Min Z-Score", className="card-title"),
                        html.H4(f"{filtered_df['Z_Score'].min():.2f}", className="text-warning")
                    ])
                ])
            ], md=2),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H6("Max Z-Score", className="card-title"),
                        html.H4(f"{filtered_df['Z_Score'].max():.2f}", className="text-danger")
                    ])
                ])
            ], md=2),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H6("Unique Sectors", className="card-title"),
                        html.H4(f"{filtered_df['Custom_Sector_1'].nunique()}", className="text-secondary")
                    ])
                ])
            ], md=2)
        ])
    else:
        summary_stats = dbc.Alert("No data matches the current filters.", color="warning")
    
    return filtered_df_display.to_dict('records'), summary_stats

@app.callback(
    [Output('sector1-filter', 'value'),
     Output('rating1-filter', 'value'),
     Output('currency1-filter', 'value'),
     Output('zscore-range', 'value'),
     Output('search-input', 'value')],
    [Input('reset-btn', 'n_clicks')]
)
def reset_filters(n_clicks):
    if n_clicks:
        return 'all', 'all', 'all', [df['Z_Score'].min(), df['Z_Score'].max()], ''
    return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

if __name__ == '__main__':
    print("Starting Bond Z-Score Dashboard...")
    print(f"Dashboard will be available at: http://localhost:8050")
    print(f"Data loaded: {len(df):,} records with {len(df.columns)} columns")
    app.run(debug=True, host='0.0.0.0', port=8050)