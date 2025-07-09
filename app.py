import dash
from dash import dcc, html, Input, Output, State, callback, ctx
import dash_ag_grid as dag
import pandas as pd
from pathlib import Path
import json
import plotly.express as px
import dash_bootstrap_components as dbc

# Load the data
parquet_path = Path('historical g spread/bond_z.parquet')
df = pd.read_parquet(parquet_path)

# Define column configurations with advanced formatting
def get_column_defs(df):
    column_defs = []
    for col in df.columns:
        col_def = {
            "headerName": col,
            "field": col,
            "filter": True,
            "sortable": True,
            "resizable": True,
            "checkboxSelection": True if col == df.columns[0] else False,
            "headerCheckboxSelection": True if col == df.columns[0] else False,
            "pinned": None,
            "width": 150,
            "minWidth": 80,
            "maxWidth": 400,
        }
        # Apply specific formatting based on column type and name
        if df[col].dtype in ['float64', 'int64']:
            if 'spread' in col.lower() or 'price' in col.lower() or 'bid' in col.lower() or 'offer' in col.lower():
                col_def.update({
                    "type": "numericColumn",
                    "valueFormatter": {"function": "d3.format(',.4f')(params.value)"},
                    "cellStyle": {"textAlign": "right", "color": "#2E86AB"},
                })
            elif 'z_score' in col.lower() or 'percentile' in col.lower():
                col_def.update({
                    "type": "numericColumn",
                    "valueFormatter": {"function": "d3.format(',.2f')(params.value)"},
                    "cellStyle": {
                        "function": """
                        function(params) {
                            if (params.value > 2) return {backgroundColor: '#ff4444', color: 'white'};
                            if (params.value > 1) return {backgroundColor: '#ffaa44', color: 'white'};
                            if (params.value < -2) return {backgroundColor: '#44ff44', color: 'white'};
                            if (params.value < -1) return {backgroundColor: '#aaffaa', color: 'black'};
                            return {textAlign: 'right'};
                        }
                        """
                    }
                })
            else:
                col_def.update({
                    "type": "numericColumn",
                    "valueFormatter": {"function": "d3.format(',.2f')(params.value)"},
                    "cellStyle": {"textAlign": "right"},
                })
        elif 'rating' in col.lower():
            col_def.update({
                "cellStyle": {
                    "function": """
                    function(params) {
                        if (params.value && params.value.includes('AAA')) return {backgroundColor: '#00ff00', color: 'black'};
                        if (params.value && params.value.includes('BB')) return {backgroundColor: '#ffaa00', color: 'black'};
                        if (params.value && params.value.includes('C')) return {backgroundColor: '#ff0000', color: 'white'};
                        return {};
                    }
                    """
                }
            })
        column_defs.append(col_def)
    return column_defs

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])

# Define the layout
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("Z Analytics Dashboard", className="text-center mb-4"),
            # Control Panel
            dbc.Card([
                dbc.CardHeader("Grid Controls"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Select Columns:"),
                            dcc.Dropdown(
                                id='column-selector',
                                options=[{'label': col, 'value': col} for col in df.columns],
                                value=list(df.columns),
                                multi=True,
                                style={'color': 'black'}
                            )
                        ], width=4),
                        dbc.Col([
                            dbc.Label("Pin Columns:"),
                            dcc.Dropdown(
                                id='pin-selector',
                                options=[{'label': col, 'value': col} for col in df.columns[:10]],
                                value=[],
                                multi=True,
                                style={'color': 'black'}
                            )
                        ], width=4),
                        dbc.Col([
                            dbc.Label("Row Height:"),
                            dcc.Slider(
                                id='row-height-slider',
                                min=25,
                                max=100,
                                step=5,
                                value=35,
                                marks={25: '25px', 50: '50px', 75: '75px', 100: '100px'}
                            )
                        ], width=4),
                    ]),
                    html.Hr(),
                    dbc.Row([
                        dbc.Col([
                            dbc.Button("Export to Excel", id="export-btn", color="primary", className="me-2"),
                            dbc.Button("Export to CSV", id="export-csv-btn", color="secondary", className="me-2"),
                            dbc.Button("Reset Grid", id="reset-btn", color="warning"),
                        ], width=12)
                    ])
                ])
            ], className="mb-4"),
            # Resizable Grid Container
            dbc.Card([
                dbc.CardHeader([
                    html.H5("Data Grid", className="mb-0"),
                    dbc.Badge(f"{len(df):,} rows × {len(df.columns)} columns", color="info")
                ]),
                dbc.CardBody([
                    dag.AgGrid(
                        id="z-analytics-grid",
                        columnDefs=get_column_defs(df),
                        rowData=df.to_dict('records'),
                        defaultColDef={
                            "filter": True,
                            "sortable": True,
                            "resizable": True,
                            "minWidth": 80,
                        },
                        dashGridOptions={
                            "pagination": True,
                            "paginationPageSize": 100,
                            "rowSelection": "multiple",
                            "enableRangeSelection": True,
                            "enableColumnReorder": True,
                            "enableColumnResizing": True,
                            "animateRows": True,
                            "rowHeight": 35,
                            "headerHeight": 40,
                            "enableAdvancedFilter": True,
                            "sideBar": {
                                "toolPanels": [
                                    {
                                        "id": "columns",
                                        "labelDefault": "Columns",
                                        "labelKey": "columns",
                                        "iconKey": "columns",
                                        "toolPanel": "agColumnsToolPanel",
                                        "toolPanelParams": {
                                            "suppressRowGroups": True,
                                            "suppressValues": True,
                                            "suppressPivots": True,
                                            "suppressPivotMode": True,
                                            "suppressColumnFilter": False,
                                            "suppressColumnSelectAll": False,
                                            "suppressColumnExpandAll": False
                                        }
                                    },
                                    {
                                        "id": "filters",
                                        "labelDefault": "Filters",
                                        "labelKey": "filters",
                                        "iconKey": "filter",
                                        "toolPanel": "agFiltersToolPanel"
                                    }
                                ]
                            }
                        },
                        style={
                            "height": "140vh", 
                            "resize": "vertical", 
                            "overflow": "auto", 
                            "min-height": "800px",
                            "max-height": "190vh"
                        },
                        className="ag-theme-alpine-dark"
                    )
                ], style={"padding": "0"})
            ])
        ])
    ]),
    dcc.Store(id='grid-data-store', data=df.to_dict('records')),
    dcc.Download(id="download-excel"),
    dcc.Download(id="download-csv"),
], fluid=True)

# Callbacks
@callback(
    Output("z-analytics-grid", "columnDefs"),
    Output("z-analytics-grid", "rowData"),
    Input("column-selector", "value"),
    Input("pin-selector", "value"),
    Input("row-height-slider", "value"),
    State("grid-data-store", "data")
)
def update_grid(selected_columns, pinned_columns, row_height, stored_data):
    if not selected_columns:
        selected_columns = list(df.columns)
    filtered_df = df[selected_columns]
    column_defs = get_column_defs(filtered_df)
    for col_def in column_defs:
        if col_def["field"] in pinned_columns:
            col_def["pinned"] = "left"
    row_data = filtered_df.to_dict('records')
    return column_defs, row_data

@callback(
    Output("z-analytics-grid", "dashGridOptions"),
    Input("row-height-slider", "value")
)
def update_row_height(row_height):
    return {
        "pagination": True,
        "paginationPageSize": 100,
        "rowSelection": "multiple",
        "enableRangeSelection": True,
        "enableColumnReorder": True,
        "enableColumnResizing": True,
        "animateRows": True,
        "rowHeight": row_height,
        "headerHeight": 40,
        "enableAdvancedFilter": True,
        "sideBar": {
            "toolPanels": [
                {
                    "id": "columns",
                    "labelDefault": "Columns",
                    "labelKey": "columns",
                    "iconKey": "columns",
                    "toolPanel": "agColumnsToolPanel",
                    "toolPanelParams": {
                        "suppressRowGroups": True,
                        "suppressValues": True,
                        "suppressPivots": True,
                        "suppressPivotMode": True,
                        "suppressColumnFilter": False,
                        "suppressColumnSelectAll": False,
                        "suppressColumnExpandAll": False
                    }
                },
                {
                    "id": "filters",
                    "labelDefault": "Filters",
                    "labelKey": "filters",
                    "iconKey": "filter",
                    "toolPanel": "agFiltersToolPanel"
                }
            ]
        }
    }

@callback(
    Output("download-excel", "data"),
    Input("export-btn", "n_clicks"),
    State("column-selector", "value"),
    prevent_initial_call=True
)
def export_excel(n_clicks, selected_columns):
    if n_clicks and selected_columns:
        filtered_df = df[selected_columns]
        return dcc.send_data_frame(filtered_df.to_excel, "z_analytics_export.xlsx", index=False)

@callback(
    Output("download-csv", "data"),
    Input("export-csv-btn", "n_clicks"),
    State("column-selector", "value"),
    prevent_initial_call=True
)
def export_csv(n_clicks, selected_columns):
    if n_clicks and selected_columns:
        filtered_df = df[selected_columns]
        return dcc.send_data_frame(filtered_df.to_csv, "z_analytics_export.csv", index=False)

@callback(
    Output("column-selector", "value"),
    Output("pin-selector", "value"),
    Input("reset-btn", "n_clicks"),
    prevent_initial_call=True
)
def reset_grid(n_clicks):
    if n_clicks:
        return list(df.columns), []

# Run the app
if __name__ == "__main__":
    app.run_server(debug=True, port=8050)

pd.set_option('display.max_rows', 100)  # Show up to 100 rows
pd.set_option('display.max_columns', None)  # Show all columns 