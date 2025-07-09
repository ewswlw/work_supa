import dash
from dash import dcc, html, Input, Output, State, callback, ctx
import dash_ag_grid as dag
import pandas as pd
from pathlib import Path
import json
import dash_bootstrap_components as dbc
from datetime import datetime
import plotly.graph_objects as go
import numpy as np

# Load the data
parquet_path = Path('historical g spread/bond_z.parquet')
df = pd.read_parquet(parquet_path)

# Enhanced column configurations
def get_column_defs(df, formatting_rules=None):
    if formatting_rules is None:
        formatting_rules = {}
    
    column_defs = []
    for i, col in enumerate(df.columns):
        col_def = {
            "field": col,
            "headerName": f"🎨 {col}" if col in formatting_rules else col,
            "sortable": True,
            "filter": True,
            "resizable": True,
            "headerCheckboxSelection": True if col == df.columns[0] else False,
            "checkboxSelection": True if col == df.columns[0] else False,
            "pinned": "left" if col == df.columns[0] else None,
            "width": 150,
            "cellStyle": formatting_rules.get(col, {})
        }
        column_defs.append(col_def)
    
    return column_defs

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Simple, clean layout with all components included
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("📊 Enhanced Financial Dashboard", className="text-center mb-4"),
            html.Hr(),
            
            # Control Panel
            dbc.Card([
                dbc.CardBody([
                    html.H5("🎛️ Control Panel"),
                    dbc.Row([
                        dbc.Col([
                            html.Label("Select Column to Format:"),
                            dcc.Dropdown(
                                id='format-column-dropdown',
                                options=[{'label': col, 'value': col} for col in df.columns],
                                placeholder="Select column to format..."
                            )
                        ], width=6),
                        dbc.Col([
                            html.Label("Global Search:"),
                            dcc.Input(
                                id='global-search',
                                type='text',
                                placeholder='Search across all columns...',
                                className='form-control'
                            )
                        ], width=6)
                    ])
                ])
            ], className="mb-4"),
            
            # Stats Panel
            dbc.Card([
                dbc.CardBody([
                    html.H5("📈 Dashboard Statistics"),
                    dbc.Row([
                        dbc.Col([
                            html.H6("Total Rows"),
                            html.H4(f"{len(df):,}", className="text-primary")
                        ], width=3),
                        dbc.Col([
                            html.H6("Total Columns"),
                            html.H4(f"{len(df.columns)}", className="text-info")
                        ], width=3),
                        dbc.Col([
                            html.H6("Formatted Columns"),
                            html.H4("0", id="formatted-count", className="text-success")
                        ], width=3),
                        dbc.Col([
                            html.H6("Memory Usage"),
                            html.H4(f"{df.memory_usage().sum() / 1024**2:.1f} MB", className="text-warning")
                        ], width=3)
                    ])
                ])
            ], className="mb-4"),
            
            # Data Table
            dag.AgGrid(
                id='data-table',
                columnDefs=get_column_defs(df),
                rowData=df.to_dict('records'),
                dashGridOptions={
                    "pagination": True,
                    "paginationPageSize": 50,
                    "domLayout": "normal",
                    "defaultColDef": {
                        "sortable": True,
                        "filter": True,
                        "resizable": True
                    }
                },
                style={"height": "600px"}
            )
        ])
    ]),
    
    # Format Modal - All components included in initial layout
    dbc.Modal([
        dbc.ModalHeader([
            html.H4("🎨 Format Column", id="format-modal-title")
        ]),
        dbc.ModalBody([
            # Format Type Selection
            dbc.Row([
                dbc.Col([
                    html.Label("Format Type:"),
                    dcc.Dropdown(
                        id='format-type-dropdown',
                        options=[
                            {'label': 'Number Format', 'value': 'number'},
                            {'label': 'Conditional Formatting', 'value': 'conditional'},
                            {'label': 'Text Style', 'value': 'text'}
                        ],
                        value='number',
                        placeholder="Select format type..."
                    )
                ], width=12)
            ], className="mb-3"),
            
            # Number Format Options (always present, controlled by visibility)
            html.Div([
                dbc.Row([
                    dbc.Col([
                        html.Label("Decimal Places:"),
                        dcc.Input(id='decimal-places', type='number', value=2, min=0, max=10)
                    ], width=4),
                    dbc.Col([
                        html.Label("Prefix:"),
                        dcc.Input(id='number-prefix', type='text', placeholder='$')
                    ], width=4),
                    dbc.Col([
                        html.Label("Suffix:"),
                        dcc.Input(id='number-suffix', type='text', placeholder='%')
                    ], width=4)
                ])
            ], id='number-format-options', style={'display': 'block'}),
            
            # Conditional Format Options (always present, controlled by visibility)
            html.Div([
                dbc.Row([
                    dbc.Col([
                        html.Label("Condition Type:"),
                        dcc.Dropdown(
                            id='condition-type-dropdown',
                            options=[
                                {'label': 'None', 'value': 'none'},
                                {'label': 'Color Scale', 'value': 'color_scale'},
                                {'label': 'Data Bars', 'value': 'data_bars'},
                                {'label': 'Icon Sets', 'value': 'icon_sets'}
                            ],
                            value='none'
                        )
                    ], width=12)
                ], className="mb-3"),
                
                # Color Scale Options (always present)
                html.Div([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Min Value:"),
                            dcc.Input(id='color-min-value', type='number', value=0)
                        ], width=3),
                        dbc.Col([
                            html.Label("Max Value:"),
                            dcc.Input(id='color-max-value', type='number', value=100)
                        ], width=3),
                        dbc.Col([
                            html.Label("Start Color:"),
                            dcc.Input(id='color-start', type='color', value='#ff0000')
                        ], width=3),
                        dbc.Col([
                            html.Label("End Color:"),
                            dcc.Input(id='color-end', type='color', value='#00ff00')
                        ], width=3)
                    ])
                ], id='color-scale-options', style={'display': 'none'}),
                
                # Data Bars Options (always present)
                html.Div([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Bar Color:"),
                            dcc.Input(id='bar-color', type='color', value='#007bff')
                        ], width=6),
                        dbc.Col([
                            html.Label("Show Values:"),
                            dcc.Checklist(
                                id='show-bar-values',
                                options=[{'label': 'Show', 'value': 'show'}],
                                value=[]
                            )
                        ], width=6)
                    ])
                ], id='data-bars-options', style={'display': 'none'}),
                
                # Icon Sets Options (always present)
                html.Div([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Icon Set:"),
                            dcc.Dropdown(
                                id='icon-set-dropdown',
                                options=[
                                    {'label': 'Traffic Light', 'value': 'traffic'},
                                    {'label': 'Arrows', 'value': 'arrows'},
                                    {'label': 'Stars', 'value': 'stars'}
                                ],
                                value='traffic'
                            )
                        ], width=12)
                    ])
                ], id='icon-sets-options', style={'display': 'none'})
            ], id='conditional-format-options', style={'display': 'none'}),
            
            # Text Style Options (always present, controlled by visibility)
            html.Div([
                dbc.Row([
                    dbc.Col([
                        html.Label("Font Weight:"),
                        dcc.Dropdown(
                            id='font-weight-dropdown',
                            options=[
                                {'label': 'Normal', 'value': 'normal'},
                                {'label': 'Bold', 'value': 'bold'},
                                {'label': 'Light', 'value': '300'}
                            ],
                            value='normal'
                        )
                    ], width=4),
                    dbc.Col([
                        html.Label("Text Color:"),
                        dcc.Input(id='text-color', type='color', value='#000000')
                    ], width=4),
                    dbc.Col([
                        html.Label("Background Color:"),
                        dcc.Input(id='background-color', type='color', value='#ffffff')
                    ], width=4)
                ])
            ], id='text-style-options', style={'display': 'none'})
        ]),
        dbc.ModalFooter([
            dbc.Button("Cancel", id="cancel-format-btn", color="secondary", className="me-2"),
            dbc.Button("Apply Format", id="apply-format-btn", color="primary")
        ])
    ], id="format-modal", is_open=False, size="lg"),
    
    # Toast notifications (always present)
    dbc.Toast(
        id="format-toast",
        header="Format Applied",
        icon="success",
        duration=3000,
        is_open=False,
        style={"position": "fixed", "top": 66, "right": 10, "width": 350, "zIndex": 9999}
    )
], fluid=True)

# Callback to open format modal
@callback(
    [Output("format-modal", "is_open"),
     Output("format-modal-title", "children")],
    [Input("format-column-dropdown", "value")],
    [State("format-modal", "is_open")]
)
def toggle_format_modal(selected_column, is_open):
    if selected_column and not is_open:
        return True, f"🎨 Format Column: {selected_column}"
    return False, "🎨 Format Column"

# Callback to show/hide format options based on format type
@callback(
    [Output("number-format-options", "style"),
     Output("conditional-format-options", "style"),
     Output("text-style-options", "style")],
    [Input("format-type-dropdown", "value")]
)
def update_format_options_visibility(format_type):
    number_style = {'display': 'block' if format_type == 'number' else 'none'}
    conditional_style = {'display': 'block' if format_type == 'conditional' else 'none'}
    text_style = {'display': 'block' if format_type == 'text' else 'none'}
    
    return number_style, conditional_style, text_style

# Callback to show/hide conditional format sub-options
@callback(
    [Output("color-scale-options", "style"),
     Output("data-bars-options", "style"),
     Output("icon-sets-options", "style")],
    [Input("condition-type-dropdown", "value")]
)
def update_conditional_options_visibility(condition_type):
    color_scale_style = {'display': 'block' if condition_type == 'color_scale' else 'none'}
    data_bars_style = {'display': 'block' if condition_type == 'data_bars' else 'none'}
    icon_sets_style = {'display': 'block' if condition_type == 'icon_sets' else 'none'}
    
    return color_scale_style, data_bars_style, icon_sets_style

# Main callback to apply formatting
@callback(
    [Output("data-table", "columnDefs"),
     Output("format-modal", "is_open", allow_duplicate=True),
     Output("format-toast", "is_open"),
     Output("formatted-count", "children")],
    [Input("apply-format-btn", "n_clicks"),
     Input("cancel-format-btn", "n_clicks")],
    [State("format-column-dropdown", "value"),
     State("format-type-dropdown", "value"),
     State("condition-type-dropdown", "value"),
     State("color-min-value", "value"),
     State("color-max-value", "value"),
     State("color-start", "value"),
     State("color-end", "value"),
     State("bar-color", "value"),
     State("text-color", "value"),
     State("background-color", "value"),
     State("font-weight-dropdown", "value"),
     State("decimal-places", "value"),
     State("number-prefix", "value"),
     State("number-suffix", "value"),
     State("data-table", "columnDefs")],
    prevent_initial_call=True
)
def handle_format_actions(apply_clicks, cancel_clicks, selected_column, format_type, 
                         condition_type, color_min, color_max, color_start, color_end,
                         bar_color, text_color, bg_color, font_weight, decimal_places,
                         number_prefix, number_suffix, current_column_defs):
    
    if not ctx.triggered:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if button_id == "cancel-format-btn":
        return dash.no_update, False, False, dash.no_update
    
    if button_id == "apply-format-btn" and selected_column:
        # Create new formatting rules
        formatting_rules = {}
        
        if format_type == 'conditional' and condition_type == 'color_scale':
            # Apply color scale formatting
            if color_min is not None and color_max is not None:
                formatting_rules[selected_column] = {
                    'background': f'linear-gradient(90deg, {color_start}, {color_end})',
                    'color': 'white',
                    'fontWeight': 'bold'
                }
        elif format_type == 'text':
            # Apply text formatting
            formatting_rules[selected_column] = {
                'color': text_color,
                'backgroundColor': bg_color,
                'fontWeight': font_weight
            }
        elif format_type == 'number':
            # Number formatting would typically be handled by cellRenderer
            formatting_rules[selected_column] = {
                'textAlign': 'right',
                'fontFamily': 'monospace'
            }
        
        # Update column definitions
        new_column_defs = get_column_defs(df, formatting_rules)
        
        # Count formatted columns
        formatted_count = len([col for col in new_column_defs if col['headerName'].startswith('🎨')])
        
        return new_column_defs, False, True, str(formatted_count)
    
    return dash.no_update, dash.no_update, dash.no_update, dash.no_update

# Global search callback
@callback(
    Output("data-table", "dashGridOptions"),
    [Input("global-search", "value")]
)
def update_global_search(search_value):
    if search_value:
        return {
            "pagination": True,
            "paginationPageSize": 50,
            "domLayout": "normal",
            "quickFilterText": search_value,
            "defaultColDef": {
                "sortable": True,
                "filter": True,
                "resizable": True
            }
        }
    else:
        return {
            "pagination": True,
            "paginationPageSize": 50,
            "domLayout": "normal",
            "defaultColDef": {
                "sortable": True,
                "filter": True,
                "resizable": True
            }
        }

if __name__ == '__main__':
    app.run_server(debug=True, port=8057) 