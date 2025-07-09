import dash
from dash import dcc, html, Input, Output, State, callback, ctx
import dash_ag_grid as dag
import pandas as pd
from pathlib import Path
import json
import dash_bootstrap_components as dbc

# Load the data
parquet_path = Path('historical g spread/bond_z.parquet')
df = pd.read_parquet(parquet_path)

# Define column configurations with visual indicators (simple text only)
def get_column_defs(df, pinned_columns=None, formatting_rules=None):
    if pinned_columns is None:
        pinned_columns = {}
    if formatting_rules is None:
        formatting_rules = {}
    
    column_defs = []
    for col in df.columns:
        # Check if column has custom formatting
        has_custom_format = col in formatting_rules
        
        # Create simple header name with emoji indicator only
        if has_custom_format:
            header_name = f"🎨 {col}"
        else:
            header_name = col
        
        col_def = {
            "headerName": header_name,
            "field": col,
            "filter": True,
            "sortable": True,
            "resizable": True,
            "checkboxSelection": True if col == df.columns[0] else False,
            "headerCheckboxSelection": True if col == df.columns[0] else False,
            "pinned": pinned_columns.get(col, None),
            "width": 150,
            "minWidth": 80,
            "maxWidth": 400,
        }
        
        # Add custom header styling for formatted columns
        if has_custom_format:
            col_def["headerClass"] = "formatted-column-header"
        
        # Apply custom formatting if exists
        if col in formatting_rules:
            format_rule = formatting_rules[col]
            
            # Apply number formatting
            if format_rule.get('number_format'):
                nf = format_rule['number_format']
                if nf['type'] == 'decimal':
                    decimals = nf.get('decimals', 2)
                    col_def["valueFormatter"] = {"function": f"d3.format(',.{decimals}f')(params.value)"}
                elif nf['type'] == 'currency':
                    symbol = nf.get('symbol', '$')
                    decimals = nf.get('decimals', 2)
                    col_def["valueFormatter"] = {"function": f"'{symbol}' + d3.format(',.{decimals}f')(params.value)"}
                elif nf['type'] == 'percentage':
                    decimals = nf.get('decimals', 1)
                    col_def["valueFormatter"] = {"function": f"d3.format(',.{decimals}%')(params.value/100)"}
                elif nf['type'] == 'scientific':
                    col_def["valueFormatter"] = {"function": "d3.format('.2e')(params.value)"}
            
            # Apply conditional formatting
            if format_rule.get('conditional_format'):
                cf = format_rule['conditional_format']
                if cf['type'] == 'color_scale':
                    min_val = cf.get('min_value', 0)
                    max_val = cf.get('max_value', 100)
                    start_color = cf.get('start_color', '#ff0000')
                    end_color = cf.get('end_color', '#00ff00')
                    
                    # Convert hex to RGB
                    def hex_to_rgb(hex_color):
                        hex_color = hex_color.lstrip('#')
                        return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
                    
                    start_rgb = hex_to_rgb(start_color)
                    end_rgb = hex_to_rgb(end_color)
                    
                    col_def["cellStyle"] = {
                        "function": f"""
                        function(params) {{
                            if (params.value == null) return {{}};
                            var value = parseFloat(params.value);
                            var min = {min_val};
                            var max = {max_val};
                            var ratio = Math.max(0, Math.min(1, (value - min) / (max - min)));
                            
                            var r = Math.round({start_rgb[0]} * (1 - ratio) + {end_rgb[0]} * ratio);
                            var g = Math.round({start_rgb[1]} * (1 - ratio) + {end_rgb[1]} * ratio);
                            var b = Math.round({start_rgb[2]} * (1 - ratio) + {end_rgb[2]} * ratio);
                            
                            return {{
                                backgroundColor: 'rgb(' + r + ',' + g + ',' + b + ')',
                                color: ratio > 0.5 ? 'white' : 'black',
                                textAlign: 'right'
                            }};
                        }}
                        """
                    }
                elif cf['type'] == 'data_bars':
                    max_val = cf.get('max_value', 100)
                    bar_color = cf.get('bar_color', '#4CAF50')
                    
                    col_def["cellRenderer"] = {
                        "function": f"""
                        function(params) {{
                            if (params.value == null) return '';
                            var value = parseFloat(params.value);
                            var max = {max_val};
                            var percentage = Math.max(0, Math.min(100, (value / max) * 100));
                            
                            return '<div style="display: flex; align-items: center; height: 100%; position: relative;">' +
                                   '<div style="background: linear-gradient(to right, {bar_color} ' + percentage + '%, transparent ' + percentage + '%); width: 100%; height: 20px; border-radius: 3px; position: absolute; top: 50%; transform: translateY(-50%);"></div>' +
                                   '<span style="z-index: 1; font-weight: bold; position: relative; padding: 0 5px;">' + params.value + '</span>' +
                                   '</div>';
                        }}
                        """
                    }
            
            # Apply text formatting
            if format_rule.get('text_format'):
                tf = format_rule['text_format']
                style_obj = {
                    "textAlign": tf.get('alignment', 'left'),
                    "fontWeight": tf.get('font_weight', 'normal'),
                    "fontSize": f"{tf.get('font_size', 14)}px",
                    "color": tf.get('text_color', '#ffffff'),
                    "backgroundColor": tf.get('background_color', 'transparent')
                }
                
                # Merge with existing cellStyle if it exists
                if "cellStyle" in col_def and isinstance(col_def["cellStyle"], dict):
                    col_def["cellStyle"].update(style_obj)
                elif "cellStyle" not in col_def:
                    col_def["cellStyle"] = style_obj
        
        # Apply default formatting based on column type and name (if no custom formatting)
        elif df[col].dtype in ['float64', 'int64']:
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
                            if (params.value > 2) return {backgroundColor: '#ff4444', color: 'white', textAlign: 'right'};
                            if (params.value > 1) return {backgroundColor: '#ffaa44', color: 'white', textAlign: 'right'};
                            if (params.value < -2) return {backgroundColor: '#44ff44', color: 'white', textAlign: 'right'};
                            if (params.value < -1) return {backgroundColor: '#aaffaa', color: 'black', textAlign: 'right'};
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

# Custom CSS for formatted column headers
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            .formatted-column-header {
                background-color: #28a745 !important;
                color: white !important;
                font-weight: bold !important;
            }
            .ag-header-cell-text {
                overflow: visible !important;
            }
            .ag-header-cell {
                cursor: pointer !important;
                position: relative;
            }
            .ag-header-cell:hover {
                background-color: #495057 !important;
            }
            .ag-header-cell:hover::after {
                content: "⚙️ Click to format";
                position: absolute;
                bottom: -20px;
                left: 50%;
                transform: translateX(-50%);
                background: #007bff;
                color: white;
                padding: 2px 6px;
                border-radius: 3px;
                font-size: 10px;
                white-space: nowrap;
                z-index: 1000;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# Define the layout
app.layout = dbc.Container([
    html.H1("Z Analytics Dashboard - Column Formatting", className="text-center mb-4"),
    
    # Grid Controls
    dbc.Card([
        dbc.CardHeader([
            html.H5("Grid Controls", className="mb-0")
        ]),
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
                ], width=3),
                dbc.Col([
                    dbc.Label("Pin Columns:"),
                    dcc.Dropdown(
                        id='pin-columns-dropdown',
                        options=[{'label': col, 'value': col} for col in df.columns],
                        value=[],
                        multi=True,
                        placeholder="Select columns to pin left...",
                        style={'color': 'black'}
                    )
                ], width=3),
                dbc.Col([
                    dbc.Label("Row Height:"),
                    dcc.Dropdown(
                        id='row-height-dropdown',
                        options=[
                            {'label': '25px - Compact', 'value': 25},
                            {'label': '35px - Default', 'value': 35},
                            {'label': '50px - Medium', 'value': 50},
                            {'label': '75px - Large', 'value': 75},
                            {'label': '100px - Extra Large', 'value': 100}
                        ],
                        value=35,
                        clearable=False,
                        style={'color': 'black'}
                    )
                ], width=3),
                dbc.Col([
                    dbc.Label("Page Size:"),
                    dcc.Dropdown(
                        id='page-size-selector',
                        options=[
                            {'label': '20', 'value': 20},
                            {'label': '50', 'value': 50},
                            {'label': '100', 'value': 100},
                            {'label': '500', 'value': 500}
                        ],
                        value=100,
                        style={'color': 'black'},
                        clearable=False
                    )
                ], width=3)
            ]),
            html.Hr(),
            dbc.Row([
                dbc.Col([
                    dbc.Button("Export to Excel", id="export-btn", color="primary", className="me-2"),
                    dbc.Button("Export to CSV", id="export-csv-btn", color="secondary", className="me-2"),
                    dbc.Button("Reset Grid", id="reset-btn", color="warning", className="me-2"),
                    dbc.Button("Clear All Formatting", id="clear-all-btn", color="danger"),
                ], width=12)
            ])
        ])
    ], className="mb-4"),
    
    # Grid Container - Full Width
    dbc.Card([
        dbc.CardHeader([
            html.Div([
                html.H5("Data Grid", className="mb-0"),
                dbc.Badge(f"{len(df):,} rows × {len(df.columns)} columns", color="info"),
                html.Small(" • Click on any column header to format", className="text-muted ms-2")
            ], style={"display": "flex", "align-items": "center", "gap": "10px"})
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
                    "domLayout": "autoHeight",
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
                    "suppressContextMenu": False,
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
                    "width": "100%",
                    "max-height": "80vh",
                    "resize": "vertical", 
                    "overflow": "auto"
                },
                className="ag-theme-alpine-dark"
            )
        ], style={"padding": "0"})
    ]),
    
    # Formatting Modal
    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle(id="format-modal-title")),
        dbc.ModalBody([
            # Number Formatting Section
            dbc.Card([
                dbc.CardHeader("📊 Number Formatting"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Format Type:"),
                            dcc.Dropdown(
                                id='modal-number-format-type',
                                options=[
                                    {'label': 'None', 'value': 'none'},
                                    {'label': 'Decimal', 'value': 'decimal'},
                                    {'label': 'Currency', 'value': 'currency'},
                                    {'label': 'Percentage', 'value': 'percentage'},
                                    {'label': 'Scientific', 'value': 'scientific'}
                                ],
                                value='none',
                                style={'color': 'black'}
                            )
                        ], width=6),
                        dbc.Col([
                            html.Div(id='modal-number-format-options')
                        ], width=6)
                    ])
                ])
            ], className="mb-3"),
            
            # Conditional Formatting Section
            dbc.Card([
                dbc.CardHeader("🌈 Conditional Formatting"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Condition Type:"),
                            dcc.Dropdown(
                                id='modal-conditional-format-type',
                                options=[
                                    {'label': 'None', 'value': 'none'},
                                    {'label': 'Color Scale', 'value': 'color_scale'},
                                    {'label': 'Data Bars', 'value': 'data_bars'}
                                ],
                                value='none',
                                style={'color': 'black'}
                            )
                        ], width=12)
                    ]),
                    html.Div(id='modal-conditional-format-options', className="mt-2")
                ])
            ], className="mb-3"),
            
            # Text Formatting Section
            dbc.Card([
                dbc.CardHeader("📝 Text Formatting"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Font Size:"),
                            dcc.Slider(
                                id='modal-font-size-slider',
                                min=8, max=24, step=1, value=14,
                                marks={8: '8px', 14: '14px', 20: '20px', 24: '24px'}
                            )
                        ], width=12, className="mb-3"),
                    ]),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Font Weight:"),
                            dcc.Dropdown(
                                id='modal-font-weight-dropdown',
                                options=[
                                    {'label': 'Normal', 'value': 'normal'},
                                    {'label': 'Bold', 'value': 'bold'}
                                ],
                                value='normal',
                                style={'color': 'black'}
                            )
                        ], width=4),
                        dbc.Col([
                            dbc.Label("Text Alignment:"),
                            dcc.Dropdown(
                                id='modal-text-alignment-dropdown',
                                options=[
                                    {'label': 'Left', 'value': 'left'},
                                    {'label': 'Center', 'value': 'center'},
                                    {'label': 'Right', 'value': 'right'}
                                ],
                                value='left',
                                style={'color': 'black'}
                            )
                        ], width=4),
                        dbc.Col([
                            dbc.Label("Text Color:"),
                            dbc.Input(
                                id='modal-text-color-input',
                                type='color',
                                value='#ffffff',
                                style={'width': '100%', 'height': '38px'}
                            )
                        ], width=4)
                    ], className="mb-3"),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Background Color:"),
                            dbc.Input(
                                id='modal-background-color-input',
                                type='color',
                                value='#000000',
                                style={'width': '100%', 'height': '38px'}
                            )
                        ], width=6),
                        dbc.Col([
                            dbc.Label("Preview:"),
                            html.Div(
                                "Sample Text",
                                id="format-preview",
                                style={
                                    "border": "1px solid #ccc",
                                    "padding": "8px",
                                    "border-radius": "4px",
                                    "text-align": "center",
                                    "background-color": "#000000",
                                    "color": "#ffffff",
                                    "font-size": "14px",
                                    "font-weight": "normal"
                                }
                            )
                        ], width=6)
                    ])
                ])
            ])
        ]),
        dbc.ModalFooter([
            dbc.Button("Clear Format", id="modal-clear-btn", color="warning", className="me-2"),
            dbc.Button("Apply Format", id="modal-apply-btn", color="success", className="me-2"),
            dbc.Button("Cancel", id="modal-cancel-btn", color="secondary")
        ])
    ], id="format-modal", is_open=False, size="lg", centered=True),
    
    # Store for formatting rules and selected column
    dcc.Store(id='grid-data-store', data=df.to_dict('records')),
    dcc.Store(id='formatting-rules-store', data={}),
    dcc.Store(id='selected-column-store', data=None),
    dcc.Download(id="download-excel"),
    dcc.Download(id="download-csv"),
], fluid=True)

# Callback to detect column header clicks and open modal
@callback(
    [Output("format-modal", "is_open"),
     Output("format-modal-title", "children"),
     Output("selected-column-store", "data")],
    Input("z-analytics-grid", "cellClicked"),
    prevent_initial_call=True
)
def open_format_modal_on_header_click(cell_clicked):
    if cell_clicked and cell_clicked.get('rowIndex') is None:  # Header click
        column_id = cell_clicked.get('colId')
        if column_id:
            return True, f"Format Column: {column_id}", column_id
    return False, "", None

# Dynamic formatting options for modal
@callback(
    Output('modal-number-format-options', 'children'),
    Input('modal-number-format-type', 'value')
)
def update_modal_number_format_options(format_type):
    if not format_type or format_type == 'none':
        return []
    
    options = []
    
    if format_type in ['decimal', 'currency', 'percentage']:
        options.append(
            html.Div([
                dbc.Label("Decimal Places:"),
                dcc.Slider(
                    id='modal-decimal-places-slider',
                    min=0, max=8, step=1, value=2,
                    marks={0: '0', 2: '2', 4: '4', 8: '8'}
                )
            ])
        )
    
    if format_type == 'currency':
        options.append(
            html.Div([
                dbc.Label("Currency Symbol:"),
                dcc.Dropdown(
                    id='modal-currency-symbol-dropdown',
                    options=[
                        {'label': '$ (Dollar)', 'value': '$'},
                        {'label': '€ (Euro)', 'value': '€'},
                        {'label': '£ (Pound)', 'value': '£'},
                        {'label': '¥ (Yen)', 'value': '¥'}
                    ],
                    value='$',
                    style={'color': 'black'}
                )
            ], className="mt-2")
        )
    
    return options

@callback(
    Output('modal-conditional-format-options', 'children'),
    Input('modal-conditional-format-type', 'value')
)
def update_modal_conditional_format_options(format_type):
    if not format_type or format_type == 'none':
        return []
    
    if format_type == 'color_scale':
        return [
            dbc.Row([
                dbc.Col([
                    dbc.Label("Min Value:"),
                    dbc.Input(id='modal-color-scale-min', type='number', value=0)
                ], width=3),
                dbc.Col([
                    dbc.Label("Max Value:"),
                    dbc.Input(id='modal-color-scale-max', type='number', value=100)
                ], width=3),
                dbc.Col([
                    dbc.Label("Start Color:"),
                    dbc.Input(id='modal-color-scale-start', type='color', value='#ff0000')
                ], width=3),
                dbc.Col([
                    dbc.Label("End Color:"),
                    dbc.Input(id='modal-color-scale-end', type='color', value='#00ff00')
                ], width=3)
            ])
        ]
    
    elif format_type == 'data_bars':
        return [
            dbc.Row([
                dbc.Col([
                    dbc.Label("Max Value:"),
                    dbc.Input(id='modal-data-bars-max', type='number', value=100)
                ], width=6),
                dbc.Col([
                    dbc.Label("Bar Color:"),
                    dbc.Input(id='modal-data-bars-color', type='color', value='#4CAF50')
                ], width=6)
            ])
        ]
    
    return []

# Preview update callback
@callback(
    Output("format-preview", "style"),
    [Input("modal-font-size-slider", "value"),
     Input("modal-font-weight-dropdown", "value"),
     Input("modal-text-alignment-dropdown", "value"),
     Input("modal-text-color-input", "value"),
     Input("modal-background-color-input", "value")]
)
def update_format_preview(font_size, font_weight, text_align, text_color, bg_color):
    return {
        "border": "1px solid #ccc",
        "padding": "8px",
        "border-radius": "4px",
        "text-align": text_align,
        "background-color": bg_color,
        "color": text_color,
        "font-size": f"{font_size}px",
        "font-weight": font_weight
    }

# Modal action callbacks
@callback(
    [Output("format-modal", "is_open", allow_duplicate=True),
     Output('formatting-rules-store', 'data')],
    [Input("modal-apply-btn", "n_clicks"),
     Input("modal-cancel-btn", "n_clicks"),
     Input("modal-clear-btn", "n_clicks")],
    [State("selected-column-store", "data"),
     State("modal-number-format-type", "value"),
     State("modal-conditional-format-type", "value"),
     State("modal-font-size-slider", "value"),
     State("modal-font-weight-dropdown", "value"),
     State("modal-text-alignment-dropdown", "value"),
     State("modal-text-color-input", "value"),
     State("modal-background-color-input", "value"),
     State("formatting-rules-store", "data")],
    prevent_initial_call=True
)
def handle_modal_actions(apply_clicks, cancel_clicks, clear_clicks, selected_column,
                        number_format, conditional_format, font_size, font_weight,
                        text_align, text_color, bg_color, current_rules):
    
    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if triggered_id == "modal-cancel-btn":
        return False, current_rules
    
    if triggered_id == "modal-clear-btn" and selected_column:
        new_rules = current_rules.copy()
        if selected_column in new_rules:
            del new_rules[selected_column]
        return False, new_rules
    
    if triggered_id == "modal-apply-btn" and selected_column:
        new_rules = current_rules.copy()
        
        if selected_column not in new_rules:
            new_rules[selected_column] = {}
        
        # Apply number formatting
        if number_format and number_format != 'none':
            new_rules[selected_column]['number_format'] = {
                'type': number_format,
                'decimals': 2
            }
        
        # Apply conditional formatting
        if conditional_format and conditional_format != 'none':
            new_rules[selected_column]['conditional_format'] = {
                'type': conditional_format,
                'min_value': 0,
                'max_value': 100,
                'start_color': '#ff0000',
                'end_color': '#00ff00',
                'bar_color': '#4CAF50'
            }
        
        # Apply text formatting
        new_rules[selected_column]['text_format'] = {
            'font_size': font_size,
            'font_weight': font_weight,
            'alignment': text_align,
            'text_color': text_color,
            'background_color': bg_color
        }
        
        return False, new_rules
    
    return False, current_rules

# Main grid update callback
@callback(
    [Output("z-analytics-grid", "columnDefs"),
     Output("z-analytics-grid", "rowData")],
    [Input("column-selector", "value"),
     Input("pin-columns-dropdown", "value"),
     Input("formatting-rules-store", "data")],
    State("grid-data-store", "data")
)
def update_grid(selected_columns, pinned_columns, formatting_rules, stored_data):
    if not selected_columns:
        selected_columns = list(df.columns)
    
    # Create pinning dictionary
    pinning_dict = {col: 'left' for col in pinned_columns} if pinned_columns else {}
    
    filtered_df = df[selected_columns]
    column_defs = get_column_defs(filtered_df, pinning_dict, formatting_rules)
    row_data = filtered_df.to_dict('records')
    
    return column_defs, row_data

@callback(
    Output("z-analytics-grid", "dashGridOptions"),
    [Input("page-size-selector", "value"),
     Input("row-height-dropdown", "value")]
)
def update_grid_options(page_size, row_height):
    return {
        "domLayout": "autoHeight",
        "pagination": True,
        "paginationPageSize": page_size,
        "rowSelection": "multiple",
        "enableRangeSelection": True,
        "enableColumnReorder": True,
        "enableColumnResizing": True,
        "animateRows": True,
        "rowHeight": row_height,
        "headerHeight": 40,
        "enableAdvancedFilter": True,
        "suppressContextMenu": False,
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

# Export callbacks
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

# Reset and clear all formatting callbacks
@callback(
    [Output("column-selector", "value"),
     Output("pin-columns-dropdown", "value"),
     Output("row-height-dropdown", "value"),
     Output('formatting-rules-store', 'data', allow_duplicate=True)],
    [Input("reset-btn", "n_clicks"),
     Input("clear-all-btn", "n_clicks")],
    prevent_initial_call=True
)
def reset_or_clear_grid(reset_clicks, clear_clicks):
    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if triggered_id == "reset-btn":
        return list(df.columns), [], 35, {}
    elif triggered_id == "clear-all-btn":
        return dash.no_update, dash.no_update, dash.no_update, {}
    
    return dash.no_update, dash.no_update, dash.no_update, dash.no_update

# Run the app
if __name__ == "__main__":
    app.run_server(debug=True, port=8056) 