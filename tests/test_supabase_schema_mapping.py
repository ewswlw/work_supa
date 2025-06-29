import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from src.pipeline.supabase_processor import SupabaseProcessor
from src.utils.config import SupabaseConfig

# Use a valid JWT format for testing
FAKE_JWT = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"

@patch('src.pipeline.supabase_processor.create_client')
def test_schema_mapping_drops_extra_columns(mock_create_client):
    mock_create_client.return_value = MagicMock()
    config = SupabaseConfig(url='https://fake.supabase.co', key=FAKE_JWT, table='runs')
    processor = SupabaseProcessor(config, MagicMock(), table_name='runs')
    
    # Use actual column names from the runs mapping
    df = pd.DataFrame({
        'Date': ['2023-01-01', '2023-01-02'], 
        'CUSIP': ['123456', '789012'], 
        'Dealer': ['DealerA', 'DealerB'],
        'extra_col': [5, 6]
    })
    schema = ['date', 'cusip', 'dealer', 'security']
    result = processor.validate_and_map_schema(df, schema)
    assert result is not None
    assert 'extra_col' not in result.columns
    assert list(result.columns) == schema

@patch('src.pipeline.supabase_processor.create_client')
def test_schema_mapping_aborts_on_missing_required(mock_create_client):
    mock_create_client.return_value = MagicMock()
    config = SupabaseConfig(url='https://fake.supabase.co', key=FAKE_JWT, table='runs')
    processor = SupabaseProcessor(config, MagicMock(), table_name='runs')  # Use 'runs' table which has critical columns
    
    df = pd.DataFrame({'col1': [1, 2]})
    schema = ['date', 'cusip', 'dealer']  # These are critical columns for 'runs' table
    result = processor.validate_and_map_schema(df, schema)
    assert result is None  # Should return None when critical columns are missing

@patch('src.pipeline.supabase_processor.create_client')
def test_schema_mapping_success(mock_create_client):
    mock_create_client.return_value = MagicMock()
    config = SupabaseConfig(url='https://fake.supabase.co', key=FAKE_JWT, table='runs')
    processor = SupabaseProcessor(config, MagicMock(), table_name='runs')
    
    # Use actual column names that map to the schema
    df = pd.DataFrame({
        'Date': ['2023-01-01', '2023-01-02'], 
        'CUSIP': ['123456', '789012'], 
        'Dealer': ['DealerA', 'DealerB'],
        'Security': ['SecA', 'SecB']
    })
    schema = ['date', 'cusip', 'dealer', 'security']
    result = processor.validate_and_map_schema(df, schema)
    assert result is not None
    assert list(result.columns) == schema
    assert len(result) == 2

# Add more tests for upload and error handling as needed 