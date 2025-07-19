"""
Tests for CUSIP Standardization

This module tests the CUSIP standardization functionality including
validation, pattern matching, check digit calculation, and batch processing.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sys
from unittest.mock import Mock, patch

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from db.utils.cusip_standardizer import CUSIPStandardizer


class TestCUSIPStandardizer:
    """Test CUSIPStandardizer class functionality."""
    
    @pytest.fixture
    def mock_logger(self):
        """Create mock logger."""
        return Mock()
    
    @pytest.fixture
    def standardizer(self, mock_logger):
        """Create CUSIPStandardizer instance."""
        return CUSIPStandardizer(logger=mock_logger)
    
    def test_initialization(self, mock_logger):
        """Test CUSIPStandardizer initialization."""
        standardizer = CUSIPStandardizer(logger=mock_logger)
        
        assert standardizer.logger == mock_logger
        assert standardizer.enable_check_digit_validation is True
        
        # Check patterns
        assert 'standard_9' in standardizer.patterns
        assert 'standard_8' in standardizer.patterns
        assert 'cdx' in standardizer.patterns
        assert 'cash' in standardizer.patterns
        
        # Check special mappings
        assert '123' in standardizer.special_mappings
        assert standardizer.special_mappings['123'] == '123000002'
    
    def test_initialization_without_check_digit_validation(self, mock_logger):
        """Test initialization without check digit validation."""
        standardizer = CUSIPStandardizer(logger=mock_logger, enable_check_digit_validation=False)
        
        assert standardizer.enable_check_digit_validation is False
    
    def test_standardize_cusip_empty(self, standardizer):
        """Test standardization of empty CUSIP."""
        result = standardizer.standardize_cusip('')
        
        assert result['cusip_original'] == ''
        assert result['cusip_standardized'] == ''
        assert result['standardization_status'] == 'invalid'
        assert 'Empty or null CUSIP' in result['standardization_message']
    
    def test_standardize_cusip_none(self, standardizer):
        """Test standardization of None CUSIP."""
        result = standardizer.standardize_cusip(None)
        
        assert result['cusip_original'] == ''
        assert result['cusip_standardized'] == ''
        assert result['standardization_status'] == 'invalid'
        assert 'Empty or null CUSIP' in result['standardization_message']
    
    def test_standardize_cusip_nan(self, standardizer):
        """Test standardization of NaN CUSIP."""
        result = standardizer.standardize_cusip(np.nan)
        
        assert result['cusip_original'] == ''
        assert result['cusip_standardized'] == ''
        assert result['standardization_status'] == 'invalid'
        assert 'Empty or null CUSIP' in result['standardization_message']
    
    def test_standardize_cusip_special_mapping(self, standardizer):
        """Test standardization with special mapping."""
        result = standardizer.standardize_cusip('123')
        
        assert result['cusip_original'] == '123'
        assert result['cusip_standardized'] == '123000002'
        assert result['standardization_status'] == 'mapped'
        assert 'Special mapping applied' in result['standardization_message']
    
    def test_standardize_cusip_standard_9(self, standardizer):
        """Test standardization of 9-character CUSIP."""
        # Valid 9-character CUSIP (check digit calculated)
        result = standardizer.standardize_cusip('912810TM0')
        
        assert result['cusip_original'] == '912810TM0'
        assert result['cusip_standardized'] == '912810TM0'
        assert result['standardization_status'] == 'standardized'
        assert 'Pattern: standard_9' in result['standardization_message']
    
    def test_standardize_cusip_standard_8(self, standardizer):
        """Test standardization of 8-character CUSIP."""
        # 8-character CUSIP (check digit will be added)
        result = standardizer.standardize_cusip('912810TM')
        
        assert result['cusip_original'] == '912810TM'
        assert result['cusip_standardized'] == '912810TM0'  # Check digit added
        assert result['standardization_status'] == 'standardized'
        assert 'Pattern: standard_8' in result['standardization_message']
    
    def test_standardize_cusip_standard_6(self, standardizer):
        """Test standardization of 6-character CUSIP."""
        result = standardizer.standardize_cusip('912810')
        
        assert result['cusip_original'] == '912810'
        assert result['cusip_standardized'] == '912810000'  # Padded with zeros
        assert result['standardization_status'] == 'standardized'
        assert 'Pattern: standard_6' in result['standardization_message']
    
    def test_standardize_cusip_cdx(self, standardizer):
        """Test standardization of CDX CUSIP."""
        result = standardizer.standardize_cusip('456')
        
        assert result['cusip_original'] == '456'
        assert result['cusip_standardized'] == '456000000'  # CDX pattern
        assert result['standardization_status'] == 'standardized'
        assert 'Pattern: cdx' in result['standardization_message']
    
    def test_standardize_cusip_cash(self, standardizer):
        """Test standardization of CASH CUSIP."""
        result = standardizer.standardize_cusip('CASH USD')
        
        assert result['cusip_original'] == 'CASH USD'
        assert result['cusip_standardized'] == 'CASHUSD'  # Spaces removed
        assert result['standardization_status'] == 'standardized'
        assert 'Pattern: cash' in result['standardization_message']
    
    def test_standardize_cusip_invalid_pattern(self, standardizer):
        """Test standardization of invalid CUSIP pattern."""
        result = standardizer.standardize_cusip('INVALID!@#')
        
        assert result['cusip_original'] == 'INVALID!@#'
        assert result['cusip_standardized'] == 'INVALID!@#'
        assert result['standardization_status'] == 'invalid'
        assert 'No pattern match' in result['standardization_message']
    
    def test_standardize_cusip_check_digit_validation_failure(self, standardizer):
        """Test standardization with check digit validation failure."""
        # Invalid 9-character CUSIP (wrong check digit)
        result = standardizer.standardize_cusip('912810TM1')
        
        assert result['cusip_original'] == '912810TM1'
        assert result['cusip_standardized'] == '912810TM1'
        assert result['standardization_status'] == 'invalid'
        assert 'Check digit validation failed' in result['standardization_message']
    
    def test_standardize_cusip_without_check_digit_validation(self, mock_logger):
        """Test standardization without check digit validation."""
        standardizer = CUSIPStandardizer(logger=mock_logger, enable_check_digit_validation=False)
        
        # Invalid 9-character CUSIP should pass without check digit validation
        result = standardizer.standardize_cusip('912810TM1')
        
        assert result['cusip_original'] == '912810TM1'
        assert result['cusip_standardized'] == '912810TM1'
        assert result['standardization_status'] == 'standardized'
        assert 'Pattern: standard_9' in result['standardization_message']
    
    def test_standardize_cusip_batch(self, standardizer):
        """Test batch CUSIP standardization."""
        cusips = pd.Series(['912810TM0', '123', '456', 'INVALID!@#', ''])
        
        results = standardizer.standardize_cusip_batch(cusips)
        
        assert len(results) == 5
        assert isinstance(results, pd.DataFrame)
        
        # Check results
        assert results.iloc[0]['cusip_original'] == '912810TM0'
        assert results.iloc[0]['standardization_status'] == 'standardized'
        
        assert results.iloc[1]['cusip_original'] == '123'
        assert results.iloc[1]['standardization_status'] == 'mapped'
        
        assert results.iloc[2]['cusip_original'] == '456'
        assert results.iloc[2]['standardization_status'] == 'standardized'
        
        assert results.iloc[3]['cusip_original'] == 'INVALID!@#'
        assert results.iloc[3]['standardization_status'] == 'invalid'
        
        assert results.iloc[4]['cusip_original'] == ''
        assert results.iloc[4]['standardization_status'] == 'invalid'
    
    def test_validate_cusip_valid(self, standardizer):
        """Test CUSIP validation with valid CUSIP."""
        result = standardizer.validate_cusip('912810TM0')
        
        assert result['cusip'] == '912810TM0'
        assert result['is_valid'] is True
        assert result['cusip_type'] == 'standard_9'
        assert 'Valid standard_9 CUSIP' in result['validation_message']
    
    def test_validate_cusip_invalid(self, standardizer):
        """Test CUSIP validation with invalid CUSIP."""
        result = standardizer.validate_cusip('INVALID!@#')
        
        assert result['cusip'] == 'INVALID!@#'
        assert result['is_valid'] is False
        assert result['cusip_type'] is None
        assert 'No pattern match' in result['validation_message']
    
    def test_validate_cusip_special_mapping(self, standardizer):
        """Test CUSIP validation with special mapping."""
        result = standardizer.validate_cusip('123')
        
        assert result['cusip'] == '123'
        assert result['is_valid'] is True
        assert result['cusip_type'] == 'special_mapping'
        assert 'Special CUSIP mapping' in result['validation_message']
    
    def test_validate_check_digit_valid(self, standardizer):
        """Test check digit validation with valid CUSIP."""
        # Valid 9-character CUSIP
        assert standardizer._validate_check_digit('912810TM0') is True
    
    def test_validate_check_digit_invalid(self, standardizer):
        """Test check digit validation with invalid CUSIP."""
        # Invalid 9-character CUSIP (wrong check digit)
        assert standardizer._validate_check_digit('912810TM1') is False
    
    def test_validate_check_digit_wrong_length(self, standardizer):
        """Test check digit validation with wrong length."""
        # 8-character CUSIP
        assert standardizer._validate_check_digit('912810TM') is False
    
    def test_add_check_digit(self, standardizer):
        """Test adding check digit to 8-character CUSIP."""
        # 8-character CUSIP
        result = standardizer._add_check_digit('912810TM')
        
        assert result == '912810TM0'  # Check digit should be 0
    
    def test_add_check_digit_wrong_length(self, standardizer):
        """Test adding check digit with wrong length."""
        # 6-character CUSIP
        result = standardizer._add_check_digit('912810')
        
        assert result == '912810'  # Should return original if not 8 characters
    
    def test_match_pattern_standard_9(self, standardizer):
        """Test pattern matching for 9-character CUSIP."""
        result = standardizer._match_pattern('912810TM0')
        
        assert result is not None
        assert result['type'] == 'standard_9'
        assert result['pattern'] == standardizer.patterns['standard_9']
    
    def test_match_pattern_standard_8(self, standardizer):
        """Test pattern matching for 8-character CUSIP."""
        result = standardizer._match_pattern('912810TM')
        
        assert result is not None
        assert result['type'] == 'standard_8'
        assert result['pattern'] == standardizer.patterns['standard_8']
    
    def test_match_pattern_cdx(self, standardizer):
        """Test pattern matching for CDX CUSIP."""
        result = standardizer._match_pattern('456')
        
        assert result is not None
        assert result['type'] == 'cdx'
        assert result['pattern'] == standardizer.patterns['cdx']
    
    def test_match_pattern_cash(self, standardizer):
        """Test pattern matching for CASH CUSIP."""
        result = standardizer._match_pattern('CASH USD')
        
        assert result is not None
        assert result['type'] == 'cash'
        assert result['pattern'] == standardizer.patterns['cash']
    
    def test_match_pattern_no_match(self, standardizer):
        """Test pattern matching with no match."""
        result = standardizer._match_pattern('INVALID!@#')
        
        assert result is None
    
    def test_get_cusip_statistics(self, standardizer):
        """Test CUSIP statistics calculation."""
        cusips = pd.Series(['912810TM0', '123', '456', 'INVALID!@#', ''])
        
        stats = standardizer.get_cusip_statistics(cusips)
        
        assert stats['total_cusips'] == 5
        assert stats['valid_cusips'] == 2  # 912810TM0 and 456 (CDX) are valid
        assert stats['invalid_cusips'] == 2  # INVALID!@# and empty string
        assert stats['standardized_cusips'] == 3  # 912810TM0, 123, 456
        assert stats['validation_rate'] == 40.0  # 2/5 * 100
        assert stats['standardization_rate'] == 60.0  # 3/5 * 100
        
        # Check CUSIP types
        assert 'standard_9' in stats['cusip_types']
        assert 'special_mapping' in stats['cusip_types']
        assert 'cdx' in stats['cusip_types']
        assert 'unknown' in stats['cusip_types']
        assert 'empty' in stats['cusip_types']
    
    def test_get_cusip_statistics_empty(self, standardizer):
        """Test CUSIP statistics with empty series."""
        cusips = pd.Series([])
        
        stats = standardizer.get_cusip_statistics(cusips)
        
        assert stats['total_cusips'] == 0
        assert stats['valid_cusips'] == 0
        assert stats['invalid_cusips'] == 0
        assert stats['standardized_cusips'] == 0
        assert stats['validation_rate'] == 0.0
        assert stats['standardization_rate'] == 0.0
    
    def test_add_special_mapping(self, standardizer):
        """Test adding special CUSIP mapping."""
        standardizer.add_special_mapping('TEST', 'TEST000001')
        
        assert 'TEST' in standardizer.special_mappings
        assert standardizer.special_mappings['TEST'] == 'TEST000001'
    
    def test_remove_special_mapping(self, standardizer):
        """Test removing special CUSIP mapping."""
        # Add mapping first
        standardizer.add_special_mapping('TEST', 'TEST000001')
        assert 'TEST' in standardizer.special_mappings
        
        # Remove mapping
        standardizer.remove_special_mapping('TEST')
        assert 'TEST' not in standardizer.special_mappings
    
    def test_remove_special_mapping_nonexistent(self, standardizer):
        """Test removing nonexistent special mapping."""
        # Should not raise exception
        standardizer.remove_special_mapping('NONEXISTENT')
    
    def test_get_special_mappings(self, standardizer):
        """Test getting all special mappings."""
        mappings = standardizer.get_special_mappings()
        
        assert isinstance(mappings, dict)
        assert '123' in mappings
        assert '789' in mappings
        assert mappings['123'] == '123000002'
        assert mappings['789'] == '789000007'
    
    def test_clear_special_mappings(self, standardizer):
        """Test clearing all special mappings."""
        # Verify mappings exist
        assert len(standardizer.special_mappings) > 0
        
        # Clear mappings
        standardizer.clear_special_mappings()
        
        assert len(standardizer.special_mappings) == 0
    
    def test_case_insensitive_standardization(self, standardizer):
        """Test that CUSIP standardization is case insensitive."""
        # Test lowercase CUSIP
        result = standardizer.standardize_cusip('912810tm0')
        
        assert result['cusip_original'] == '912810TM0'  # Should be converted to uppercase
        assert result['cusip_standardized'] == '912810TM0'
        assert result['standardization_status'] == 'standardized'
    
    def test_whitespace_handling(self, standardizer):
        """Test handling of whitespace in CUSIPs."""
        # Test CUSIP with leading/trailing whitespace
        result = standardizer.standardize_cusip('  912810TM0  ')
        
        assert result['cusip_original'] == '912810TM0'  # Should be stripped
        assert result['cusip_standardized'] == '912810TM0'
        assert result['standardization_status'] == 'standardized'
    
    def test_logging_integration(self, mock_logger):
        """Test integration with logging system."""
        standardizer = CUSIPStandardizer(logger=mock_logger)
        
        # Standardize a CUSIP
        result = standardizer.standardize_cusip('912810TM0')
        
        # Check that logger was called
        assert mock_logger.log_cusip_operation.called
    
    def test_error_handling(self, standardizer):
        """Test error handling during standardization."""
        # Mock _match_pattern to raise exception
        with patch.object(standardizer, '_match_pattern', side_effect=Exception("Test error")):
            result = standardizer.standardize_cusip('912810TM0')
            
            assert result['cusip_original'] == '912810TM0'
            assert result['cusip_standardized'] == '912810TM0'
            assert result['standardization_status'] == 'error'
            assert 'Standardization error' in result['standardization_message']
    
    def test_context_parameter(self, standardizer):
        """Test context parameter in standardization."""
        context = {'source': 'test', 'operation': 'validation'}
        
        result = standardizer.standardize_cusip('912810TM0', context=context)
        
        assert result['cusip_original'] == '912810TM0'
        assert result['standardization_status'] == 'standardized'
        
        # Check that context was passed to logger
        standardizer.logger.log_cusip_operation.assert_called_with(
            'standardize', '912810TM0', '912810TM0', True, context
        ) 