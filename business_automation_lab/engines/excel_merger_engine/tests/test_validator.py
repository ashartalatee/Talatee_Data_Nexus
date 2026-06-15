import pytest
from services.validator import ExcelSchemaValidator

@pytest.fixture
def target_schema():
    """Kontrak skema global yang kita harapkan."""
    return {
        "sku": "object",
        "revenue": "float64",
        "qty": "int64"
    }

def test_validator_accepts_perfect_dataframe(target_schema, valid_dataframe):
    """Memastikan data yang benar lolos tanpa hambatan."""
    validator = ExcelSchemaValidator(expected_schema=target_schema)
    
    is_valid, error_msg = validator.validate_structure(valid_dataframe, "file_sempurna.xlsx")
    
    assert is_valid is True
    assert error_msg is None

def test_validator_catches_missing_column(target_schema, invalid_dataframe_missing_col):
    """Memastikan sistem mendeteksi jika ada kolom esensial yang hilang."""
    validator = ExcelSchemaValidator(expected_schema=target_schema)
    
    is_valid, error_msg = validator.validate_structure(invalid_dataframe_missing_col, "file_cacat_kolom.xlsx")
    
    assert is_valid is False
    assert "kehilangan kolom wajib" in error_msg

def test_validator_catches_wrong_data_type(target_schema, invalid_dataframe_wrong_type):
    """Memastikan sistem menolak data jika tipe data terdeteksi rusak/inkonsisten."""
    validator = ExcelSchemaValidator(expected_schema=target_schema)
    
    is_valid, error_msg = validator.validate_structure(invalid_dataframe_wrong_type, "file_salah_tipe.xlsx")
    
    assert is_valid is False
    assert "tidak valid. Diharapkan numerik." in error_msg