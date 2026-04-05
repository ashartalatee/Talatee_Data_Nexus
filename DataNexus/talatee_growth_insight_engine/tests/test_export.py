"""
Unit tests for export module.
Tests CSV, Excel, and PDF report generation.
"""

import pytest
import pandas as pd
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
import pandas.testing as tm

from output import export_reports
from output.exporter import _export_csv_reports, _export_excel_report
from utils.config_loader import get_output_dir


@pytest.fixture
def sample_export_data():
    """Sample data for export testing."""
    
    return {
        'metrics_df': pd.DataFrame({
            'platform': ['Shopee', 'Tokopedia'],
            'total_revenue': [1000000, 500000],
            'total_orders': [100, 50]
        }),
        'summary_df': pd.DataFrame({
            'Metric': ['Total Revenue', 'Total Orders'],
            'Value': ['Rp 1.5M', '150']
        }),
        'insights': [
            {'title': 'Test Insight 1', 'priority': 1},
            {'title': 'Test Insight 2', 'priority': 2}
        ]
    }


@pytest.fixture
def export_config():
    """Export configuration."""
    return {
        "client_id": "test_export",
        "export": {
            "format": ["csv", "xlsx"],
            "include_raw": False,
            "output_dir": "tests/export_test"
        }
    }


@pytest.fixture(autouse=True)
def cleanup_exports():
    """Cleanup export files after tests."""
    yield
    export_path = Path("tests/export_test")
    if export_path.exists():
        for file in export_path.glob("*.csv"):
            file.unlink()
        for file in export_path.glob("*.xlsx"):
            file.unlink()
        export_path.rmdir()


def test_export_reports_creates_files(sample_export_data, export_config, tmp_path):
    """Test main export function creates files."""
    
    export_config['export']['output_dir'] = str(tmp_path / "reports")
    
    result_path = export_reports(
        "test_client", export_config,
        metrics_df=sample_export_data['metrics_df'],
        summary_df=sample_export_data['summary_df'],
        insights=sample_export_data['insights']
    )
    
    assert result_path.exists()
    assert len(list(result_path.glob("*.csv"))) >= 2
    assert len(list(result_path.glob("*.xlsx"))) == 1


def test_csv_export(sample_export_data, export_config, tmp_path):
    """Test CSV export functionality."""
    
    output_dir = tmp_path / "csv_test"
    filename = "test_report"
    
    paths = _export_csv_reports(
        output_dir, filename, False,
        metrics_df=sample_export_data['metrics_df'],
        summary_df=sample_export_data['summary_df'],
        insights=sample_export_data['insights']
    )
    
    assert len(paths) >= 2
    assert all(p.exists() for p in paths)
    
    # Verify metrics CSV
    metrics_path = next(p for p in paths if 'metrics' in p.name)
    metrics_loaded = pd.read_csv(metrics_path)
    tm.assert_frame_equal(sample_export_data['metrics_df'], metrics_loaded)


def test_excel_export(sample_export_data, export_config, tmp_path):
    """Test Excel multi-sheet export."""
    
    filename = tmp_path / "test_export.xlsx"
    
    result_path = _export_excel_report(
        tmp_path, "test_export.xlsx", False,
        metrics_df=sample_export_data['metrics_df'],
        summary_df=sample_export_data['summary_df'],
        insights=sample_export_data['insights']
    )
    
    assert result_path.exists()
    
    # Verify Excel sheets
    xl_file = pd.ExcelFile(result_path)
    assert len(xl_file.sheet_names) >= 3
    assert '📊 Executive Summary' in xl_file.sheet_names
    assert '📈 Metrics' in xl_file.sheet_names
    assert '💡 Insights & Actions' in xl_file.sheet_names


def test_export_handles_empty_data(export_config, tmp_path):
    """Test export with empty/None data."""
    
    export_config['export']['output_dir'] = str(tmp_path / "empty_test")
    
    result_path = export_reports(
        "test_client", export_config,
        metrics_df=pd.DataFrame(),
        summary_df=None,
        insights=[]
    )
    
    assert result_path.exists()
    # Should still create directory even with empty data


def test_export_config_variations(export_config, tmp_path):
    """Test different export configurations."""
    
    # Test CSV only
    export_config['export']['format'] = ['csv']
    export_config['export']['output_dir'] = str(tmp_path / "csv_only")
    
    result1 = export_reports("test", export_config, metrics_df=pd.DataFrame({'a': [1]}))
    assert len(list(result1.glob("*.csv"))) > 0
    assert len(list(result1.glob("*.xlsx"))) == 0
    
    # Test Excel only
    export_config['export']['format'] = ['xlsx']
    export_config['export']['output_dir'] = str(tmp_path / "xlsx_only")
    
    result2 = export_reports("test", export_config, metrics_df=pd.DataFrame({'a': [1]}))
    assert len(list(result2.glob("*.xlsx"))) > 0
    assert len(list(result2.glob("*.csv"))) == 0


def test_filename_timestamping(export_config, tmp_path):
    """Test dynamic filename generation."""
    
    export_config['export']['output_dir'] = str(tmp_path)
    export_config['export']['filename_template'] = "{client_id}_{timestamp}_report"
    
    before = datetime.now()
    result1 = export_reports("clientA", export_config, metrics_df=pd.DataFrame({'a': [1]}))
    after = datetime.now()
    
    files = list(result1.glob("clientA_*.csv"))
    assert len(files) > 0
    
    # Verify timestamp format
    timestamp_str = files[0].stem.split('_')[1]
    try:
        timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
        assert before <= timestamp <= after
    except ValueError:
        pytest.fail("Invalid timestamp format")


def test_include_raw_data_flag(sample_export_data, export_config, tmp_path):
    """Test raw data inclusion."""
    
    raw_df = pd.DataFrame({'raw_col': [1, 2, 3]})
    
    # Without raw data
    export_config['export']['include_raw'] = False
    export_config['export']['output_dir'] = str(tmp_path / "no_raw")
    
    result1 = export_reports("test", export_config, raw_data=raw_df)
    csv_files1 = list(result1.glob("*.csv"))
    assert all('raw' not in f.name for f in csv_files1)
    
    # With raw data
    export_config['export']['include_raw'] = True
    export_config['export']['output_dir'] = str(tmp_path / "with_raw")
    
    result2 = export_reports("test", export_config, raw_data=raw_df)
    csv_files2 = list(result2.glob("*.csv"))
    assert any('raw' in f.name for f in csv_files2)


@patch('output.exporter.pd.ExcelWriter')
def test_excel_writer(mock_writer, sample_export_data, tmp_path):
    """Test ExcelWriter context manager."""
    
    filename = tmp_path / "mock_excel.xlsx"
    
    _export_excel_report(tmp_path, "mock_excel.xlsx", False,
                        metrics_df=sample_export_data['metrics_df'])
    
    # Verify ExcelWriter was called
    mock_writer.assert_called_once()
    
    # Verify sheets were written
    assert mock_writer.return_value.sheets_called


def test_data_catalog_generation(tmp_path):
    """Test data catalog creation."""
    
    from output.exporter import save_data_catalog
    
    dataframes = {
        'metrics': pd.DataFrame({'a': [1, 2]}),
        'summary': pd.DataFrame({'b': [3, 4]})
    }
    
    catalog_path = save_data_catalog(dataframes, tmp_path)
    
    assert catalog_path.exists()
    catalog_df = pd.read_csv(catalog_path)
    assert len(catalog_df) == 2
    assert 'Rows' in catalog_df.columns
    assert 'Size_MB' in catalog_df.columns


@pytest.mark.skipif("reportlab" not in pytest.importorskip("reportlab", reason="reportlab not installed"))
def test_pdf_export(sample_export_data, tmp_path):
    """Test PDF summary generation (requires reportlab)."""
    
    from output.exporter import create_pdf_summary
    
    summary_df = pd.DataFrame({
        'Metric': ['Revenue', 'Orders'],
        'Value': ['Rp 1.5M', '150']
    })
    
    pdf_path = create_pdf_summary(summary_df, tmp_path / "test.pdf", 
                                 sample_export_data['insights'], "Test Client")
    
    assert pdf_path is not None
    assert pdf_path.exists()


def test_export_custom_directory(tmp_path):
    """Test custom output directory."""
    
    config = {
        "client_id": "custom_dir",
        "export": {
            "format": ["csv"],
            "output_dir": str(tmp_path / "custom")
        }
    }
    
    result = export_reports("test", config, metrics_df=pd.DataFrame({'a': [1]}))
    
    assert result == Path(tmp_path / "custom")
    assert len(list(result.glob("*.csv"))) > 0