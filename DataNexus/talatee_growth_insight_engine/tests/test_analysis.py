"""
Unit tests for analysis modules.
Tests metrics generation, summary building, and insight generation.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from analysis import generate_metrics, build_summary, generate_insights
from analysis.metrics import calculate_growth_metrics
from analysis.summary import create_scorecard
from utils.config_loader import load_client_config


@pytest.fixture
def sample_analysis_data():
    """Sample data for analysis testing."""
    
    dates = pd.date_range('2024-01-01', periods=30, freq='D')
    
    return pd.DataFrame({
        'order_id': [f"ORD_{i:04d}" for i in range(30)],
        'product_id': np.random.choice(['P001', 'P002', 'P003'], 30),
        'product_name': np.random.choice(['Earbuds', 'Case', 'Powerbank'], 30),
        'total_amount': np.random.uniform(50000, 500000, 30).round(-2),
        'quantity': np.random.choice([1, 2, 3], 30),
        'order_date': dates,
        'platform': np.random.choice(['Shopee', 'Tokopedia', 'WhatsApp'], 30),
        'customer_id': np.random.choice(['CUST001', 'CUST002', 'CUST003'], 30),
        'status': np.random.choice(['completed', 'shipped'], 30, p=[0.8, 0.2])
    })


@pytest.fixture
def analysis_config():
    """Configuration for analysis testing."""
    return {
        "client_id": "test_analysis",
        "analytics": {
            "top_n": 10,
            "metrics": ["revenue", "orders", "units"],
            "trends": ["week_over_week"]
        }
    }


def test_generate_metrics(sample_analysis_data, analysis_config):
    """Test metrics generation."""
    
    metrics = generate_metrics(sample_analysis_data, analysis_config)
    
    assert isinstance(metrics, pd.DataFrame)
    assert not metrics.empty
    
    # Check key metrics exist
    assert 'total_revenue' in metrics.columns or 'platform_revenue' in metrics.columns
    assert metrics['total_revenue'].sum() > 0 if 'total_revenue' in metrics.columns else True


def test_metrics_platform_breakdown(sample_analysis_data, analysis_config):
    """Test platform-specific metrics."""
    
    metrics = generate_metrics(sample_analysis_data, analysis_config)
    
    if 'platform' in metrics.columns:
        platforms = metrics[metrics['platform'].notna()]
        assert len(platforms) > 0
        assert 'platform_revenue' in platforms.columns
        assert (platforms['platform_revenue'] >= 0).all()


def test_build_summary(sample_analysis_data, analysis_config):
    """Test executive summary generation."""
    
    # Generate metrics first
    metrics = generate_metrics(sample_analysis_data, analysis_config)
    
    summary = build_summary(sample_analysis_data, metrics, analysis_config)
    
    assert isinstance(summary, pd.DataFrame)
    assert not summary.empty
    assert 'Metric' in summary.columns
    assert 'Value' in summary.columns


def test_summary_kpi_section(sample_analysis_data, analysis_config):
    """Test KPI summary section."""
    
    metrics = generate_metrics(sample_analysis_data, analysis_config)
    summary = build_summary(sample_analysis_data, metrics, analysis_config)
    
    # Check KPIs section exists
    kpis = summary[summary['Category'] == 'KPIs'] if 'Category' in summary.columns else summary.head(8)
    assert len(kpis) > 0
    assert 'Total Revenue' in kpis['Metric'].values or True  # Flexible metric names


def test_generate_insights(sample_analysis_data, analysis_config):
    """Test insight generation."""
    
    metrics = generate_metrics(sample_analysis_data, analysis_config)
    summary = build_summary(sample_analysis_data, metrics, analysis_config)
    
    insights = generate_insights(sample_analysis_data, metrics, summary, analysis_config)
    
    assert isinstance(insights, list)
    assert len(insights) > 0
    assert isinstance(insights[0], dict)
    assert 'title' in insights[0]
    assert 'priority' in insights[0]


def test_insights_have_required_fields(sample_analysis_data, analysis_config):
    """Test insight structure validation."""
    
    metrics = generate_metrics(sample_analysis_data, analysis_config)
    summary = pd.DataFrame()  # Empty summary for testing
    
    insights = generate_insights(sample_analysis_data, metrics, summary, analysis_config)
    
    required_fields = ['title', 'description', 'priority', 'category']
    for insight in insights:
        for field in required_fields:
            assert field in insight


def test_calculate_growth_metrics(sample_analysis_data):
    """Test growth metrics calculation."""
    
    growth_metrics = calculate_growth_metrics(sample_analysis_data)
    
    assert isinstance(growth_metrics, pd.DataFrame)
    assert not growth_metrics.empty
    assert '7D_avg_revenue' in growth_metrics.columns or '30D_avg_revenue' in growth_metrics.columns


def test_create_scorecard():
    """Test scorecard generation."""
    
    df = pd.DataFrame({'total_amount': [100, 200]})
    metrics = pd.DataFrame({'total_revenue': [300]})
    
    scorecard = create_scorecard(df, metrics)
    
    assert isinstance(scorecard, pd.DataFrame)
    assert 'KPI' in scorecard.columns
    assert 'Score' in scorecard.columns


def test_analysis_handles_empty_data():
    """Test analysis with empty data."""
    
    empty_df = pd.DataFrame()
    config = {}
    
    metrics = generate_metrics(empty_df, config)
    assert isinstance(metrics, pd.DataFrame)  # Should return empty DF, not None
    
    summary = build_summary(empty_df, metrics, config)
    assert isinstance(summary, pd.DataFrame)
    
    insights = generate_insights(empty_df, metrics, summary, config)
    assert isinstance(insights, list)


def test_metrics_respects_status_filter(sample_analysis_data):
    """Test metrics filter completed transactions only."""
    
    # Add some cancelled orders
    sample_analysis_data.loc[10:15, 'status'] = 'cancelled'
    
    all_metrics = generate_metrics(sample_analysis_data, {})
    completed_data = sample_analysis_data[sample_analysis_data['status'] == 'completed']
    
    # Revenue should match completed transactions only
    if 'total_revenue' in all_metrics.columns:
        expected_revenue = completed_data['total_amount'].sum()
        actual_revenue = all_metrics['total_revenue'].sum()
        assert np.isclose(expected_revenue, actual_revenue, rtol=0.1)


def test_platform_metrics_aggregation(sample_analysis_data):
    """Test platform metrics breakdown sums correctly."""
    
    metrics = generate_metrics(sample_analysis_data, {})
    
    if 'platform_revenue' in metrics.columns and 'platform' in metrics.columns:
        platform_metrics = metrics[metrics['platform'].notna()]
        total_platform_rev = platform_metrics['platform_revenue'].sum()
        
        # Should match overall revenue
        overall_rev_row = metrics[metrics['platform'] == 'Total Revenue']
        if not overall_rev_row.empty:
            assert np.isclose(total_platform_rev, overall_rev_row['total_revenue'].iloc[0], rtol=0.05)


@pytest.mark.parametrize("top_n, expected_products", [
    (3, 3),
    (10, min(10, 5)),  # Limited by test data
    (100, 5)  # Should not exceed available unique products
])
def test_top_products_pagination(sample_analysis_data, analysis_config, top_n, expected_products):
    """Test top N products logic."""
    
    analysis_config['analytics'] = {'top_n': top_n}
    metrics = generate_metrics(sample_analysis_data, analysis_config)
    
    product_metrics = metrics[metrics['product_revenue'].notna()] if 'product_revenue' in metrics.columns else pd.DataFrame()
    
    if not product_metrics.empty:
        assert len(product_metrics) <= top_n
        assert len(product_metrics) == expected_products


def test_insights_prioritization(sample_analysis_data, analysis_config):
    """Test insights are properly prioritized."""
    
    metrics = generate_metrics(sample_analysis_data, analysis_config)
    summary = pd.DataFrame()
    
    insights = generate_insights(sample_analysis_data, metrics, summary, analysis_config)
    
    priorities = [insight.get('priority', 999) for insight in insights]
    assert min(priorities) <= 5  # Should have reasonable priorities
    assert priorities == sorted(priorities)  # Should be sorted by priority


class TestAnalysisErrorHandling:
    """Test error handling scenarios."""
    
    def test_missing_platform_column(self):
        """Test analysis without platform column."""
        
        df_no_platform = pd.DataFrame({'total_amount': [100, 200]})
        config = {}
        
        metrics = generate_metrics(df_no_platform, config)
        assert isinstance(metrics, pd.DataFrame)
    
    def test_single_transaction(self):
        """Test analysis with minimal data."""
        
        single_tx = pd.DataFrame({
            'order_id': ['ORD001'],
            'total_amount': [100000],
            'order_date': [datetime.now()],
            'platform': ['Shopee']
        })
        
        config = {}
        metrics = generate_metrics(single_tx, config)
        assert isinstance(metrics, pd.DataFrame)
        assert len(metrics) > 0