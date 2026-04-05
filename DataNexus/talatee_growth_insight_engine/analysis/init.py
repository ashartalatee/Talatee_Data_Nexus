"""Analysis package initialization."""
__all__ = [
    'generate_metrics', 
    'build_summary',
    'generate_insights',
    'calculate_growth_metrics'
]

from .metrics import generate_metrics, calculate_growth_metrics
from .summary import build_summary, create_scorecard
from .insight import generate_insights