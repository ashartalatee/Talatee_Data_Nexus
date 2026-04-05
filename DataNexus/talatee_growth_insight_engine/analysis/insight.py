"""
AI-powered insight generation module.
Generates actionable business recommendations from analytics data.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, Optional, List
from utils.logger import setup_logger
from utils.config_loader import get_config_value
from utils.constants import ALERT_THRESHOLDS


def generate_insights(df: pd.DataFrame, metrics_df: pd.DataFrame, 
                     summary_df: pd.DataFrame, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Generate actionable business insights and recommendations.
    
    Args:
        df: Feature-engineered DataFrame
        metrics_df: Metrics DataFrame
        summary_df: Summary DataFrame
        config: Client configuration
        
    Returns:
        List of insight dictionaries
    """
    logger = setup_logger("analysis.insight")
    logger.info("💡 Generating actionable insights...")
    
    insights = []
    
    # Priority 1: Critical alerts
    critical_insights = _generate_critical_alerts(df, metrics_df, config)
    insights.extend(critical_insights)
    
    # Priority 2: Revenue opportunities
    revenue_insights = _generate_revenue_insights(df, metrics_df)
    insights.extend(revenue_insights)
    
    # Priority 3: Platform performance
    platform_insights = _generate_platform_insights(df)
    insights.extend(platform_insights)
    
    # Priority 4: Product insights
    product_insights = _generate_product_insights(df, config)
    insights.extend(product_insights)
    
    # Priority 5: Customer insights
    customer_insights = _generate_customer_insights(df)
    insights.extend(customer_insights)
    
    # Priority 6: Trend insights
    trend_insights = _generate_trend_insights(df)
    insights.extend(trend_insights)
    
    # Sort by priority/impact
    insights.sort(key=lambda x: x.get('priority', 5))
    
    logger.info(f"✅ Generated {len(insights)} actionable insights")
    _log_insights_summary(insights, logger)
    
    return insights[:25]  # Top 25 insights


def _generate_critical_alerts(df: pd.DataFrame, metrics_df: pd.DataFrame, 
                            config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Generate high-priority alerts."""
    
    alerts = []
    thresholds = get_config_value(config, 'alerts.thresholds', ALERT_THRESHOLDS)
    
    # Revenue drop alert
    if 'total_revenue' in metrics_df.columns:
        recent_rev = metrics_df[metrics_df['platform'] == 'Total Revenue']['total_revenue'].iloc[0]
        if recent_rev < thresholds.get('revenue_drop', 0.20):
            alerts.append({
                'title': '🚨 Revenue Alert: Significant Drop Detected',
                'description': f'Revenue appears {recent_rev:.1%} below threshold. Review platform performance and inventory.',
                'priority': 1,
                'category': 'Critical',
                'action': 'Check platform APIs, inventory stockouts, pricing changes',
                'severity': 'High'
            })
    
    # Platform dependency
    if 'platform' in df.columns:
        platform_shares = df.groupby('platform')['total_amount'].sum()
        max_share = platform_shares.max() / platform_shares.sum()
        if max_share > 0.7:
            top_platform = platform_shares.idxmax()
            alerts.append({
                'title': f'⚠️ Platform Risk: {top_platform} Dominance',
                'description': f'{top_platform} accounts for {max_share:.1%} of revenue. Diversification recommended.',
                'priority': 2,
                'category': 'Risk',
                'action': f'Develop {top_platform} alternatives (Tokopedia/TikTok Shop)',
                'severity': 'Medium'
            })
    
    return alerts


def _generate_revenue_insights(df: pd.DataFrame, metrics_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Revenue optimization insights."""
    
    insights = []
    
    if 'platform' in df.columns:
        platform_rev = df.groupby('platform')['total_amount'].sum().sort_values(ascending=False)
        
        # Best performing platform
        top_platform = platform_rev.index[0]
        insights.append({
            'title': f'💰 Revenue Leader: {top_platform}',
            'description': f'{top_platform} generated {platform_rev[top_platform]:.1%} of total revenue.',
            'priority': 3,
            'category': 'Opportunity',
            'action': f'Scale successful {top_platform} strategies to other platforms',
            'impact': 'High'
        })
    
    # High AOV products/customers
    if 'product_name' in df.columns:
        high_aov_products = df.groupby('product_name')['total_amount'].mean().nlargest(3)
        insights.append({
            'title': '🎯 High AOV Products Identified',
            'description': f"Top products: {', '.join(high_aov_products.index.tolist())}",
            'priority': 4,
            'category': 'Optimization',
            'action': 'Promote high-margin products, bundle with low performers',
            'impact': 'Medium'
        })
    
    return insights


def _generate_platform_insights(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Platform-specific recommendations."""
    
    insights = []
    
    if 'platform' in df.columns:
        platform_stats = df.groupby('platform').agg({
            'total_amount': 'sum',
            'quantity': 'sum',
            'order_id': 'nunique'
        })
        
        # Underperforming platform
        if len(platform_stats) > 1:
            worst_platform = platform_stats['total_amount'].idxmin()
            insights.append({
                'title': f'📉 {worst_platform} Underperforming',
                'description': f'{worst_platform} contributes only {platform_stats.loc[worst_platform, "total_amount"]:.1%} of revenue.',
                'priority': 4,
                'category': 'Optimization',
                'action': f'Optimize {worst_platform} listings, promotions, or reconsider focus',
                'impact': 'Medium'
            })
    
    return insights


def _generate_product_insights(df: pd.DataFrame, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Product performance insights."""
    
    insights = []
    top_n = get_config_value(config, 'analytics.top_n', 10)
    
    if 'product_id' in df.columns:
        product_perf = df.groupby('product_id').agg({
            'total_amount': 'sum',
            'quantity': 'sum'
        })
        
        # Hero products
        top_products = product_perf.nlargest(top_n, 'total_amount')
        if len(top_products) > 0:
            insights.append({
                'title': f'🏆 Top {min(top_n, len(top_products))} Hero Products',
                'description': f"These products drive {top_products['total_amount'].sum():.0%} of revenue.",
                'priority': 3,
                'category': 'Strength',
                'action': 'Double down on inventory, featured promotions, bundling opportunities',
                'impact': 'High'
            })
        
        # Slow movers
        slow_products = product_perf.nsmallest(10, 'total_amount')
        if len(slow_products) > 0:
            slow_rev_pct = slow_products['total_amount'].sum() / product_perf['total_amount'].sum()
            if slow_rev_pct < 0.05:  # Less than 5%
                insights.append({
                    'title': '🐌 Slow Moving Products Detected',
                    'description': f"{len(slow_products)} products contribute only {slow_rev_pct:.1%} revenue.",
                    'priority': 5,
                    'category': 'Optimization',
                    'action': 'Discount, bundle, or delist low performers',
                    'impact': 'Low'
                })
    
    return insights


def _generate_customer_insights(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Customer behavior insights."""
    
    insights = []
    
    if 'customer_id' in df.columns:
        customer_stats = df.groupby('customer_id').size()
        
        repeat_customers = (customer_stats > 1).sum()
        total_customers = len(customer_stats)
        
        if repeat_customers / total_customers > 0.2:
            insights.append({
                'title': '🔄 Strong Customer Loyalty',
                'description': f"{repeat_customers/total_customers:.1%} of customers are repeat buyers.",
                'priority': 3,
                'category': 'Strength',
                'action': 'Loyalty program, personalized offers, VIP tiers',
                'impact': 'High'
            })
    
    return insights


def _generate_trend_insights(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Trend and seasonality insights."""
    
    insights = []
    
    if 'order_date' in df.columns:
        df['date_only'] = df['order_date'].dt.date
        df['weekday'] = df['order_date'].dt.weekday
        
        # Day of week patterns
        dow_revenue = df.groupby('weekday')['total_amount'].sum()
        peak_dow = dow_revenue.idxmax()
        day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        
        insights.append({
            'title': f'📅 Peak Day: {day_names[peak_dow]}',
            'description': f'Highest revenue on {day_names[peak_dow]} ({dow_revenue[peak_dow]:.1%} of weekly total).',
            'priority': 4,
            'category': 'Optimization',
            'action': f'Schedule promotions and ads for {day_names[peak_dow]} peak',
            'impact': 'Medium'
        })
    
    return insights


def _log_insights_summary(insights: List[Dict], logger: logging.Logger) -> None:
    """Log insights summary."""
    
    by_priority = {}
    for insight in insights:
        priority = insight.get('priority', 5)
        by_priority.setdefault(priority, []).append(insight['title'])
    
    logger.info("📋 Insights by Priority:")
    for priority in sorted(by_priority.keys()):
        titles = [t[:50] + '...' for t in by_priority[priority][:3]]
        logger.info(f"  P{priority}: {len(by_priority[priority])} insights - {', '.join(titles)}")