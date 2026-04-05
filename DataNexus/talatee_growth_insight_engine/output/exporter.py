"""
Multi-format report exporter.
Supports CSV, Excel (multi-sheet), and PDF generation.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
from utils.logger import setup_logger
from utils.config_loader import get_output_dir, get_config_value
from utils.constants import SUPPORTED_EXPORT_FORMATS, DEFAULT_SHEETS


def export_reports(client_id: str, config: Dict[str, Any],
                  raw_data: Optional[pd.DataFrame] = None,
                  cleaned_data: Optional[pd.DataFrame] = None,
                  transformed_data: Optional[pd.DataFrame] = None,
                  metrics_df: Optional[pd.DataFrame] = None,
                  summary_df: Optional[pd.DataFrame] = None,
                  insights: Optional[List[Dict]] = None) -> Path:
    """
    Export all reports in configured formats.
    
    Args:
        client_id: Client identifier
        config: Client configuration
        raw_data: Raw ingested data
        cleaned_data: Cleaned data
        transformed_data: Feature engineered data
        metrics_df: Metrics DataFrame
        summary_df: Summary dashboard
        insights: Insights list
        
    Returns:
        Path to main export directory
    """
    logger = setup_logger("output.exporter")
    logger.info("📤 Starting report export...")
    
    export_config = config.get('export', {})
    formats = export_config.get('format', ['csv'])
    output_dir = get_output_dir(config)
    include_raw = export_config.get('include_raw', False)
    
    # Generate filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    filename_template = export_config.get('filename_template', '{client_id}_{date:%Y%m%d}_report')
    filename = filename_template.format(
        client_id=client_id,
        date=datetime.now(),
        timestamp=timestamp
    )
    
    export_paths = []
    
    for fmt in formats:
        if fmt.lower() == 'csv':
            csv_paths = _export_csv_reports(
                output_dir, filename, include_raw, raw_data, cleaned_data, 
                transformed_data, metrics_df, summary_df, insights
            )
            export_paths.extend(csv_paths)
            
        elif fmt.lower() == 'xlsx':
            xlsx_path = _export_excel_report(
                output_dir, f"{filename}.xlsx", include_raw, raw_data, cleaned_data, 
                transformed_data, metrics_df, summary_df, insights
            )
            export_paths.append(xlsx_path)
    
    logger.info(f"✅ Reports exported: {len(export_paths)} files")
    for path in export_paths[:5]:
        logger.info(f"📄 {path.name}")
    
    return output_dir


def _export_csv_reports(output_dir: Path, filename: str, include_raw: bool,
                       raw_data: Optional[pd.DataFrame], cleaned_data: Optional[pd.DataFrame],
                       transformed_data: Optional[pd.DataFrame], metrics_df: pd.DataFrame,
                       summary_df: pd.DataFrame, insights: List[Dict]) -> List[Path]:
    """Export individual CSV files."""
    
    paths = []
    
    # Metrics
    if metrics_df is not None and not metrics_df.empty:
        metrics_path = output_dir / f"{filename}_metrics.csv"
        metrics_df.to_csv(metrics_path, index=False, encoding='utf-8-sig')
        paths.append(metrics_path)
    
    # Summary
    if summary_df is not None and not summary_df.empty:
        summary_path = output_dir / f"{filename}_summary.csv"
        summary_df.to_csv(summary_path, index=False, encoding='utf-8-sig')
        paths.append(summary_path)
    
    # Insights
    if insights:
        insights_df = pd.DataFrame(insights)
        insights_path = output_dir / f"{filename}_insights.csv"
        insights_df.to_csv(insights_path, index=False, encoding='utf-8-sig')
        paths.append(insights_path)
    
    # Data files (sampled for large datasets)
    data_files = [
        ('cleaned_sample', cleaned_data),
        ('transformed_sample', transformed_data)
    ]
    
    for name, data_df in data_files:
        if data_df is not None and not data_df.empty:
            sample_df = data_df.head(10000)  # Sample for preview
            sample_path = output_dir / f"{filename}_{name}.csv"
            sample_df.to_csv(sample_path, index=False, encoding='utf-8-sig')
            paths.append(sample_path)
    
    if include_raw and raw_data is not None and not raw_data.empty:
        raw_path = output_dir / f"{filename}_raw_sample.csv"
        raw_sample = raw_data.head(5000)
        raw_sample.to_csv(raw_path, index=False, encoding='utf-8-sig')
        paths.append(raw_path)
    
    return paths


def _export_excel_report(output_dir: Path, filename: str, include_raw: bool,
                        raw_data: Optional[pd.DataFrame], cleaned_data: Optional[pd.DataFrame],
                        transformed_data: Optional[pd.DataFrame], metrics_df: pd.DataFrame,
                        summary_df: pd.DataFrame, insights: List[Dict]) -> Path:
    """Export comprehensive multi-sheet Excel report."""
    
    with pd.ExcelWriter(output_dir / filename, engine='openpyxl') as writer:
        
        # Summary Dashboard (first sheet)
        if summary_df is not None and not summary_df.empty:
            summary_df.to_excel(writer, sheet_name='📊 Executive Summary', index=False)
        
        # Detailed Metrics
        if metrics_df is not None and not metrics_df.empty:
            metrics_df.to_excel(writer, sheet_name='📈 Metrics', index=False)
        
        # Insights
        if insights:
            insights_df = pd.DataFrame(insights)
            insights_df.to_excel(writer, sheet_name='💡 Insights & Actions', index=False)
        
        # Platform Breakdown
        if 'platform' in metrics_df.columns:
            platform_metrics = metrics_df[metrics_df['platform'].notna()]
            platform_metrics.to_excel(writer, sheet_name='🌐 Platform Performance', index=False)
        
        # Top Products
        if 'product_revenue' in metrics_df.columns:
            product_metrics = metrics_df.nlargest(50, 'product_revenue')
            product_metrics.to_excel(writer, sheet_name='🏆 Top Products', index=False)
        
        # Data Previews
        if cleaned_data is not None:
            cleaned_sample = cleaned_data.head(1000)
            cleaned_sample.to_excel(writer, sheet_name='🔍 Cleaned Data Sample', index=False)
        
        if transformed_data is not None:
            feature_sample = transformed_data.select_dtypes(include=[np.number]).head(500)
            feature_sample.to_excel(writer, sheet_name='⚙️ Feature Sample', index=False)
        
        if include_raw and raw_data is not None:
            raw_sample = raw_data.head(500)
            raw_sample.to_excel(writer, sheet_name='📥 Raw Data Sample', index=False)
    
    return output_dir / filename


def create_pdf_summary(summary_df: pd.DataFrame, output_path: Path, 
                      insights: List[Dict], client_name: str) -> Optional[Path]:
    """
    Generate PDF summary report (requires reportlab).
    
    Args:
        summary_df: Summary data
        output_path: Output path
        insights: Insights list
        client_name: Client name
        
    Returns:
        PDF file path
    """
    try:
        from reportlab.lib.pagesizes import letter, A4
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib import colors
        from reportlab.lib.units import inch
        
        doc = SimpleDocTemplate(str(output_path), pagesize=A4)
        story = []
        styles = getSampleStyleSheet()
        
        # Title
        title = Paragraph(f"<b><font size=24>{client_name} Analytics Report</font></b>", styles['Title'])
        story.append(title)
        story.append(Spacer(1, 0.5*inch))
        
        # Summary KPIs
        if 'KPIs' in summary_df['Category'].values:
            kpi_data = summary_df[summary_df['Category'] == 'KPIs'][['Metric', 'Value']].head(8).values.tolist()
            kpi_table = Table(kpi_data, colWidths=[3*inch, 3*inch])
            kpi_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 14),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            story.append(kpi_table)
        
        story.append(Spacer(1, 0.3*inch))
        
        # Top Insights
        insights_title = Paragraph("<b><font size=16>Top Actionable Insights</font></b>", styles['Heading2'])
        story.append(insights_title)
        
        for insight in insights[:5]:
            insight_p = Paragraph(
                f"<b>{insight['title']}</b><br/><i>{insight['description'][:200]}...</i>", 
                ParagraphStyle('Insight', parent=styles['Normal'], leftIndent=20)
            )
            story.append(insight_p)
            story.append(Spacer(1, 0.1*inch))
        
        doc.build(story)
        logger = setup_logger("pdf_export")
        logger.info(f"📄 PDF summary created: {output_path}")
        
        return output_path
        
    except ImportError:
        logger = setup_logger("pdf_export")
        logger.warning("⚠️ reportlab not installed. Install with: pip install reportlab")
        return None
    except Exception as e:
        logger = setup_logger("pdf_export")
        logger.error(f"💥 PDF generation failed: {str(e)}")
        return None


def save_data_catalog(dataframes: Dict[str, pd.DataFrame], output_dir: Path) -> Path:
    """
    Save data catalog with DataFrame info and samples.
    
    Args:
        dataframes: Dict of {name: DataFrame}
        output_dir: Output directory
        
    Returns:
        Catalog file path
    """
    catalog_data = []
    
    for name, df in dataframes.items():
        if df is not None and not df.empty:
            catalog_data.append({
                'Dataset': name.title(),
                'Rows': f"{len(df):,}",
                'Columns': len(df.columns),
                'Size_MB': df.memory_usage(deep=True).sum() / 1024**2,
                'Sample': df.head(3).to_dict()
            })
    
    catalog_df = pd.DataFrame(catalog_data)
    catalog_path = output_dir / "data_catalog.csv"
    catalog_df.to_csv(catalog_path, index=False)
    
    return catalog_path