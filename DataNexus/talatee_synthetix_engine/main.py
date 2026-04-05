import logging
import json
import argparse
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd

# Internal Module Imports
from utils.logger import setup_custom_logger
from ingestion.load_data import DataIngestor
from transform.column_mapper import ColumnMapper # MODUL BARU
from cleaning.standardize import DataStandardizer
from transform.feature_engineering import FeatureEngineer
from analysis.metrics import MetricsEngine
from analysis.insight import InsightGenerator
from analysis.summary import SummaryBuilder
from output.exporter import DataExporter

# Initialize Logger
logger = setup_custom_logger("main_engine")

class SentinelAnalyticsEngine:
    """
    Production-grade Engine to orchestrate multi-client, multi-marketplace 
    data pipelines based on JSON configurations.
    """

    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.config = self._load_config()
        self.client_id = self.config.get("client_id", "unknown_client")
        logger.info(f"Initialized Engine for Client: {self.client_id}")

    def _load_config(self) -> Dict[str, Any]:
        """Loads and validates the client JSON configuration."""
        if not self.config_path.exists():
            logger.error(f"Configuration file not found: {self.config_path}")
            raise FileNotFoundError(f"Config not found at {self.config_path}")
        
        try:
            with open(self.config_path, "r", encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in {self.config_path}: {e}")
            raise

    def run_pipeline(self):
        """Orchestrates the end-to-end data pipeline."""
        try:
            logger.info(f"Starting pipeline execution for {self.client_id}")

            # 1. Ingestion
            ingestion_cfg = self.config.get("ingestion") or self.config.get("marketplaces")
            if not ingestion_cfg:
                logger.error("No ingestion/marketplaces configuration found.")
                return

            ingestor = DataIngestor(ingestion_cfg)
            raw_data: Optional[pd.DataFrame] = ingestor.load_all_sources()
            
            if raw_data is None or raw_data.empty:
                logger.warning("No data ingested. Terminating pipeline.")
                return

            # --- TAHAP BARU: Column Mapping ---
            # Menghubungkan kolom mentah ke schema standar (price, quantity, product_name)
            logger.info("Applying column mapping to standardize schema.")
            mapper = ColumnMapper(ingestion_cfg)
            standardized_df = mapper.map_columns(raw_data)

            # 2. Cleaning & Standardization
            # Gunakan standardized_df agar pembersihan fokus pada kolom yang sudah seragam
            cleaner = DataStandardizer(self.config.get("cleaning_rules", {}))
            clean_df = cleaner.process(standardized_df) 
            
            if clean_df.empty:
                logger.error("Dataframe empty after cleaning stage.")
                return

            # 3. Transformation & Feature Engineering
            transformer = FeatureEngineer(self.config.get("transform_settings", {}))
            enriched_df = transformer.transform(clean_df)

            # 4. Analysis: Metrics & Insights
            metrics_cfg = self.config.get("analysis_settings", {})
            
            # A. Calculate Metrics
            metrics_engine = MetricsEngine(metrics_cfg)
            calculated_metrics = metrics_engine.calculate(enriched_df)

            # B. Generate Actionable Insights
            insight_gen = InsightGenerator(metrics_cfg)
            insights = insight_gen.generate(calculated_metrics, enriched_df)

            # C. Build Summary Data
            summary_builder = SummaryBuilder(metrics_cfg)
            exec_summary = summary_builder.build_executive_summary(enriched_df, calculated_metrics, insights)
            mkp_breakdown = summary_builder.build_marketplace_breakdown(enriched_df)

            # 5. Export & Reporting
            exporter = DataExporter(self.config.get("output_settings", {}), client_id=self.client_id)
            
            data_to_export = {
                "Executive_Summary": pd.DataFrame([exec_summary]) if isinstance(exec_summary, dict) else pd.DataFrame(),
                "Insights_Action_Plan": pd.DataFrame(insights),
                "Marketplace_Comparison": mkp_breakdown,
                "Cleaned_Final_Data": enriched_df.head(10000)
            }

            excel_path = exporter.export_to_excel(data_to_export, suffix="Full_Analytics_Report")
            exporter.export_summary_json(exec_summary, suffix="Executive_Summary")

            if excel_path:
                logger.info(f"Pipeline completed successfully. Report: {excel_path}")
            else:
                logger.error(f"Pipeline finished but failed to export report.")

        except Exception as e:
            logger.critical(f"Uncaught exception in pipeline for {self.client_id}: {str(e)}", exc_info=True)

def main():
    """Entry point for the production engine."""
    parser = argparse.ArgumentParser(description="Talatee Sentinel Analytics Engine")
    parser.add_argument(
        "--config", 
        type=str, 
        required=True, 
        help="Path to the client JSON configuration file"
    )
    
    args = parser.parse_args()
    config_file = Path(args.config)

    if not config_file.exists():
        logger.error(f"Config path provided does not exist: {config_file}")
        return

    try:
        engine = SentinelAnalyticsEngine(config_file)
        engine.run_pipeline()
    except Exception as e:
        logger.error(f"Failed to initialize or run engine: {e}")

if __name__ == "__main__":
    main()