import argparse
import logging
import json
from pathlib import Path
from typing import Dict, Any, Optional, List

import pandas as pd

from utils.logger import setup_custom_logger
from ingestion.load_data import DataIngestor
from validation.schema_validator import SchemaValidator
from cleaning.standardize import DataStandardizer
from transform.column_mapper import ColumnMapper
from transform.feature_engineering import FeatureEngineer
from analysis.metrics import MetricsEngine
from analysis.summary import SummaryBuilder
from analysis.insight import InsightGenerator
from output.exporter import DataExporter

logger = setup_custom_logger("main_engine")


class SentinelAnalyticsEngine:
    """
    Central orchestrator for multi-client analytics pipeline.
    Executes pipeline based on client configuration.
    """

    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.config: Dict[str, Any] = self._load_config()
        self.client_id: str = self.config.get("client_id", "unknown_client")
        self.pipeline_steps: List[str] = self.config.get(
            "pipeline",
            ["ingestion", "validation", "mapping", "cleaning", "transform", "analysis", "output"]
        )
        logger.info(f"Engine initialized for client: {self.client_id}")

    def _load_config(self) -> Dict[str, Any]:
        if not self.config_path.exists():
            logger.error(f"Config not found: {self.config_path}")
            raise FileNotFoundError(f"Missing config: {self.config_path}")

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise

    def run_pipeline(self) -> None:
        logger.info(f"Starting pipeline for client: {self.client_id}")

        df: Optional[pd.DataFrame] = None
        metrics: Optional[Dict[str, Any]] = None
        insights: Optional[List[Dict[str, Any]]] = None

        try:
            # INGESTION
            if "ingestion" in self.pipeline_steps:
                try:
                    ingestor = DataIngestor(self.config.get("ingestion", {}))
                    df = ingestor.load_all_sources()
                    if df is None or df.empty:
                        logger.warning("Ingestion returned empty dataframe")
                        df = pd.DataFrame()
                except Exception as e:
                    logger.error(f"Ingestion failed: {e}", exc_info=True)
                    df = pd.DataFrame()

            # VALIDATION
            if "validation" in self.pipeline_steps:
                try:
                    validator = SchemaValidator(self.config.get("schema", {}))
                    df = validator.validate(df)
                except Exception as e:
                    logger.error(f"Validation failed: {e}", exc_info=True)

            # COLUMN MAPPING
            if "mapping" in self.pipeline_steps:
                try:
                    mapper = ColumnMapper(self.config.get("ingestion", {}))
                    df = mapper.map_columns(df)
                except Exception as e:
                    logger.error(f"Column mapping failed: {e}", exc_info=True)

            # CLEANING
            if "cleaning" in self.pipeline_steps:
                try:
                    cleaner = DataStandardizer(self.config.get("cleaning_rules", {}))
                    df = cleaner.process(df)
                except Exception as e:
                    logger.error(f"Cleaning failed: {e}", exc_info=True)

            # TRANSFORM
            if "transform" in self.pipeline_steps:
                try:
                    transformer = FeatureEngineer(self.config.get("transform_settings", {}))
                    df = transformer.transform(df)
                except Exception as e:
                    logger.error(f"Transform failed: {e}", exc_info=True)

            # ANALYSIS
            if "analysis" in self.pipeline_steps:
                try:
                    metrics_engine = MetricsEngine(self.config.get("analysis_settings", {}))
                    metrics = metrics_engine.calculate(df)

                    insight_engine = InsightGenerator(self.config.get("analysis_settings", {}))
                    insights = insight_engine.generate(metrics, df)
                except Exception as e:
                    logger.error(f"Analysis failed: {e}", exc_info=True)

            # OUTPUT
            if "output" in self.pipeline_steps:
                try:
                    summary_builder = SummaryBuilder(self.config.get("analysis_settings", {}))
                    exec_summary = summary_builder.build_executive_summary(df, metrics, insights)
                    marketplace_summary = summary_builder.build_marketplace_breakdown(df)

                    exporter = DataExporter(
                        self.config.get("output_settings", {}),
                        client_id=self.client_id
                    )

                    export_payload = {
                        "Executive_Summary": pd.DataFrame([exec_summary]) if isinstance(exec_summary, dict) else pd.DataFrame(),
                        "Insights": pd.DataFrame(insights) if insights else pd.DataFrame(),
                        "Marketplace_Breakdown": marketplace_summary if isinstance(marketplace_summary, pd.DataFrame) else pd.DataFrame(),
                        "Processed_Data": df.head(10000) if isinstance(df, pd.DataFrame) else pd.DataFrame()
                    }

                    exporter.export_to_excel(export_payload, suffix="Analytics_Report")
                    exporter.export_summary_json(exec_summary, suffix="Summary")

                except Exception as e:
                    logger.error(f"Export failed: {e}", exc_info=True)

            logger.info(f"Pipeline completed for client: {self.client_id}")

        except Exception as e:
            logger.critical(f"Critical failure in pipeline: {e}", exc_info=True)


def main():
    parser = argparse.ArgumentParser(description="Sentinel Analytics Engine")
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to client config JSON"
    )

    args = parser.parse_args()
    config_path = Path(args.config)

    if not config_path.exists():
        logger.error(f"Invalid config path: {config_path}")
        return

    try:
        engine = SentinelAnalyticsEngine(config_path)
        engine.run_pipeline()
    except Exception as e:
        logger.error(f"Engine execution failed: {e}", exc_info=True)


if __name__ == "__main__":
    main()