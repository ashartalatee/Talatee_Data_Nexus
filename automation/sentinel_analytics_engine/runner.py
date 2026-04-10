import logging
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd

from ingestion.load_data import DataIngestor
from validation.schema_validator import SchemaValidator
from cleaning.standardize import DataCleaner
from transform.feature_engineering import DataTransformer
from analysis.metrics import AnalyticsEngine
from output.exporter import DataExporter
from utils.logger import setup_logger

class PipelineRunner:
    """
    Orchestrates the sequential execution of the data pipeline for a specific client.
    Ensures fail-safe transitions between modular steps.
    """
    def __init__(self, client_id: str, config: Dict[str, Any], global_settings: Dict[str, Any], base_dir: Path):
        self.client_id = client_id
        self.config = config
        self.global_settings = global_settings
        self.base_dir = base_dir
        
        # Setup client-specific logging
        log_file = base_dir / "logs" / f"{client_id}_pipeline.log"
        self.logger = setup_logger(f"runner_{client_id}", log_file)
        
        # Initialize Step Modules
        self.ingestor = DataIngestor(self.config, self.base_dir, self.logger)
        self.validator = SchemaValidator(self.config, self.logger)
        self.cleaner = DataCleaner(self.config, self.logger)
        self.transformer = DataTransformer(self.config, self.logger)
        self.analyzer = AnalyticsEngine(self.config, self.logger)
        self.exporter = DataExporter(self.config, self.base_dir, self.logger)

    def run_pipeline(self) -> bool:
        """
        Executes the pipeline flow: Ingestion -> Validation -> Cleaning -> Transform -> Analysis -> Output.
        Returns True if the critical path completes, False otherwise.
        """
        self.logger.info(f"Starting pipeline execution for client: {self.client_id}")
        
        df: Optional[pd.DataFrame] = None
        
        try:
            # 1. Ingestion
            df = self.ingestor.run()
            if df is None or df.empty:
                self.logger.error("Pipeline aborted: Ingestion returned empty/None DataFrame.")
                return False

            # 2. Validation (Fail-safe check)
            is_valid, df = self.validator.validate(df)
            if not is_valid and self.config.get("strict_mode", True):
                self.logger.error("Pipeline aborted: Schema validation failed in strict mode.")
                return False

            # 3. Cleaning
            df = self.cleaner.process(df)
            if df is None:
                self.logger.error("Pipeline aborted: Cleaning step failed.")
                return False

            # 4. Transformation
            df = self.transformer.apply_logic(df)
            if df is None:
                self.logger.error("Pipeline aborted: Transformation step failed.")
                return False

            # 5. Analysis & Metrics
            analysis_results = self.analyzer.generate_insights(df)
            if analysis_results is None:
                self.logger.warning("Analysis step produced no results, continuing to export raw/cleaned data.")

            # 6. Output / Export
            export_success = self.exporter.save(df, analysis_results)
            
            if export_success:
                self.logger.info(f"Pipeline successfully finalized for {self.client_id}")
                return True
            else:
                self.logger.error(f"Pipeline failed at the Export stage for {self.client_id}")
                return False

        except Exception as e:
            self.logger.critical(f"Unhandled exception in pipeline runner: {str(e)}", exc_info=True)
            return False

    def get_status(self) -> Dict[str, Any]:
        """Returns metadata about the last run."""
        return {
            "client_id": self.client_id,
            "config_version": self.config.get("version", "1.0.0"),
            "timestamp": pd.Timestamp.now().isoformat()
        }