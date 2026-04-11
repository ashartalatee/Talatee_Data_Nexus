import pandas as pd
from core.logger import CustomLogger
from modules.connectors.shopee_connector import ShopeeConnector
from modules.connectors.tokopedia_connector import TokopediaConnector
from modules.connectors.tiktok_connector import TikTokConnector
from modules.connectors.excel_connector import ExcelConnector
from modules.processors.cleaner import DataCleaner
from modules.processors.normalizer import DataNormalizer
from modules.analytics.metrics import SalesAnalytics
from modules.analytics.insight_generator import InsightGenerator
from modules.exporters.excel_exporter import ExcelExporter
from modules.exporters.csv_exporter import CSVExporter

class SalesOrchestrator:
    def __init__(self):
        self.logger = CustomLogger(name="Orchestrator").get_logger()
        # Mapping connector types to their respective classes
        self.connector_map = {
            "shopee_connector": ShopeeConnector,
            "tokopedia_connector": TokopediaConnector,
            "tiktok_connector": TikTokConnector,
            "excel_connector": ExcelConnector
        }

    def run_pipeline(self, client_config: dict):
        """
        Executes the full E2E pipeline for a single client.
        """
        client_id = client_config.get("client_id")
        client_name = client_config.get("client_name")
        all_channel_data = []

        self.logger.info(f"--- Starting Pipeline for {client_name} ---")

        # 1. Ingestion & Normalization Phase
        for source in client_config.get("data_sources", []):
            channel = source.get("channel")
            connector_type = source.get("connector_type")
            source_path = source.get("source_path")
            mapping = source.get("column_mapping")

            self.logger.info(f"Ingesting data from {channel} using {connector_type}...")
            
            try:
                # Instantiate correct connector
                connector_class = self.connector_map.get(connector_type)
                if not connector_class:
                    self.logger.error(f"Unsupported connector type: {connector_type}")
                    continue

                connector = connector_class(source_path)
                raw_data = connector.fetch_data()

                # Normalize to standard schema
                normalizer = DataNormalizer(raw_data, mapping, channel)
                standardized_df = normalizer.transform()
                
                all_channel_data.append(standardized_df)
            
            except Exception as e:
                self.logger.error(f"Error in ingestion phase for {channel}: {str(e)}")

        if not all_channel_data:
            self.logger.warning(f"No data collected for client {client_id}. Skipping.")
            return

        # 2. Consolidation & Cleaning Phase
        full_df = pd.concat(all_channel_data, ignore_index=True)
        cleaner = DataCleaner(full_df)
        clean_df = cleaner.process()

        # 3. Analytics Phase
        self.logger.info("Calculating business metrics and generating insights...")
        analytics = SalesAnalytics(clean_df)
        metrics_summary = analytics.calculate_all()

        insight_engine = InsightGenerator(metrics_summary, client_config.get("analysis_settings"))
        insights = insight_engine.generate()

        # 4. Export Phase
        output_cfg = client_config.get("output_settings", {})
        export_format = output_cfg.get("format", "excel").lower()
        
        self.logger.info(f"Exporting results to {export_format}...")
        
        if export_format == "csv":
            exporter = CSVExporter(clean_df, metrics_summary, insights, client_config)
        else:
            exporter = ExcelExporter(clean_df, metrics_summary, insights, client_config)
            
        report_path = exporter.export()
        
        self.logger.info(f"Pipeline completed. Report saved at: {report_path}")
        self.logger.info(f"--- Finished Pipeline for {client_name} ---")