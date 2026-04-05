#!/usr/bin/env python3
"""
Main entry point for the E-commerce Analytics Engine.
Orchestrates the full pipeline for multi-client, multi-marketplace analytics.
"""

import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import json

from utils.logger import setup_logger
from utils.config_loader import load_client_config
from utils.scheduler import should_run_pipeline
from ingestion.load_data import load_all_sources
from cleaning.standardize import clean_and_standardize
from transform.feature_engineering import transform_features
from analysis.metrics import generate_metrics
from analysis.summary import build_summary
from analysis.insight import generate_insights
from output.exporter import export_reports
from utils.constants import PIPELINE_STAGES


def main(client_id: str = None) -> None:
    """
    Main pipeline orchestrator.
    
    Args:
        client_id: Specific client ID to process. If None, processes all clients.
    """
    # Setup root logger
    logger = setup_logger("main")
    logger.info(f"🚀 E-commerce Analytics Engine started at {datetime.now()}")
    
    try:
        # 1. Load configuration
        logger.info("📋 Loading client configurations...")
        configs = load_client_config(client_id)
        if not configs:
            logger.error("❌ No valid client configurations found")
            sys.exit(1)
        
        processed_clients = []
        
        # 2. Process each client
        for client_id, config in configs.items():
            logger.info(f"{'='*60}")
            logger.info(f"👤 Processing client: {client_id}")
            
            try:
                # Check schedule
                if not should_run_pipeline(config.get('schedule', {})):
                    logger.info(f"⏭️ Skipping {client_id}: Not scheduled to run")
                    continue
                
                # 3. Data Ingestion
                logger.info("📥 Starting data ingestion...")
                raw_data = load_all_sources(config)
                if raw_data is None or raw_data.empty:
                    logger.warning(f"⚠️ No data loaded for {client_id}")
                    continue
                
                logger.info(f"✅ Loaded {len(raw_data)} records from {len(raw_data.columns)} columns")
                
                # 4. Data Cleaning Pipeline
                logger.info("🧹 Starting data cleaning pipeline...")
                cleaned_data = clean_and_standardize(raw_data, config)
                if cleaned_data is None or cleaned_data.empty:
                    logger.error(f"❌ Cleaning failed for {client_id}")
                    continue
                
                logger.info(f"✅ Cleaning complete: {len(cleaned_data)} records")
                
                # 5. Data Transformation & Feature Engineering
                logger.info("🔄 Starting transformation & feature engineering...")
                transformed_data = transform_features(cleaned_data, config)
                if transformed_data is None:
                    logger.error(f"❌ Transformation failed for {client_id}")
                    continue
                
                logger.info("✅ Transformation complete")
                
                # 6. Analytics & Insights
                logger.info("📊 Generating analytics & insights...")
                metrics_df = generate_metrics(transformed_data, config)
                summary_df = build_summary(transformed_data, metrics_df, config)
                insights = generate_insights(transformed_data, metrics_df, summary_df, config)
                
                if metrics_df is None or summary_df is None:
                    logger.error(f"❌ Analytics failed for {client_id}")
                    continue
                
                logger.info("✅ Analytics complete")
                
                # 7. Export Reports
                logger.info("📤 Generating reports...")
                export_path = export_reports(
                    client_id=client_id,
                    config=config,
                    raw_data=raw_data,
                    cleaned_data=cleaned_data,
                    transformed_data=transformed_data,
                    metrics_df=metrics_df,
                    summary_df=summary_df,
                    insights=insights
                )
                
                logger.info(f"✅ Reports exported to: {export_path}")
                processed_clients.append(client_id)
                
            except Exception as e:
                logger.error(f"💥 Pipeline failed for {client_id}: {str(e)}", exc_info=True)
                continue
        
        # 8. Final Summary
        logger.info(f"{'='*60}")
        logger.info(f"🎉 Pipeline completed successfully!")
        logger.info(f"✅ Processed clients: {len(processed_clients)}")
        logger.info(f"📋 Clients: {', '.join(processed_clients)}")
        
    except KeyboardInterrupt:
        logger.warning("⚠️ Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"💥 Critical pipeline failure: {str(e)}", exc_info=True)
        sys.exit(1)


def run_single_client(client_id: str) -> None:
    """Run pipeline for a single specific client."""
    main(client_id)


def run_all_clients() -> None:
    """Run pipeline for all configured clients."""
    main()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        client_id = sys.argv[1]
        run_single_client(client_id)
    else:
        run_all_clients()