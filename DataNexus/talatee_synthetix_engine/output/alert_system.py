import logging
import json
import requests
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime

# Internal Module Imports
from utils.logger import setup_custom_logger

# Initialize Logger
logger = setup_custom_logger("alert_system")

class AlertSystem:
    """
    Production-grade Alerting System for e-commerce engine.
    Supports multi-channel alerts (Webhook, Slack, Discord) based on 
    insight priority and client configuration.
    """

    def __init__(self, alert_config: Dict[str, Any], client_id: str):
        """
        :param alert_config: Dictionary containing 'channels', 'thresholds', and 'enabled'.
        :param client_id: Unique identifier for the client context.
        """
        self.config = alert_config
        self.client_id = client_id
        self.enabled = self.config.get("enabled", True)
        self.channels = self.config.get("channels", {})
        self.min_priority = self.config.get("min_priority", "MEDIUM")
        
        # Priority Weighting for filtering
        self._priority_map = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
        
        logger.info(f"AlertSystem initialized for client: {self.client_id}. Enabled: {self.enabled}")

    def send_alerts(self, insights: List[Dict[str, Any]]) -> bool:
        """
        Processes a list of insights and dispatches alerts to configured channels
        if they meet the priority threshold.
        """
        if not self.enabled or not insights:
            logger.debug("Alerting disabled or no insights to process.")
            return False

        try:
            # Filter insights based on minimum priority level
            min_weight = self._priority_map.get(self.min_priority, 1)
            target_alerts = [
                i for i in insights 
                if self._priority_map.get(i.get("priority", "LOW"), 0) >= min_weight
            ]

            if not target_alerts:
                logger.debug("No insights met the priority threshold for alerting.")
                return True

            success_flag = True
            
            # Dispatch to Webhook/Slack if configured
            if "webhook" in self.channels:
                webhook_url = self.channels["webhook"].get("url")
                if webhook_url:
                    if not self._dispatch_webhook(webhook_url, target_alerts):
                        success_flag = False

            # Log critical alerts to a dedicated local security/alert log
            self._log_locally(target_alerts)

            return success_flag

        except Exception as e:
            logger.error(f"Critical failure in AlertSystem dispatch: {str(e)}", exc_info=True)
            return False

    def _dispatch_webhook(self, url: str, alerts: List[Dict[str, Any]]) -> bool:
        """Sends formatted JSON alerts to a generic webhook or Slack-compatible endpoint."""
        payload = {
            "client_id": self.client_id,
            "timestamp": datetime.now().isoformat(),
            "alert_count": len(alerts),
            "alerts": alerts
        }

        try:
            response = requests.post(
                url, 
                json=payload, 
                timeout=10,
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            logger.info(f"Successfully dispatched {len(alerts)} alerts to webhook.")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Webhook dispatch failed: {e}")
            return False

    def _log_locally(self, alerts: List[Dict[str, Any]]):
        """Maintains a persistent local audit trail of triggered alerts."""
        log_dir = Path("logs/alerts")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        log_file = log_dir / f"{self.client_id}_alerts.jsonl"
        
        try:
            with open(log_file, "a", encoding="utf-8") as f:
                for alert in alerts:
                    entry = {
                        "timestamp": datetime.now().isoformat(),
                        "client_id": self.client_id,
                        **alert
                    }
                    f.write(json.dumps(entry) + "\n")
        except Exception as e:
            logger.warning(f"Failed to write local alert log: {e}")

    def trigger_system_error(self, module: str, error_msg: str):
        """
        Special high-priority trigger for pipeline failures 
        rather than business insights.
        """
        system_alert = [{
            "type": "SYSTEM_FAILURE",
            "priority": "CRITICAL",
            "message": f"Pipeline failure in module [{module}]: {error_msg}",
            "action": "Check main engine logs and connectivity."
        }]
        logger.critical(f"Triggering system error alert for {module}")
        self.send_alerts(system_alert)