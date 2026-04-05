import logging
import requests
import hmac
import hashlib
import time
import pandas as pd
from typing import Dict, Any, Optional, List
from pathlib import Path

# Internal Module Imports
from utils.logger import setup_custom_logger

# Initialize Logger
logger = setup_custom_logger("shopee_connector")

class ShopeeConnector:
    """
    Production-grade Shopee API Connector (v2).
    Handles authentication, request signing, and data retrieval for the Shopee Open Platform.
    """

    def __init__(self, auth_config: Dict[str, Any]):
        """
        :param auth_config: Dictionary containing host, partner_id, partner_key, and shop_id.
        """
        self.host = auth_config.get("host", "https://partner.shopeemobile.com")
        self.partner_id = auth_config.get("partner_id")
        self.partner_key = auth_config.get("partner_key")
        self.shop_id = auth_config.get("shop_id")
        self.access_token = auth_config.get("access_token")
        
        # Validation
        if not all([self.partner_id, self.partner_key, self.shop_id]):
            logger.error("Missing critical Shopee API credentials in configuration.")
            raise ValueError("Incomplete Shopee API configuration.")

    def _generate_sign(self, path: str, timestamp: int) -> str:
        """Generates HMAC-SHA256 signature for Shopee API V2."""
        base_str = f"{self.partner_id}{path}{timestamp}{self.access_token}{self.shop_id}"
        sign = hmac.new(
            self.partner_key.encode(),
            base_str.encode(),
            hashlib.sha256
        ).hexdigest()
        return sign

    def get_order_list(self, time_from: int, time_to: int, status: str = "ALL") -> pd.DataFrame:
        """
        Retrieves order list within a specific time range.
        Handles pagination and returns a safe DataFrame.
        """
        path = "/api/v2/order/get_order_list"
        timestamp = int(time.time())
        
        params = {
            "partner_id": self.partner_id,
            "timestamp": timestamp,
            "sign": self._generate_sign(path, timestamp),
            "shop_id": self.shop_id,
            "access_token": self.access_token,
            "time_range_field": "create_time",
            "time_from": time_from,
            "time_to": time_to,
            "page_size": 100,
            "order_status": status
        }

        all_orders: List[Dict[str, Any]] = []
        cursor = ""

        try:
            while True:
                if cursor:
                    params["cursor"] = cursor

                logger.info(f"Fetching Shopee orders with cursor: {cursor or 'start'}")
                response = requests.get(f"{self.host}{path}", params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                if data.get("error"):
                    logger.error(f"Shopee API Error: {data.get('message')} (Request ID: {data.get('request_id')})")
                    break

                response_body = data.get("response", {})
                orders = response_body.get("order_list", [])
                all_orders.extend(orders)

                if not response_body.get("more"):
                    break
                
                cursor = response_body.get("next_cursor")

            if not all_orders:
                logger.warning("No orders found in the specified range for Shopee.")
                return pd.DataFrame()

            df = pd.DataFrame(all_orders)
            logger.info(f"Successfully ingested {len(df)} order headers from Shopee.")
            return df

        except requests.exceptions.RequestException as e:
            logger.error(f"Network error during Shopee API call: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.critical(f"Unexpected error in ShopeeConnector: {e}", exc_info=True)
            return pd.DataFrame()

    def get_order_details(self, order_sn_list: List[str]) -> pd.DataFrame:
        """
        Retrieves detailed order information including items and prices for a list of order IDs.
        """
        path = "/api/v2/order/get_order_detail"
        timestamp = int(time.time())
        
        # Shopee allows max 50 order_sn per request
        chunk_size = 50
        detailed_orders = []

        try:
            for i in range(0, len(order_sn_list), chunk_size):
                chunk = order_sn_list[i : i + chunk_size]
                
                params = {
                    "partner_id": self.partner_id,
                    "timestamp": timestamp,
                    "sign": self._generate_sign(path, timestamp),
                    "shop_id": self.shop_id,
                    "access_token": self.access_token,
                    "order_sn_list": ",".join(chunk),
                    "response_optional_fields": "item_list,receiver_address,payment_method,total_amount"
                }

                response = requests.get(f"{self.host}{path}", params=params, timeout=30)
                data = response.json()
                
                if data.get("response"):
                    detailed_orders.extend(data["response"]["order_list"])

            if not detailed_orders:
                return pd.DataFrame()

            # Normalize nested item_list if needed, or return raw detailed DF
            df = pd.json_normalize(
                detailed_orders, 
                record_path=['item_list'], 
                meta=['order_sn', 'order_status', 'create_time', 'total_amount', 'payment_method'],
                errors='ignore'
            )
            return df

        except Exception as e:
            logger.error(f"Failed to fetch Shopee order details: {e}")
            return pd.DataFrame()