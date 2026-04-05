import logging
import time
import hmac
import hashlib
import requests
import pandas as pd
from typing import Dict, Any, Optional, List
from pathlib import Path

# Internal Module Imports
from utils.logger import setup_custom_logger

# Initialize Logger
logger = setup_custom_logger("tiktok_connector")

class TikTokShopConnector:
    """
    Production-grade TikTok Shop API Connector.
    Handles authentication, request signing (SHA256), and order data retrieval
    via TikTok Shop Global Developer APIs.
    """

    def __init__(self, auth_config: Dict[str, Any]):
        """
        :param auth_config: Dictionary containing app_key, app_secret, shop_id, and access_token.
        """
        self.host = auth_config.get("host", "https://open-api.tiktokglobalshop.com")
        self.app_key = auth_config.get("app_key")
        self.app_secret = auth_config.get("app_secret")
        self.shop_id = auth_config.get("shop_id")
        self.access_token = auth_config.get("access_token")
        
        if not all([self.app_key, self.app_secret, self.shop_id]):
            logger.error("TikTok Shop credentials missing app_key, app_secret, or shop_id.")
            raise ValueError("Incomplete TikTok Shop API configuration.")

    def _generate_signature(self, path: str, params: Dict[str, Any]) -> str:
        """
        Generates HMAC-SHA256 signature for TikTok Shop API.
        Logic: Concatenate (secret + path + sorted params + secret)
        """
        # 1. Sort parameters by key
        sorted_keys = sorted(params.keys())
        params_str = "".join([f"{k}{params[k]}" for k in sorted_keys if k not in ["sign", "access_token"]])
        
        # 2. Construct base string
        base_str = f"{self.app_secret}{path}{params_str}{self.app_secret}"
        
        # 3. HMAC-SHA256
        sign = hmac.new(
            self.app_secret.encode("utf-8"),
            base_str.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        
        return sign

    def get_order_list(self, create_time_from: int, create_time_to: int) -> pd.DataFrame:
        """
        Retrieves a list of order IDs from TikTok Shop.
        Uses pagination via 'cursor'.
        """
        path = "/api/orders/search"
        timestamp = int(time.time())
        
        # Base query params
        params = {
            "app_key": self.app_key,
            "shop_id": self.shop_id,
            "timestamp": timestamp,
            "version": "202212"
        }

        # Request Body
        body = {
            "create_time_from": create_time_from,
            "create_time_to": create_time_to,
            "page_size": 50
        }

        all_order_ids: List[str] = []
        cursor = ""

        try:
            while True:
                if cursor:
                    body["cursor"] = cursor

                # Re-sign for every page as timestamp/cursor changes
                params["timestamp"] = int(time.time())
                sign = self._generate_signature(path, params)
                
                full_url = f"{self.host}{path}"
                request_params = {**params, "sign": sign, "access_token": self.access_token}

                logger.info(f"Fetching TikTok orders. Cursor: {cursor or 'initial'}")
                response = requests.post(full_url, params=request_params, json=body, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                if data.get("code") != 0:
                    logger.error(f"TikTok API Error: {data.get('message')} (Code: {data.get('code')})")
                    break

                resp_data = data.get("data", {})
                orders = resp_data.get("order_list", [])
                
                for order in orders:
                    all_order_ids.append(order.get("order_id"))

                if not resp_data.get("more"):
                    break
                
                cursor = resp_data.get("next_cursor")

            if not all_order_ids:
                return pd.DataFrame()

            # Return a simple DF of IDs to be used by details fetcher
            return pd.DataFrame({"order_id": all_order_ids})

        except Exception as e:
            logger.error(f"Failed to fetch TikTok order list: {e}", exc_info=True)
            return pd.DataFrame()

    def get_order_details(self, order_ids: List[str]) -> pd.DataFrame:
        """
        Retrieves full order details (SKU, prices, quantities).
        TikTok allows batching order IDs in the request body.
        """
        path = "/api/orders/detail/query"
        timestamp = int(time.time())
        
        params = {
            "app_key": self.app_key,
            "shop_id": self.shop_id,
            "timestamp": timestamp,
            "version": "202212"
        }

        # Batching 20 IDs per request (TikTok limit)
        chunk_size = 20
        all_details = []

        try:
            for i in range(0, len(order_ids), chunk_size):
                chunk = order_ids[i : i + chunk_size]
                body = {"order_id_list": chunk}
                
                params["timestamp"] = int(time.time())
                sign = self._generate_signature(path, params)
                
                full_url = f"{self.host}{path}"
                request_params = {**params, "sign": sign, "access_token": self.access_token}

                response = requests.post(full_url, params=request_params, json=body, timeout=30)
                data = response.json()
                
                if data.get("code") == 0:
                    batch_orders = data.get("data", {}).get("order_list", [])
                    all_details.extend(batch_orders)
                
                time.sleep(0.2) # Rate limit protection

            if not all_details:
                return pd.DataFrame()

            # Flattening item-level data
            df = pd.json_normalize(
                all_details,
                record_path=['item_list'],
                meta=['order_id', 'order_status', 'create_time', 'total_amount', 'payment_method'],
                errors='ignore'
            )
            logger.info(f"Successfully retrieved {len(df)} line items from TikTok Shop.")
            return df

        except Exception as e:
            logger.error(f"Error fetching TikTok order details: {e}")
            return pd.DataFrame()