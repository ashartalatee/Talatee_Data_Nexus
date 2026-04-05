import logging
import requests
import time
import pandas as pd
from typing import Dict, Any, Optional, List
from pathlib import Path

# Internal Module Imports
from utils.logger import setup_custom_logger

# Initialize Logger
logger = setup_custom_logger("tokopedia_connector")

class TokopediaConnector:
    """
    Production-grade Tokopedia API Connector.
    Handles OAuth2 authentication and order data retrieval via Tokopedia Developer APIs.
    """

    def __init__(self, auth_config: Dict[str, Any]):
        """
        :param auth_config: Dictionary containing client_id, client_secret, and fs_id.
        """
        self.host = auth_config.get("host", "https://fs.tokopedia.net")
        self.account_host = auth_config.get("account_host", "https://accounts.tokopedia.com")
        self.client_id = auth_config.get("client_id")
        self.client_secret = auth_config.get("client_secret")
        self.fs_id = auth_config.get("fs_id")
        self._access_token: Optional[str] = None
        
        if not all([self.client_id, self.client_secret, self.fs_id]):
            logger.error("Tokopedia credentials missing client_id, client_secret, or fs_id.")
            raise ValueError("Incomplete Tokopedia API configuration.")

    def _authenticate(self) -> bool:
        """Retrieves OAuth2 access token using client credentials."""
        path = "/token?grant_type=client_credentials"
        try:
            response = requests.post(
                f"{self.account_host}{path}",
                auth=(self.client_id, self.client_secret),
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            self._access_token = data.get("access_token")
            logger.info("Successfully authenticated with Tokopedia.")
            return True
        except Exception as e:
            logger.error(f"Tokopedia authentication failed: {e}")
            return False

    def get_order_list(self, start_date: int, end_date: int, shop_id: int) -> pd.DataFrame:
        """
        Retrieves list of orders for a specific shop and time range.
        Dates should be unix timestamps.
        """
        if not self._access_token and not self._authenticate():
            return pd.DataFrame()

        path = "/v2/order/list"
        headers = {"Authorization": f"Bearer {self._access_token}"}
        params = {
            "fs_id": self.fs_id,
            "shop_id": shop_id,
            "from_date": start_date,
            "to_date": end_date,
            "page": 1,
            "per_page": 50
        }

        all_orders: List[Dict[str, Any]] = []

        try:
            while True:
                logger.info(f"Fetching Tokopedia orders page: {params['page']}")
                response = requests.get(f"{self.host}{path}", headers=headers, params=params, timeout=30)
                
                if response.status_code == 401:
                    logger.warning("Token expired, re-authenticating...")
                    self._authenticate()
                    headers["Authorization"] = f"Bearer {self._access_token}"
                    continue

                response.raise_for_status()
                data = response.json()
                
                orders = data.get("data", [])
                if not orders:
                    break
                
                all_orders.extend(orders)
                
                # Check pagination
                paging = data.get("paging", {})
                if params["page"] >= paging.get("total_page", 1):
                    break
                    
                params["page"] += 1

            if not all_orders:
                logger.warning("No orders found for Tokopedia shop_id: %s", shop_id)
                return pd.DataFrame()

            df = pd.DataFrame(all_orders)
            logger.info(f"Retrieved {len(df)} orders from Tokopedia.")
            return df

        except Exception as e:
            logger.error(f"Error fetching Tokopedia order list: {e}", exc_info=True)
            return pd.DataFrame()

    def get_order_details(self, order_ids: List[int]) -> pd.DataFrame:
        """
        Retrieves full details for specific order IDs including product info.
        """
        if not self._access_token and not self._authenticate():
            return pd.DataFrame()

        path = "/v2/order/detail"
        headers = {"Authorization": f"Bearer {self._access_token}"}
        
        detailed_data = []

        try:
            for order_id in order_ids:
                params = {"order_id": order_id}
                response = requests.get(f"{self.host}{path}", headers=headers, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json().get("data")
                    if data:
                        detailed_data.append(data)
                
                # Rate limiting protection
                time.sleep(0.1)

            if not detailed_data:
                return pd.DataFrame()

            # Flattening product items within orders
            df = pd.json_normalize(
                detailed_data,
                record_path=['products'],
                meta=['order_id', 'invoice_number', 'order_status', 'create_time', 'amt', 'payment_date'],
                errors='ignore'
            )
            return df

        except Exception as e:
            logger.error(f"Error fetching Tokopedia order details: {e}")
            return pd.DataFrame()