from typing import List

class DataSchema:
    """
    Standard Data Schema Definition.
    This class serves as the contract for all connectors and processors.
    """
    
    # Standard column names as per requirements
    ORDER_ID = "order_id"
    DATE = "date"
    PRODUCT = "product"
    CATEGORY = "category"
    QUANTITY = "quantity"
    PRICE = "price"
    CUSTOMER_ID = "customer_id"
    CHANNEL = "channel"

    @classmethod
    def get_required_columns(cls) -> List[str]:
        """Returns the mandatory columns for the engine."""
        return [
            cls.ORDER_ID,
            cls.DATE,
            cls.PRODUCT,
            cls.CATEGORY,
            cls.QUANTITY,
            cls.PRICE,
            cls.CUSTOMER_ID,
            cls.CHANNEL
        ]

    @classmethod
    def get_dtype_map(cls) -> dict:
        """
        Defines the expected data types for normalization.
        Ensures numerical columns are handled correctly.
        """
        return {
            cls.ORDER_ID: str,
            cls.DATE: 'datetime64[ns]',
            cls.PRODUCT: str,
            cls.CATEGORY: str,
            cls.QUANTITY: int,
            cls.PRICE: float,
            cls.CUSTOMER_ID: str,
            cls.CHANNEL: str
        }