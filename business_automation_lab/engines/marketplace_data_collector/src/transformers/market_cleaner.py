import logging
from typing import Dict, Any
from src.transformers.schemas import MarketplaceUnifiedModel

logger = logging.getLogger("TalateeEngine")

class MarketplaceTransformer:
    def standardize(self, raw_data: Dict[str, Any], platform: str, run_id: str) -> MarketplaceUnifiedModel:
        """
        Mengubah data mentah spesifik platform menjadi format standar industri yang bersih.
        """
        try:
            if platform.lower() == "shopee":
                return self._transform_shopee(raw_data, run_id)
            elif platform.lower() == "tokopedia":
                return self._transform_tokopedia(raw_data, run_id)
            else:
                raise ValueError(f"Platform transformer untuk '{platform}' belum diimplementasikan.")
        except KeyError as ke:
            logger.error(f"Gagal transformasi data: Struktur JSON {platform} berubah. Missing key: {str(ke)}")
            raise ke

    def _transform_shopee(self, data: Dict[str, Any], run_id: str) -> MarketplaceUnifiedModel:
        # Contoh ekstraksi berdasarkan payload API Shopee asli yang biasanya bersarang di bawah objek 'item'
        item = data.get("item", {})
        
        # Normalisasi harga: Shopee API sering kali mengembalikan harga dikali 100.000
        raw_price = float(item.get("price", 0))
        clean_price = raw_price / 100000 if raw_price > 100000 else raw_price

        return MarketplaceUnifiedModel(
            run_id=run_id,
            platform="shopee",
            product_id=str(item.get("itemid")),
            product_name=str(item.get("name")).strip(),
            price=clean_price,
            stock=int(item.get("stock", 0)),
            total_sold=int(item.get("historical_sold", 0))
        )

    def _transform_tokopedia(self, data: Dict[str, Any], run_id: str) -> MarketplaceUnifiedModel:
        # Contoh ekstraksi dari response GraphQL Tokopedia (biasanya di dalam data.data_product_by_id)
        p_data = data.get("data", {}).get("data_product_by_id", {})
        
        return MarketplaceUnifiedModel(
            run_id=run_id,
            platform="tokopedia",
            product_id=str(p_data.get("product_id")),
            product_name=str(p_data.get("name")).strip(),
            price=float(p_data.get("price_number", 0)),
            stock=int(p_data.get("stock", 0)),
            total_sold=int(p_data.get("tx_stats", {}).get("sold", 0))
        )