# src/transformers/schemas.py
from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime, timezone

class MarketplaceUnifiedModel(BaseModel):
    run_id: str = Field(..., description="ID penanda sesi eksekusi pipeline data")
    platform: str = Field(..., description="Shopee, Tokopedia, atau TikTok")
    product_id: str = Field(..., description="ID Unik Produk dari platform asal")
    product_name: str = Field(..., description="Nama produk yang telah dibersihkan")
    price: float = Field(..., gt=0, description="Harga produk hasil konversi standar")
    stock: int = Field(..., ge=0)
    total_sold: int = Field(default=0, ge=0)
    product_grade: Optional[str] = Field(default="Unclassified", description="Hasil klasifikasi dari Intelligence Layer")
    
    # Perbaikan untuk menghilangkan DeprecationWarning di Python 3.12+
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator('platform')
    def validate_platform_name(cls, v):
        allowed = ['shopee', 'tokopedia', 'tiktok']
        if v.lower() not in allowed:
            raise ValueError(f"Platform {v} tidak didukung.")
        return v.lower()