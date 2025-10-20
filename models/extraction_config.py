
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional
from enum import Enum


class ExtractType(Enum):
    DAILY_FULL = "daily_full"
    INCREMENTAL = "incremental"  
    STATUS_UPDATE = "status_update"
    WEEKLY_CATCH_UP = "weekly_catch_up"
    ORDER_DETAILS = "order_details"
    SHIPMENT_UPDATE = "shipment_update"

@dataclass
class ExtractionConfig:
    extract_type: ExtractType
    date_from: datetime
    date_to: datetime
    markets: List[str]
    file_path: Optional[str] = None
    batch_size: int = 100
    description: Optional[str] = None

    def __post_init__(self):
        """Validaciones post-inicialización"""
        if self.date_from >= self.date_to:
            raise ValueError(f"date_from ({self.date_from}) must be before date_to ({self.date_to})")
        
        if self.batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {self.batch_size}")
        
        if not self.markets:
            raise ValueError("markets list cannot be empty")