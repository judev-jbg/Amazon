
from dataclasses import dataclass
from datetime import datetime
from typing import List
from enum import Enum


class ExtractType(Enum):
    DAILY_FULL = "daily_full"
    INCREMENTAL = "incremental"  
    STATUS_UPDATE = "status_update"
    WEEKLY_CATCH_UP = "weekly_catch_up"

@dataclass
class ExtractionConfig:
    extract_type: ExtractType
    date_from: datetime
    date_to: datetime
    markets: List[str]
    batch_size: int = 100