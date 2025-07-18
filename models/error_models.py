
from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any

class AlertLevel(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class ErrorCategory(Enum):
    API_ERROR = "api_error"
    DATABASE_ERROR = "database_error"
    PROCESSING_ERROR = "processing_error"
    SYSTEM_ERROR = "system_error"

@dataclass
class ErrorContext:
    """Contexto enriquecido del error"""
    error_type: str
    error_message: str
    file_name: str
    line_number: int
    function_name: str
    category: ErrorCategory
    level: AlertLevel
    timestamp: datetime
    additional_data: Dict[str, Any] = None
    stack_trace: str = None
    process_mode: str = None
    market_id: str = None