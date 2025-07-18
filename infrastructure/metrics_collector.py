
import json
import aiofiles
from datetime import datetime
from typing import Dict, Any
from models.extraction_config import ExtractionConfig
from models.error_models import ErrorContext

class MetricsCollector:
    def __init__(self):
        self.metrics_file = "logs/metrics.json"
    
    async def record_process_start(self, config: ExtractionConfig):
        """Registrar inicio de proceso"""
        metric = {
            'event': 'process_start',
            'process_type': config.extract_type.value,
            'timestamp': datetime.now().isoformat(),
            'markets': config.markets
        }
        await self._write_metric(metric)
    
    async def record_process_success(self, config: ExtractionConfig, order_count: int):
        """Registrar éxito del proceso"""
        metric = {
            'event': 'process_success',
            'process_type': config.extract_type.value,
            'timestamp': datetime.now().isoformat(),
            'order_count': order_count,
            'markets': config.markets
        }
        await self._write_metric(metric)
    
    async def record_error(self, error_context: ErrorContext):
        """Registrar error"""
        metric = {
            'event': 'error',
            'error_type': error_context.error_type,
            'error_category': error_context.category.value,
            'error_level': error_context.level.value,
            'timestamp': error_context.timestamp.isoformat(),
            'process_mode': error_context.process_mode
        }
        await self._write_metric(metric)
    
    async def _write_metric(self, metric: Dict[str, Any]):
        """Escribir métrica a archivo"""
        async with aiofiles.open(self.metrics_file, 'a') as f:
            await f.write(json.dumps(metric) + '\n')