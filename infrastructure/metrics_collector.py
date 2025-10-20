
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

    async def record_process_complementary_start(self, process_type: str):
        """Registrar inicio de proceso complementario"""
        metric = {
            'event': 'process_start',
            'process_type': process_type,
            'timestamp': datetime.now().isoformat()
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
    
    async def record_process_complementary_success(self, 
                                                   process_type: str, 
                                                   insert_count: int, 
                                                   update_count: int, 
                                                   validation_errors_count: int):
        """Registrar éxito del proceso complementario"""
        metric = {
            'event': 'process_success',
            'process_type': process_type,
            'timestamp': datetime.now().isoformat(),
            'insert_count': insert_count,
            'update_count': update_count,
            'validation_errors_count': validation_errors_count
        }
        await self._write_metric(metric)
    
    async def record_process_error(self, error_context: ErrorContext):
        """Registrar error"""
        metric = {
            'timestamp': error_context.timestamp.isoformat(),
                'error_type': error_context.error_type,
                'category': error_context.category.value,
                'level': error_context.level.value,
                'process_mode': error_context.process_mode,
                'market_id': error_context.market_id
        }
        await self._write_metric(metric)
    
    async def _write_metric(self, metric: Dict[str, Any]):
        """Escribir métrica a archivo"""
        async with aiofiles.open(self.metrics_file, 'a', encoding='utf-8') as f:
            await f.write(json.dumps(metric) + '\n')