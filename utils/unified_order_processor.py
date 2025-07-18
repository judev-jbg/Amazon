from datetime import datetime, timedelta
from enum import Enum
from config import setting as st
from core.order_service import OrderExtractionService, ExtractType
from models.extraction_config import ExtractionConfig

"""
FUNCIONALIDAD:
- Interfaz entre main.py y el servicio principal
- Configura parámetros según el modo
- Maneja la lógica de retry
- Logging de inicio/fin de proceso
"""

class ProcessMode(Enum):
    DAILY_FULL = "daily_full"
    INCREMENTAL = "incremental"
    STATUS_UPDATE = "status_update"
    WEEKLY_CATCHUP = "weekly_catchup"

class UnifiedOrderProcessor:
    def __init__(self):
        self.service = OrderExtractionService()
        
    async def run(self, mode: ProcessMode):
        """FLUJO DE EJECUCIÓN"""
        # 1. Crear configuración para el modo
        config = self._get_config_for_mode(mode)
        
        # 2. Ejecutar servicio principal de ordenes
        service = OrderExtractionService()
        await service.extract_orders(config)
    
    def _get_config_for_mode(self, mode: ProcessMode) -> ExtractionConfig:
        """Configuración según modo"""
        now = datetime.now()
        
        configs = {
            ProcessMode.DAILY_FULL: ExtractionConfig(
                extract_type=ExtractType.DAILY_FULL,
                date_from=now - timedelta(days=1),
                date_to=now,
                markets=list(st.setting_id_mkt_amz.values())
            ),
            ProcessMode.INCREMENTAL: ExtractionConfig(
                extract_type=ExtractType.INCREMENTAL,
                date_from=now - timedelta(hours=1),
                date_to=now,
                markets=list(st.setting_id_mkt_amz.values())
            ),
            ProcessMode.STATUS_UPDATE: ExtractionConfig(
                extract_type=ExtractType.STATUS_UPDATE,
                date_from=now - timedelta(hours=2),
                date_to=now,
                markets=list(st.setting_id_mkt_amz.values())
            ),
            ProcessMode.WEEKLY_CATCHUP: ExtractionConfig(
                extract_type=ExtractType.WEEKLY_CATCH_UP,
                date_from=now - timedelta(days=7),
                date_to=now,
                markets=list(st.setting_id_mkt_amz.values())
            )
        }
        
        return configs[mode]