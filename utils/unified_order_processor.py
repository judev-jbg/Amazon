import logging
from datetime import datetime, timedelta
from enum import Enum
from config import setting as st
from core.order_service import OrderExtractionService, ExtractType
from services.order_details_service import OrderDetailsService
from services.shipment_service import ShipmentService
from utils.datetime_helper import datetime_helper
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
    ORDER_DETAILS = "order_details"
    SHIPMENT_UPDATE = "shipment_update"

class UnifiedOrderProcessor:
    def __init__(self):
        self.service = OrderExtractionService()
        self.order_details_service = OrderDetailsService()
        self.shipment_service = ShipmentService() 
        self.logger = logging.getLogger("AmazonManagement")
        self.logger.info("🚀 UnifiedOrderProcessor inicializado")
        
        self.datetime_helper = datetime_helper
        
    async def run(self, mode: ProcessMode):
        """FLUJO DE EJECUCIÓN"""
        # Crear configuración para el modo
        debug_info = self.datetime_helper.get_debug_info()
        self.logger.info(f"DateTime debug info: {debug_info}")
        self.logger.info(f"Modo recibido para procesar: {mode}")

        service_map = {
            # Modos API existentes
            ProcessMode.DAILY_FULL: self._run_api_service,
            ProcessMode.INCREMENTAL: self._run_api_service,
            ProcessMode.STATUS_UPDATE: self._run_api_service,
            ProcessMode.WEEKLY_CATCHUP: self._run_api_service,
            
            # 🆕 Nuevos modos complementarios
            ProcessMode.ORDER_DETAILS: self._run_order_details,
            ProcessMode.SHIPMENT_UPDATE: self._run_shipment_update,
        }

        config = service_map.get(mode)

        if not config:
            raise ValueError(f"Modo no soportado: {mode}")
        self.logger.info( f"Procesando modo {mode.value}")
        return await config(mode)
    
    def _get_config_for_mode(self, mode: ProcessMode) -> ExtractionConfig:
        """Configuración según modo"""
        # Mapeo de configuraciones por modo
       
        config_map = {
            ProcessMode.DAILY_FULL: self._get_daily_full_config,
            ProcessMode.INCREMENTAL: self._get_incremental_config,
            ProcessMode.STATUS_UPDATE: self._get_status_update_config,
            ProcessMode.WEEKLY_CATCHUP: self._get_weekly_catchup_config,
        }
        
        config_func = config_map.get(mode)
        if not config_func:
            raise ValueError(f"Unsupported process mode: {mode}")
            
        return config_func()
    
    async def _run_api_service(self, mode: ProcessMode):
        """Ejecutar servicios que usan API"""
        config = self._get_config_for_mode(mode)
        await self.service.extract_orders(config)
    
    async def _run_order_details(self, mode: ProcessMode):
        """Ejecutar servicio de OrderDetails"""
        return await self.order_details_service.process_order_details()
    
    async def _run_shipment_update(self, mode: ProcessMode):
        """Ejecutar servicio de Shipments"""
        return await self.shipment_service.process_shipment_updates()

    def _get_daily_full_config(self) -> ExtractionConfig:
        """
        Configuración diaria completa
        Desde ayer 00:00 hasta Ahora (menos minutos de seguridad)
        """
        date_from, date_to = self.datetime_helper.get_daily_full_range()
        
        return ExtractionConfig(
            extract_type=ExtractType.DAILY_FULL,
            date_from=date_from,
            date_to=date_to,
            markets=list(st.setting_id_mkt_amz.values()),
            batch_size=100,
            description=f"Daily full extraction: {date_from.date()} to {date_to.date()}"
        )
    
    def _get_incremental_config(self) -> ExtractionConfig:
        """
        Configuración incremental
        Última hora hasta ahora (menos minutos de seguridad)
        """
        date_from, date_to = self.datetime_helper.get_incremental_range()
        
        return ExtractionConfig(
            extract_type=ExtractType.INCREMENTAL,
            date_from=date_from,
            date_to=date_to,
            markets=list(st.setting_id_mkt_amz.values()),
            batch_size=50,  # Lotes más pequeños para incremental
            description=f"Incremental extraction: {date_from.isoformat()} to {date_to.isoformat()}"
        )
    
    def _get_status_update_config(self) -> ExtractionConfig:
        """
        Configuración actualización estados
        No usa rangos de fechas, sino órdenes pendientes de la BD
        """
        # Para status update, las fechas son menos relevantes ya que se basa en órdenes pendientes
        # Pero mantenemos un rango por consistencia
        date_from, date_to = self.datetime_helper.get_status_update_range()
        
        return ExtractionConfig(
            extract_type=ExtractType.STATUS_UPDATE,
            date_from=date_from,
            date_to=date_to,
            markets=list(st.setting_id_mkt_amz.values()),
            batch_size=25,  # Lotes pequeños para updates
            description="Status update for pending orders"
        )
    
    def _get_weekly_catchup_config(self) -> ExtractionConfig:
        """
        Configuración catch-up semanal
        Últimos 7 días hasta ahora (menos minutos de seguridad)
        """
        date_from, date_to = self.datetime_helper.get_weekly_catchup_range()
        
        return ExtractionConfig(
            extract_type=ExtractType.WEEKLY_CATCH_UP,
            date_from=date_from,
            date_to=date_to,
            markets=list(st.setting_id_mkt_amz.values()),
            batch_size=200,  # Lotes más grandes para catch-up
            description=f"Weekly catchup: {date_from.date()} to {date_to.date()}"
        )
    