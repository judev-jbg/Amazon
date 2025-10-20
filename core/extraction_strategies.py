import logging
import pandas as pd
import re
import hashlib
from abc import ABC, abstractmethod
from datetime import timedelta, datetime
from typing import List, Dict
from models.extraction_config import ExtractionConfig
from utils.datetime_helper import datetime_helper

"""
FUNCIONALIDAD:
- Define 5 estrategias diferentes de extracción
- Cada estrategia implementa lógica específica

"""

class ExtractionStrategy(ABC):
    def __init__(self, api_client, db_manager=None):
        self.api_client = api_client
        self.logger = logging.getLogger('AmazonManagement')
        self.datetime_helper = datetime_helper
        self.db_manager = db_manager
        
    @abstractmethod
    async def extract(self, config: ExtractionConfig) -> List[dict]:
        pass

class DailyFullExtraction(ExtractionStrategy):
    async def extract(self, config: ExtractionConfig) -> List[dict]:
        # Extraer órdenes del día anterior completo
        self.logger.info(f"Iniciando extraccion diaria desde {config.date_from} hasta {config.date_to}")
        self.logger.info(f"Diferencia UTC: {self.datetime_helper.utc_offset_hours} hours")
        self.logger.info(f"Minutos seguros: {self.datetime_helper.minutes_before_now}")
        
        orders = await self.api_client.get_orders_paginated(
            date_from=config.date_from,
            date_to=config.date_to,
            markets=config.markets
        )

        self.logger.info(f"Extraccion diaria completa: {len(orders)} ordenes")
        return orders
        
class IncrementalExtraction(ExtractionStrategy):
    async def extract(self, config: ExtractionConfig) -> List[dict]:
        # Solo órdenes nuevas/modificadas de la última hora
        self.logger.info(f"Iniciando extraccion desde {config.date_from} hasta {config.date_to}")
        if not self.db_manager:
            raise ValueError("DatabaseManager required for incremental extraction")
        
        orders = await self.api_client.get_orders_paginated(
            date_from=config.date_from,
            date_to=config.date_to,
            markets=config.markets
        )
        self.logger.info(f"Extraccion incremental completada con exito: {len(orders)} orders")
        return orders
        
class StatusUpdateExtraction(ExtractionStrategy):
    async def extract(self, config: ExtractionConfig) -> List[dict]:
        # 1. Obtener órdenes pendientes de la base de datos
        pending_orders = await self.db_manager.get_pending_orders()

        if not pending_orders:
            self.logger.info("No hay ordenes pendientes de actualización")
            return []
        
        self.logger.info(f"Ordenes de la base de datos: {len(pending_orders)}")
        
        # 2. Obtener datos actuales de Amazon para cada orden
        updated_orders = []

        for order in pending_orders:
        # Usar método específico para status
            current_status = await self.api_client.get_order_status(order['amazonOrderId'])
            
            if current_status and self._has_status_changed(order, current_status):
                updated_orders.append(current_status)
                
        return updated_orders
    
    def _has_status_changed(self, old_order: dict, new_order: dict) -> bool:
        return (old_order['orderStatus'] != new_order['orderStatus'] or
                old_order['lastUpdateDate'] != new_order['lastUpdateDate'])

        """
        Limpiar nombres de columnas quitando el sufijo '_o'
        """
        
        clean_order = {}
        for key, value in order.items():
            if key.endswith('_o'):
                clean_key = key[:-2]  # Quitar '_o'
                clean_order[clean_key] = value
            else:
                clean_order[key] = value
        
        # Añadir timestamp de carga
        clean_order['loadDate'] = str(datetime.date(datetime.now()))
        clean_order['loadDateTime'] = datetime.now()

        
        return clean_order
        
class WeeklyCatchUpExtraction(ExtractionStrategy):
    async def extract(self, config: ExtractionConfig) -> List[dict]:
        # Solo órdenes que realmente necesitan reproceso
        stale_orders = await self.db_manager.get_stale_orders(
            older_than=timedelta(days=7)
        )
        
        orders_to_refresh = []
        for order in stale_orders:
            # Verificar si realmente necesita actualización
            current_order = await self.api_client.get_order(order['amazonOrderId'])
            if self._needs_refresh(order, current_order):
                orders_to_refresh.append(current_order)
                
        return orders_to_refresh
    