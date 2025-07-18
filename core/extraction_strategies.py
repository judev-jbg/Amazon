from abc import ABC, abstractmethod
from datetime import timedelta
from typing import List
from models.extraction_config import ExtractionConfig

"""
FUNCIONALIDAD:
- Define 4 estrategias diferentes de extracción
- Cada estrategia implementa lógica específica

"""

class ExtractionStrategy(ABC):
    def __init__(self, api_client):
        self.api_client = api_client
        
    @abstractmethod
    async def extract(self, config: ExtractionConfig) -> List[dict]:
        pass



class DailyFullExtraction(ExtractionStrategy):
    async def extract(self, config: ExtractionConfig) -> List[dict]:
        # Extraer órdenes del día anterior completo
        orders = await self.api_client.get_orders_paginated(
            date_from=config.date_from,
            date_to=config.date_to,
            markets=config.markets
        )
        return orders

        
class IncrementalExtraction(ExtractionStrategy):
    async def extract(self, config: ExtractionConfig) -> List[dict]:
        # Solo órdenes nuevas/modificadas de la última hora
        last_sync = await self.db_manager.get_last_sync_time()
        
        orders = await self.api_client.get_orders_paginated(
            date_from=last_sync,
            date_to=config.date_to,
            markets=config.markets
        )
        return orders
        
class StatusUpdateExtraction(ExtractionStrategy):
    async def extract(self, config: ExtractionConfig) -> List[dict]:
        # Solo órdenes pendientes de actualización
        pending_orders = await self.db_manager.get_pending_orders()
        
        updated_orders = []
        for order in pending_orders:
            updated_order = await self.api_client.get_order(order['amazonOrderId'])
            if self._has_status_changed(order, updated_order):
                updated_orders.append(updated_order)
                
        return updated_orders
    
    def _has_status_changed(self, old_order: dict, new_order: dict) -> bool:
        return (old_order['orderStatus'] != new_order['orderStatus'] or
                old_order['lastUpdateDate'] != new_order['lastUpdateDate'])
        
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