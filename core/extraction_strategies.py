import logging
from abc import ABC, abstractmethod
from datetime import timedelta, datetime
from typing import List
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
        self.logger.info(
            f"Iniciando extraccion diaria desde {config.date_from} hasta {config.date_to}")
        self.logger.info(
            f"Diferencia UTC: {self.datetime_helper.utc_offset_hours} hours")
        self.logger.info(
            f"Minutos seguros: {self.datetime_helper.minutes_before_now}")

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
        self.logger.info(
            f"Iniciando extraccion desde {config.date_from} hasta {config.date_to}")
        if not self.db_manager:
            raise ValueError(
                "DatabaseManager required for incremental extraction")

        orders = await self.api_client.get_orders_paginated(
            date_from=config.date_from,
            date_to=config.date_to,
            markets=config.markets
        )
        self.logger.info(
            f"Extraccion incremental completada con exito: {len(orders)} orders")
        return orders


class StatusUpdateExtraction(ExtractionStrategy):
    async def extract(self, config: ExtractionConfig) -> List[dict]:
        # 1. Obtener órdenes pendientes de la base de datos
        pending_orders = await self.db_manager.orders.get_pending_orders()

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


class WeeklyCatchUpExtraction(ExtractionStrategy):
    async def extract(self, config: ExtractionConfig) -> List[dict]:
        """
        Reprocesar órdenes Pending que pueden haber cambiado en Amazon
        e insertar órdenes que no existen en la BD
        """
        # 1. Obtener órdenes Pending de la BD que necesitan verificación
        stale_orders = await self.db_manager.orders.get_stale_orders(
            older_than=timedelta(days=7)
        )

        self.logger.info(f"Órdenes Pending en BD a verificar: {len(stale_orders)}")

        # 2. Obtener órdenes de Amazon para el mismo período
        api_orders = await self.api_client.get_orders_paginated(
            date_from=config.date_from,
            date_to=config.date_to,
            markets=config.markets
        )

        self.logger.info(f"Órdenes obtenidas de Amazon API: {len(api_orders)}")

        # 3. Crear diccionarios para comparación rápida
        stale_orders_dict = {order['amazonOrderId']: order for order in stale_orders}
        api_orders_dict = {order['amazonOrderId']: order for order in api_orders}

        # 4. Identificar órdenes que necesitan actualización o inserción
        orders_to_delete = []  # Para eliminar y reinsertar
        orders_to_insert = []  # Para insertar nuevas

        # Verificar órdenes existentes que han cambiado
        for order_id, api_order in api_orders_dict.items():
            if order_id in stale_orders_dict:
                # Existe en BD, verificar si cambió
                if self._needs_refresh(stale_orders_dict[order_id], api_order):
                    orders_to_delete.append(order_id)
                    orders_to_insert.append(api_order)
            else:
                # No existe en BD, insertar
                orders_to_insert.append(api_order)

        self.logger.info(f"Órdenes a eliminar y reinsertar: {len(orders_to_delete)}")
        self.logger.info(f"Órdenes totales a insertar: {len(orders_to_insert)}")

        # 5. Eliminar órdenes que han cambiado
        if orders_to_delete:
            await self.db_manager.orders.delete_orders(orders_to_delete)

        # 6. Retornar todas las órdenes a insertar
        return orders_to_insert

    def _needs_refresh(self, db_order: dict, api_order: dict) -> bool:
        """
        Verificar si una orden necesita actualización
        Compara orderStatus, lastUpdateDate u otros campos relevantes
        """
        if not api_order:
            return False

        # Verificar cambios en campos críticos
        status_changed = db_order.get('orderStatus') != api_order.get('orderStatus')
        last_update_changed = db_order.get('lastUpdateDate') != api_order.get('lastUpdateDate')

        return status_changed or last_update_changed
