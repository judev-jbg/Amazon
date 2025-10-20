# domain/interfaces/api_interfaces.py
"""
Interfaces para clientes de API externa
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Tuple
from datetime import datetime


class IAmazonAPIClient(ABC):
    """Interface para cliente de Amazon SP-API"""

    @abstractmethod
    async def get_orders_paginated(
        self,
        date_from: datetime,
        date_to: datetime,
        markets: List[str]
    ) -> List[Dict]:
        """Obtener órdenes con paginación automática"""
        pass

    @abstractmethod
    async def get_order(self, order_id: str, max_retries: int = 3) -> Dict:
        """Obtener una orden específica"""
        pass

    @abstractmethod
    async def get_order_items(self, order_id: str, max_retries: int = 3) -> List[Dict]:
        """Obtener items de una orden"""
        pass

    @abstractmethod
    async def get_sales_data(
        self,
        asin: str,
        sku: str,
        market: List[str],
        interval: Tuple[str, str],
        max_retries: int = 3
    ) -> List[Dict]:
        """Obtener datos de ventas para un ASIN/SKU"""
        pass

    @abstractmethod
    async def get_order_status(self, order_id: str) -> Dict:
        """Obtener solo el status de una orden"""
        pass

    @abstractmethod
    async def batch_get_orders(self, order_ids: List[str]) -> List[Dict]:
        """Obtener múltiples órdenes en paralelo"""
        pass

    @abstractmethod
    async def get_order_items_batch(
        self,
        order_ids: List[str],
        batch_size: int = 10
    ) -> Dict[str, List[Dict]]:
        """Obtener items de múltiples órdenes en lotes"""
        pass

    @abstractmethod
    async def get_sales_data_batch(
        self,
        items: List[Dict],
        interval: Tuple[str, str],
        batch_size: int = 5
    ) -> List[Dict]:
        """Obtener datos de ventas para múltiples items"""
        pass
