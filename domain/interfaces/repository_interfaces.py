
"""
Interfaces para repositorios - definen contratos entre capas
"""
from abc import ABC, abstractmethod
from typing import List, Dict
from datetime import datetime, timedelta
import pandas as pd


class IOrderRepository(ABC):
    """Interface para operaciones de órdenes"""

    @abstractmethod
    async def get_pending_orders(self) -> List[Dict]:
        """Obtener órdenes pendientes de actualización"""
        pass

    @abstractmethod
    async def upsert_orders(self, orders: List[Dict]) -> None:
        """Insertar o actualizar órdenes"""
        pass

    @abstractmethod
    async def update_order_status_only(self, orders: List[Dict]) -> None:
        """Actualizar solo el status de órdenes"""
        pass

    @abstractmethod
    async def get_stale_orders(self, older_than: timedelta) -> List[Dict]:
        """Obtener órdenes que necesitan reproceso"""
        pass

    @abstractmethod
    async def get_last_sync_time(self, table_name: str) -> datetime:
        """Obtener timestamp de última sincronización"""
        pass


class IOrderItemRepository(ABC):
    """Interface para operaciones de items de órdenes"""

    @abstractmethod
    async def upsert_order_items(self, order_items: List[Dict]) -> None:
        """Insertar o actualizar items de órdenes"""
        pass


class ISalesRepository(ABC):
    """Interface para operaciones de ventas"""

    @abstractmethod
    async def upsert_sales(self, sales: List[Dict]) -> None:
        """Insertar o actualizar estadísticas de ventas"""
        pass


class IOrderDetailRepository(ABC):
    """Interface para operaciones de order details"""

    @abstractmethod
    async def get_existing_order_details(self, unique_keys: List[str]) -> pd.DataFrame:
        """Obtener detalles de órdenes existentes"""
        pass

    @abstractmethod
    async def insert_order_details(self, df: pd.DataFrame) -> None:
        """Insertar nuevos order details"""
        pass

    @abstractmethod
    async def update_order_details(self, df: pd.DataFrame) -> None:
        """Actualizar order details existentes"""
        pass

    @abstractmethod
    async def update_asin_references(self) -> bool:
        """Actualizar referencias ASIN usando SP"""
        pass

    @abstractmethod
    async def get_orders_without_ps_reference(self) -> pd.DataFrame:
        """Obtener órdenes sin referencia Prestashop"""
        pass

    @abstractmethod
    async def update_prestashop_order_references(self, orders_df: pd.DataFrame) -> None:
        """Actualizar referencias de Prestashop"""
        pass


class IShipmentRepository(ABC):
    """Interface para operaciones de envíos"""

    @abstractmethod
    async def update_shipment_order_details(self, df: pd.DataFrame) -> bool:
        """Actualizar ordersdetail con datos de envío"""
        pass

    @abstractmethod
    async def update_shipment_orders(self, df: pd.DataFrame) -> bool:
        """Actualizar orders con datos de envío"""
        pass

    @abstractmethod
    async def update_shipment_prestashop(self, df: pd.DataFrame) -> bool:
        """Actualizar ps_order_carrier con datos de envío"""
        pass
