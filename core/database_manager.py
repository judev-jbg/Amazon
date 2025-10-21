import logging
import aiomysql
import config.setting as st

from infrastructure.repositories.order_repository import OrderRepository
from infrastructure.repositories.order_item_repository import OrderItemRepository
from infrastructure.repositories.sales_repository import SalesRepository
from infrastructure.repositories.order_detail_repository import OrderDetailRepository
from infrastructure.repositories.shipment_repository import ShipmentRepository


class DatabaseManager:
    """
    Gestor de conexiones y fábrica de repositorios.
    Gestiona pools de conexión.
    """

    def __init__(self):
        self.pool = None
        self.prestashop_pool = None
        self.logger = logging.getLogger("DatabaseManager")

        # Repositorios lazy-loaded
        self._order_repo = None
        self._order_item_repo = None
        self._sales_repo = None
        self._order_detail_repo = None
        self._shipment_repo = None

    async def init_pool(self):
        """Inicializar pool de conexiones principal"""
        self.pool = await aiomysql.create_pool(
            host=st.setting_db["mysql_toolstock"]["HOST"],
            port=int(st.setting_db["mysql_toolstock"]["PORT"]),
            user=st.setting_db["mysql_toolstock"]["USER"],
            password=st.setting_db["mysql_toolstock"]["PASS"],
            db=st.setting_db["mysql_toolstock"]["NAME"],
            minsize=5,
            maxsize=20,
            autocommit=True,
            charset='utf8mb4'
        )
        self.logger.info("Pool de conexiones Local inicializado")

    async def init_prestashop_pool(self):
        """Inicializar pool de Prestashop"""
        self.prestashop_pool = await aiomysql.create_pool(
            host=st.setting_db["mysql_toolstock_ps"]["HOST"],
            port=int(st.setting_db["mysql_toolstock_ps"]["PORT"]),
            user=st.setting_db["mysql_toolstock_ps"]["USER"],
            password=st.setting_db["mysql_toolstock_ps"]["PASS"],
            db=st.setting_db["mysql_toolstock_ps"]["NAME"],
            minsize=2,
            maxsize=5,
            autocommit=True
        )
        self.logger.info("Pool de conexiones Prestashop inicializado")

    # Factory methods para repositorios
    @property
    def orders(self) -> OrderRepository:
        """Obtener repositorio de órdenes"""
        if self._order_repo is None:
            self._order_repo = OrderRepository(self.pool)
        return self._order_repo

    @property
    def order_items(self) -> OrderItemRepository:
        """Obtener repositorio de items"""
        if self._order_item_repo is None:
            self._order_item_repo = OrderItemRepository(self.pool)
        return self._order_item_repo

    @property
    def sales(self) -> SalesRepository:
        """Obtener repositorio de ventas"""
        if self._sales_repo is None:
            self._sales_repo = SalesRepository(self.pool)
        return self._sales_repo

    @property
    def order_details(self) -> OrderDetailRepository:
        """Obtener repositorio de order details"""
        if self._order_detail_repo is None:
            self._order_detail_repo = OrderDetailRepository(
                self.pool, self.prestashop_pool)
        return self._order_detail_repo

    @property
    def shipments(self) -> ShipmentRepository:
        """Obtener repositorio de shipments"""
        if self._shipment_repo is None:
            self._shipment_repo = ShipmentRepository(
                self.pool, self.prestashop_pool)
        return self._shipment_repo

    async def close_pool(self):
        """Cerrar pool principal"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.logger.info("Pool Local cerrado")

    async def close_pool_prestashop(self):
        """Cerrar pool Prestashop"""
        if self.prestashop_pool:
            self.prestashop_pool.close()
            await self.prestashop_pool.wait_closed()
            self.logger.info("Pool Prestashop cerrado")
