"""
Repositorio para operaciones de órdenes
Responsabilidad única: Gestionar persistencia de órdenes
"""
import logging
from typing import List, Dict
from datetime import datetime, timedelta, date

import aiomysql

from domain.interfaces.repository_interfaces import IOrderRepository


class OrderRepository(IOrderRepository):
    """Repositorio especializado en órdenes"""

    def __init__(self, pool):
        self.pool = pool
        self.logger = logging.getLogger(self.__class__.__name__)

    async def get_pending_orders(self) -> List[Dict]:
        """Obtener órdenes pendientes de actualización"""
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                query = """
                SELECT amazonOrderId, orderStatus, lastUpdateDate
                FROM orders 
                WHERE orderStatus IN ('Pending', 'Unshipped')
                LIMIT 1000
                """
                await cursor.execute(query)
                return await cursor.fetchall()

    async def upsert_orders(self, orders: List[Dict]) -> None:
        """Insertar o actualizar órdenes"""
        if not orders:
            return

        self.logger.info(f"Upserting {len(orders)} órdenes")

        query = self._build_upsert_query()
        data = self._prepare_order_data(orders)

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(query, data)

        self.logger.info(f"Upsert exitoso de {len(orders)} órdenes")

    async def update_order_status_only(self, orders: List[Dict]) -> None:
        """Actualizar solo el status de las órdenes"""
        if not orders:
            return

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = """
                UPDATE orders 
                SET orderStatus = %s,
                    lastUpdateDate = %s,
                    loadDateTime = %s
                WHERE amazonOrderId = %s
                """

                data = [
                    (
                        order['orderStatus'],
                        order['lastUpdateDate'],
                        datetime.now(),
                        order['amazonOrderId']
                    )
                    for order in orders
                ]

                await cursor.executemany(query, data)

        self.logger.info(f"Actualizados {len(orders)} estados de órdenes")

    async def get_stale_orders(self, older_than: timedelta) -> List[Dict]:
        """Obtener órdenes que necesitan reproceso"""
        cutoff_date = datetime.now() - older_than

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                query = """
                SELECT amazonOrderId, orderStatus, lastUpdateDate
                FROM orders 
                WHERE orderStatus IN ('Pending')
                AND lastUpdateDate < %s
                """
                await cursor.execute(query, (cutoff_date,))
                return await cursor.fetchall()

    async def get_last_sync_time(self, table_name: str = 'orders') -> datetime:
        """Obtener timestamp de última sincronización"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = f"SELECT MAX(loadDateTime) FROM {table_name}"
                await cursor.execute(query)
                result = await cursor.fetchone()
                return result[0] if result[0] else datetime.now() - timedelta(hours=1)

    async def delete_orders(self, order_ids: List[str]) -> None:
        """Eliminar órdenes por amazonOrderId"""
        if not order_ids:
            return

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = "DELETE FROM orders WHERE amazonOrderId = %s"
                data = [(order_id,) for order_id in order_ids]
                await cursor.executemany(query, data)

        self.logger.info(f"Eliminadas {len(order_ids)} órdenes")

    def _build_upsert_query(self) -> str:
        """Construir query de upsert"""
        return """
            INSERT INTO orders (
                purchaseDate, purchaseDateEs, salesChannel, amazonOrderId, buyerEmail,
                earliestShipDate, latestShipDate, earliestDeliveryDate, latestDeliveryDate,
                lastUpdateDate, isBusinessOrder, marketplaceId, numberOfItemsShipped, 
                numberOfItemsUnshipped, orderStatus, totalOrderCurrencyCode, totalOrderAmount,
                city, countryCode, postalCode, stateOrRegion, expeditionTraking, 
                isShipFake, loadDate, loadDateTime
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON DUPLICATE KEY UPDATE
                purchaseDate = VALUES(purchaseDate),
                purchaseDateEs = VALUES(purchaseDateEs),
                salesChannel = VALUES(salesChannel),
                buyerEmail = VALUES(buyerEmail),
                earliestShipDate = VALUES(earliestShipDate),
                latestShipDate = VALUES(latestShipDate),
                earliestDeliveryDate = VALUES(earliestDeliveryDate),
                latestDeliveryDate = VALUES(latestDeliveryDate),
                lastUpdateDate = VALUES(lastUpdateDate),
                isBusinessOrder = VALUES(isBusinessOrder),
                numberOfItemsShipped = VALUES(numberOfItemsShipped),
                numberOfItemsUnshipped = VALUES(numberOfItemsUnshipped),
                orderStatus = VALUES(orderStatus),
                totalOrderCurrencyCode = VALUES(totalOrderCurrencyCode),
                totalOrderAmount = VALUES(totalOrderAmount),
                city = VALUES(city),
                countryCode = VALUES(countryCode),
                postalCode = VALUES(postalCode),
                stateOrRegion = VALUES(stateOrRegion),
                loadDateTime = VALUES(loadDateTime)
        """

    def _prepare_order_data(self, orders: List[Dict]) -> List[tuple]:
        """Preparar datos para inserción en lote"""
        return [
            (
                order.get('purchaseDate'),
                order.get('purchaseDateEs'),
                order.get('salesChannel'),
                order.get('amazonOrderId'),
                order.get('buyerEmail'),
                order.get('earliestShipDate'),
                order.get('latestShipDate'),
                order.get('earliestDeliveryDate'),
                order.get('latestDeliveryDate'),
                order.get('lastUpdateDate'),
                order.get('isBusinessOrder', 0),
                order.get('marketplaceId'),
                order.get('numberOfItemsShipped', 0),
                order.get('numberOfItemsUnshipped', 0),
                order.get('orderStatus'),
                order.get('totalOrderCurrencyCode'),
                order.get('totalOrderAmount', 0.00),
                order.get('city'),
                order.get('countryCode'),
                order.get('postalCode'),
                order.get('stateOrRegion'),
                order.get('expeditionTraking'),
                order.get('isShipFake', 0),
                order.get('loadDate', date.today()),
                order.get('loadDateTime', datetime.now())
            )
            for order in orders
        ]
