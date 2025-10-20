"""
Repositorio para operaciones de items de órdenes
"""
import logging
from typing import List, Dict
from datetime import datetime, date
import aiomysql

from domain.interfaces.repository_interfaces import IOrderItemRepository


class OrderItemRepository(IOrderItemRepository):
    """Repositorio especializado en items de órdenes"""

    def __init__(self, pool):
        self.pool = pool
        self.logger = logging.getLogger(self.__class__.__name__)

    async def upsert_order_items(self, order_items: List[Dict]) -> None:
        """Insertar o actualizar elementos de órdenes"""
        if not order_items:
            return

        self.logger.info(f"Upserting {len(order_items)} items de órdenes")

        query = self._build_upsert_query()
        data = self._prepare_item_data(order_items)

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(query, data)

        self.logger.info(f"Upsert exitoso de {len(order_items)} items")

    def _build_upsert_query(self) -> str:
        """Construir query de upsert para items"""
        return """
            INSERT INTO orderitems (
                orderId, orderItemId, asin, sku, title, conditionItem, nItems,
                qOrdered, qShipped, reasonCancel, isRequestedCancel,
                itemPriceCurrencyCode, itemPriceCurrencyAmount,
                itemTaxCurrencyCode, itemTaxCurrencyAmount,
                loadDate, loadDateTime
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON DUPLICATE KEY UPDATE
                asin = VALUES(asin),
                sku = VALUES(sku),
                title = VALUES(title),
                conditionItem = VALUES(conditionItem),
                nItems = VALUES(nItems),
                qOrdered = VALUES(qOrdered),
                qShipped = VALUES(qShipped),
                reasonCancel = VALUES(reasonCancel),
                isRequestedCancel = VALUES(isRequestedCancel),
                itemPriceCurrencyCode = VALUES(itemPriceCurrencyCode),
                itemPriceCurrencyAmount = VALUES(itemPriceCurrencyAmount),
                itemTaxCurrencyCode = VALUES(itemTaxCurrencyCode),
                itemTaxCurrencyAmount = VALUES(itemTaxCurrencyAmount),
                loadDateTime = VALUES(loadDateTime)
        """

    def _prepare_item_data(self, order_items: List[Dict]) -> List[tuple]:
        """Preparar datos de items para inserción"""
        return [
            (
                item.get('orderId'),
                item.get('orderItemId'),
                item.get('asin'),
                item.get('sku'),
                item.get('title'),
                item.get('conditionItem'),
                item.get('nItems', 1),
                item.get('qOrdered', 0),
                item.get('qShipped', 0),
                item.get('reasonCancel'),
                item.get('isRequestedCancel', False),
                item.get('itemPriceCurrencyCode'),
                item.get('itemPriceCurrencyAmount', 0.00),
                item.get('itemTaxCurrencyCode'),
                item.get('itemTaxCurrencyAmount', 0.00),
                item.get('loadDate', date.today()),
                item.get('loadDateTime', datetime.now())
            )
            for item in order_items
        ]
