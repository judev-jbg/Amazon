"""
Repositorio para operaciones de estadísticas de ventas
"""
import logging
from typing import List, Dict
from datetime import datetime, date

from domain.interfaces.repository_interfaces import ISalesRepository


class SalesRepository(ISalesRepository):
    """Repositorio especializado en ventas"""

    def __init__(self, pool):
        self.pool = pool
        self.logger = logging.getLogger(self.__class__.__name__)

    async def upsert_sales(self, sales: List[Dict]) -> None:
        """Insertar o actualizar estadísticas de ventas"""
        if not sales:
            return

        self.logger.info(f"Upserting {len(sales)} estadísticas de ventas")

        query = self._build_upsert_query()
        data = self._prepare_sales_data(sales)

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(query, data)

        self.logger.info(f"Upsert exitoso de {len(sales)} estadísticas")

    def _build_upsert_query(self) -> str:
        """Construir query de upsert para ventas"""
        return """
            INSERT INTO sales (
                asin, sku, marketplaceId, saleDateTime, saleDate, intervalHour,
                saleDateEs, intervalHourEs, qOrders, avgPriceUndCurrencyCode,
                avgPriceUndAmount, undSold, totalPriceSoldCurrencyCode,
                totalPriceSoldAmount, loadDate, loadDateTime
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON DUPLICATE KEY UPDATE
                qOrders = VALUES(qOrders),
                avgPriceUndCurrencyCode = VALUES(avgPriceUndCurrencyCode),
                avgPriceUndAmount = VALUES(avgPriceUndAmount),
                undSold = VALUES(undSold),
                totalPriceSoldCurrencyCode = VALUES(totalPriceSoldCurrencyCode),
                totalPriceSoldAmount = VALUES(totalPriceSoldAmount),
                loadDateTime = VALUES(loadDateTime)
        """

    def _prepare_sales_data(self, sales: List[Dict]) -> List[tuple]:
        """Preparar datos de ventas para inserción"""
        return [
            (
                sale.get('asin'),
                sale.get('sku'),
                sale.get('marketplaceId'),
                sale.get('saleDateTime'),
                sale.get('saleDate'),
                sale.get('intervalHour'),
                sale.get('saleDateEs'),
                sale.get('intervalHourEs'),
                sale.get('qOrders', 0),
                sale.get('avgPriceUndCurrencyCode'),
                sale.get('avgPriceUndAmount', 0.00),
                sale.get('undSold', 0),
                sale.get('totalPriceSoldCurrencyCode'),
                sale.get('totalPriceSoldAmount', 0.00),
                sale.get('loadDate', date.today()),
                sale.get('loadDateTime', datetime.now())
            )
            for sale in sales
        ]
