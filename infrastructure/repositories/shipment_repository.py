"""
Repositorio para operaciones sobre shipments
"""

import logging
from typing import List
from datetime import datetime
import aiomysql
import pandas as pd
import numpy as np

from domain.interfaces.repository_interfaces import IShipmentRepository


class ShipmentRepository(IShipmentRepository):
    """Repositorio especializado en shipments"""

    def __init__(self, pool, prestashop_pool):
        self.pool = pool
        self.prestashop_pool = prestashop_pool
        self.logger = logging.getLogger(self.__class__.__name__)

    async def update_shipment_order_details(self, df: pd.DataFrame) -> bool:
        """Actualizar ordersdetail con datos de envío"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    for _, row in df.iterrows():
                        query = """
                        UPDATE ordersdetail 
                        SET order_status = 'Shipped',
                            uIdExp = %s,
                            expeditionTraking = %s,
                            codBar = %s,
                            lastDateTimeUpdated = %s
                        WHERE orderId = %s
                        """

                        await cursor.execute(query, (
                            row['Referencia'],
                            row['Expedicion'],
                            row['codbar'],
                            datetime.now(),
                            row['DptoDst']
                        ))
            return True
        except Exception:
            return False

    async def update_shipment_orders(self, df: pd.DataFrame) -> bool:
        """Actualizar orders con datos de envío"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    for _, row in df.iterrows():
                        query = """
                        UPDATE orders 
                        SET expeditionTraking = %s
                        WHERE amazonOrderId = %s
                        """

                        await cursor.execute(query, (
                            row['Expedicion'],
                            row['DptoDst']
                        ))
            return True
        except Exception:
            return False

    async def update_shipment_prestashop(self, df: pd.DataFrame) -> bool:
        """Actualizar ps_order_carrier con datos de envío"""
        try:

            async with self.prestashop_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    for _, row in df.iterrows():
                        if pd.notna(row.get('id_order_ps')):
                            query = """
                            UPDATE ps_order_carrier 
                            SET tracking_number = %s,
                                tracking_cod_bar = %s
                            WHERE id_order = %s
                            """

                            await cursor.execute(query, (
                                row['Expedicion'],
                                row['codbar'],
                                row['id_order_ps']
                            ))
            return True
        except Exception:
            return False
