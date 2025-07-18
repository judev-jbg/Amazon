import aiomysql
from typing import List
from datetime import datetime, timedelta
import config.setting as st

"""
FUNCIONALIDAD:
- Maneja todas las operaciones de base de datos
- Pool de conexiones para eficiencia
- Operaciones asíncronas
"""

class DatabaseManager:
    def __init__(self):
        self.pool = None  # Pool de conexiones

    async def init_pool(self):
        """Inicializar pool de conexiones"""
        self.pool = await aiomysql.create_pool(
            host=st.setting_db["mysql_toolstock"]["HOST"],
            port=int(st.setting_db["mysql_toolstock"]["PORT"]),
            user=st.setting_db["mysql_toolstock"]["USER"],
            password=st.setting_db["mysql_toolstock"]["PASS"],
            db=st.setting_db["mysql_toolstock"]["NAME"],
            minsize=5,
            maxsize=20,
            autocommit=True
        )
        
    async def upsert_orders(self, orders: List[dict]):
        """Insertar o actualizar órdenes"""
        if not orders:
            return
            
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Preparar query de UPSERT
                query = """
                INSERT INTO orders (
                    amazonOrderId, purchaseDate, purchaseDateEs, salesChannel,
                    buyerEmail, orderStatus, marketplaceId, totalOrderAmount,
                    loadDate, loadDateTime
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON DUPLICATE KEY UPDATE
                    purchaseDateEs = VALUES(purchaseDateEs),
                    orderStatus = VALUES(orderStatus),
                    totalOrderAmount = VALUES(totalOrderAmount),
                    loadDateTime = VALUES(loadDateTime)
                """
                
                # Preparar datos para batch insert
                data = [
                    (
                        order['amazonOrderId'],
                        order['purchaseDate'],
                        order['purchaseDateEs'],
                        order['salesChannel'],
                        order['buyerEmail'],
                        order['orderStatus'],
                        order['marketplaceId'],
                        order['totalOrderAmount'],
                        order['loadDate'],
                        order['loadDateTime']
                    )
                    for order in orders
                ]
                
                await cursor.executemany(query, data)
        
    async def get_pending_orders(self) -> List[dict]:
        """Obtener órdenes pendientes de actualización"""
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                query = """
                SELECT amazonOrderId, orderStatus, lastUpdateDate
                FROM orders 
                WHERE orderStatus IN ('Pending', 'Unshipped')
                AND lastUpdateDate < DATE_SUB(NOW(), INTERVAL 1 HOUR)
                """
                await cursor.execute(query)
                return await cursor.fetchall()
        
    async def get_last_sync_time(self) -> datetime:
        """Obtener timestamp de última sincronización"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = "SELECT MAX(loadDateTime) FROM orders"
                await cursor.execute(query)
                result = await cursor.fetchone()
                return result[0] if result[0] else datetime.now() - timedelta(hours=1)
