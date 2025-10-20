import logging
import aiomysql
import numpy as np
import config.setting as st
import pandas as pd
from typing import List
from typing import List
from datetime import datetime, timedelta, date
from infrastructure.error_handling import EnhancedErrorHandler

"""
FUNCIONALIDAD:
- Maneja todas las operaciones de base de datos
- Pool de conexiones para eficiencia
- Operaciones asíncronas
"""


class DatabaseManager:
    def __init__(self):
        self.error_handler = EnhancedErrorHandler()
        self.pool = None  # Pool de conexiones mysql_toolstock
        self.prestashop_pool = None  # Pool de conexiones nysql_prestashop
        self.logger = logging.getLogger("AmazonManagement")

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
            autocommit=True,
            charset='utf8mb4'
        )
        self.logger.info("Pool de conexiones Local inicializado correctamente")

    async def init_prestashop_pool(self):
        """Obtener pool de conexiones de Prestashop"""
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

        self.logger.info(
            "Pool de conexiones Prestashop inicializado correctamente")

    async def upsert_orders(self, orders: List[dict]):
        """Insertar o actualizar órdenes"""
        if not orders:
            return

        print(f"Upserting {len(orders)} ordenes")

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Preparar query de UPSERT
                query = """
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

                # Preparar datos para batch insert
                data_tuples = []
                for order in orders:

                    data_tuple = (
                        # Columnas principales
                        order.get('purchaseDate'),
                        order.get('purchaseDateEs'),
                        order.get('salesChannel'),
                        order.get('amazonOrderId'),
                        order.get('buyerEmail'),

                        # Fechas de envío
                        order.get('earliestShipDate'),
                        order.get('latestShipDate'),
                        order.get('earliestDeliveryDate'),
                        order.get('latestDeliveryDate'),
                        order.get('lastUpdateDate'),

                        # Información adicional
                        order.get('isBusinessOrder', 0),
                        order.get('marketplaceId'),
                        order.get('numberOfItemsShipped', 0),
                        order.get('numberOfItemsUnshipped', 0),
                        order.get('orderStatus'),

                        # Información monetaria
                        order.get('totalOrderCurrencyCode'),
                        order.get('totalOrderAmount', 0.00),

                        # Dirección
                        order.get('city'),
                        order.get('countryCode'),
                        order.get('postalCode'),
                        order.get('stateOrRegion'),

                        # Tracking y metadatos
                        # Mantener en NULL por ahora
                        order.get('expeditionTraking'),
                        order.get('isShipFake', 0),      # Default 0
                        order.get('loadDate', date.today()),
                        order.get('loadDateTime', datetime.now())
                    )
                    data_tuples.append(data_tuple)

                await cursor.executemany(query, data_tuples)

        print(f"Upserting exitoso de {len(orders)} ordenes")

    async def update_order_status_only(self, orders: List[dict]):
        """Actualizar solo el status de las órdenes (para status_update mode)"""
        if not orders:
            return

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                for order in orders:
                    query = """
                    UPDATE orders 
                    SET orderStatus = %s,
                        lastUpdateDate = %s,
                        loadDateTime = %s
                    WHERE amazonOrderId = %s
                    """

                    await cursor.execute(query, (
                        order['orderStatus'],
                        order['lastUpdateDate'],
                        datetime.now(),
                        order['amazonOrderId']
                    ))

        print(f"✅ Actualizados {len(orders)} estados de órdenes")

    async def upsert_order_items(self, order_items: List[dict]):
        """Insertar o actualizar elementos de ordenes"""

        if not order_items:
            return

        print(f"Upserting de {len(order_items)} elemento de ordenes")

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = """
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

                data_tuples = []
                for item in order_items:

                    data_tuple = (
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
                    data_tuples.append(data_tuple)

                await cursor.executemany(query, data_tuples)

        print(
            f"Upserting exitoso de {len(order_items)} elemento(s) de ordenes")

    async def upsert_sales(self, sales: List[dict]):
        """Insertar o actualizar estadisticas de ventas"""
        if not sales:
            return

        print(f"Upserting de {len(sales)} estadisticas de ventas")

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = """
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

                data_tuples = []
                for sale in sales:
                    data_tuple = (
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
                    data_tuples.append(data_tuple)

                await cursor.executemany(query, data_tuples)

        print(f"Upserting exitoso de {len(sales)} estadisticas de ventas")

    async def get_pending_orders(self) -> List[dict]:
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
                orders = await cursor.fetchall()

                return orders

    async def get_last_sync_time(self, table_name: str = 'orders') -> datetime:
        """Obtener timestamp de última sincronización"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = f"SELECT MAX(loadDateTime) FROM {table_name}"
                await cursor.execute(query)
                result = await cursor.fetchone()
                return result[0] if result[0] else datetime.now() - timedelta(hours=1)

    async def get_stale_orders(self, older_than: timedelta) -> List[dict]:
        """Obtener órdenes que necesitan reproceso"""
        cutoff_date = datetime.now() - older_than
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                query = """
                SELECT amazonOrderId, orderStatus, lastUpdateDate
                FROM orders 
                WHERE orderStatus IN ('Pending', 'Unshipped','Canceled')
                """
                await cursor.execute(query, cutoff_date)
                return await cursor.fetchall()

    async def get_existing_order_details(self, unique_keys: List[str]) -> pd.DataFrame:
        """Obtener detalles de órdenes existentes para comparación"""
        if not unique_keys:
            return pd.DataFrame()

        placeholders = ','.join(['%s'] * len(unique_keys))
        query = f"""
        SELECT CONCAT(orderId, '|', purchaseDate, '|', orderItemId) as unique_key,
               isAmazonInvoiced, isBuyerRequestedCancellation, buyerRequestedCancelReason
        FROM ordersdetail 
        WHERE CONCAT(orderId, '|', purchaseDate, '|', orderItemId) IN ({placeholders})
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, unique_keys)
                results = await cursor.fetchall()
                return pd.DataFrame(results) if results else pd.DataFrame()

    async def insert_order_details(self, df: pd.DataFrame):
        """Insertar nuevos registros de OrderDetails"""

        if df.empty:
            self.logger.info(
                "El dataframe esta vacio, no se continua con la insercion en la BD")
            return

        df_clean = df.copy()
        if 'unique_key' in df_clean.columns:
            df_clean = df_clean.drop(columns=['unique_key'])
        df_clean = self._clean_dataframe_for_mysql(df_clean)

        # Preparar query de inserción con todas las columnas
        columns = df_clean.columns.tolist()
        placeholders = ','.join(['%s'] * len(columns))
        columns_str = ','.join(columns)

        query = f"INSERT INTO ordersdetail ({columns_str}) VALUES ({placeholders})"

        # Preparar datos
        data = [tuple(row) for row in df_clean.values]

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(query, data)

    async def update_order_details(self, df: pd.DataFrame):
        """Actualizar registros existentes de OrderDetails"""
        if df.empty:
            return

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                for _, row in df.iterrows():
                    query = """
                    UPDATE ordersdetail 
                    SET isAmazonInvoiced = %s,
                        isBuyerRequestedCancellation = %s,
                        buyerRequestedCancelReason = %s,
                        lastDateTimeUpdated = %s
                    WHERE orderId = %s AND orderItemId = %s AND purchaseDate = %s
                    """

                    await cursor.execute(query, (
                        row['isAmazonInvoiced'],
                        row['isBuyerRequestedCancellation'],
                        row['buyerRequestedCancelReason'],
                        row['lastDateTimeUpdated'],
                        row['orderId'],
                        row['orderItemId'],
                        row['purchaseDate']
                    ))

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

    async def update_asin_references(self):
        """Actualizar referencias ASIN usando procedimiento almacenado"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("CALL toolstock_amz.uSp_updateAsinAndRefProv()")

            self.logger.info(
                "ASIN y Referecia de proveedor actualizadas exitosamente")
            return True

        except Exception as e:
            self.logger.error(f"❌ Error actualizando referencias ASIN: {e}")
            raise Exception(
                f"Error ejecutando uSp_updateAsinAndRefProv: {str(e)}")

    async def get_orders_without_ps_reference(self) -> pd.DataFrame:
        """Obtener órdenes sin referencia de Prestashop"""
        try:
            query = """
            SELECT DISTINCT orderId 
            FROM ordersdetail 
            WHERE id_order_ps IS NULL 
            AND orderId IS NOT NULL
            """

            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(query)
                    results = await cursor.fetchall()
                    return pd.DataFrame(results) if results else pd.DataFrame()

        except Exception as e:
            raise Exception(
                f"Error obteniendo órdenes sin referencia PS: {str(e)}")

    # TODO: METODO PARA ELIMINAR
    async def update_prestashop_order_references(self, orders_df: pd.DataFrame):
        """Actualizar referencias de Prestashop en ordersdetail"""
        if orders_df.empty:
            return

        try:

            async with self.prestashop_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    # Obtener mapeo de órdenes de Amazon a Prestashop
                    query = """
                    SELECT 
                        CASE WHEN marketplace_order_id IS NULL THEN reference 
                        ELSE marketplace_order_id 
                        END AS marketplace_order_id, 
                        o.id_order as id_order_ps, 
                        reference as reference_ps 
                    FROM ps_orders o
                    LEFT JOIN (
                        SELECT id_order, marketplace_order_id  
                        FROM toolstock_ps.ps_beezup_order
                    ) bo ON bo.id_order = o.id_order
                    WHERE current_state IN (1, 2, 3, 6, 10, 11, 14, 15, 16, 21)
                    AND (marketplace_order_id IN %s OR reference IN %s)
                    """

                    order_ids = orders_df['orderId'].tolist()
                    await cursor.execute(query, (order_ids, order_ids))
                    ps_mappings = await cursor.fetchall()

                    if ps_mappings:
                        ps_df = pd.DataFrame(ps_mappings)

                        # Actualizar ordersdetail con referencias de Prestashop
                        async with self.pool.acquire() as amz_conn:
                            async with amz_conn.cursor() as amz_cursor:
                                for _, row in ps_df.iterrows():
                                    update_query = """
                                    UPDATE ordersdetail 
                                    SET id_order_ps = %s,
                                        reference_ps = %s,
                                        lastDateTimeUpdated = %s
                                    WHERE orderId = %s
                                    """

                                    await amz_cursor.execute(update_query, (
                                        row['id_order_ps'],
                                        row['reference_ps'],
                                        datetime.now(),
                                        row['marketplace_order_id']
                                    ))

            print(
                f"✅ Actualizadas {len(ps_mappings)} referencias de Prestashop")

        except Exception as e:
            raise Exception(
                f"Error actualizando referencias Prestashop: {str(e)}")

    def _clean_dataframe_for_mysql(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpiar dataframe para compatibilidad con MySQL"""
        df_clean = df.copy()

        # 1. Reemplazar NaN de pandas
        df_clean = df_clean.fillna(np.nan)

        # 2. Reemplazar string 'nan'
        df_clean = df_clean.replace(['nan', 'NaN', 'NaT'], None)

        # 3. Convertir timestamps de pandas a datetime de Python
        for col in df_clean.columns:
            if df_clean[col].dtype == 'datetime64[ns]':
                df_clean[col] = df_clean[col].dt.to_pydatetime()
            elif pd.api.types.is_datetime64_any_dtype(df_clean[col]):
                df_clean[col] = pd.to_datetime(
                    df_clean[col]).dt.to_pydatetime()

        # 4. Limpiar valores específicos problemáticos
        for col in df_clean.columns:
            df_clean[col] = df_clean[col].apply(
                lambda x: None if pd.isna(x) or str(x).lower() == 'nan' else x)

        return df_clean

    async def close_pool(self):
        """Cerrar pool de conexiones"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.logger.info(
                "Pool de conexiones de la base de datos Local cerrando correctamente")

    async def close_pool_prestashop(self):
        """Cerrar pool de conexiones"""
        if self.prestashop_pool:
            self.prestashop_pool.close()
            await self.prestashop_pool.wait_closed()
            self.logger.info(
                "Pool de conexiones de la base de datos Prestashop cerrando correctamente")
