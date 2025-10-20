"""
Repositorio para operaciones orders details
"""

import logging
from typing import List
from datetime import datetime
import aiomysql
import pandas as pd
import numpy as np

from domain.interfaces.repository_interfaces import IOrderDetailRepository


class OrderDetailRepository(IOrderDetailRepository):
    """Repositorio especializado en orders detail"""

    def __init__(self, pool):
        self.pool = pool
        self.logger = logging.getLogger(self.__class__.__name__)

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
