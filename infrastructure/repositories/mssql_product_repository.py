# infrastructure/repositories/mssql_product_repository.py
"""
Repositorio para consultas a SQL Server (ERP Toolstock)
Gestiona conexión y queries a la base de datos del ERP
"""
import logging
import pyodbc
import asyncio
from typing import List, Dict, Optional
from datetime import datetime
import config.setting as st


class MSSQLProductRepository:
    """
    Repositorio para consultas al ERP (SQL Server)
    Implementa async pattern con pyodbc usando thread pool
    """

    def __init__(self):
        self.connection_string = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self._connection_pool = []
        self._pool_size = 5
        self._initialized = False

    async def init_pool(self):
        """Inicializar pool de conexiones a SQL Server"""
        if self._initialized:
            self.logger.warning("Pool ya inicializado")
            return

        try:
            # Construir connection string
            self.connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={st.setting_db['mssql_toolstock']['HOST']},"
                f"{st.setting_db['mssql_toolstock']['PORT']};"
                f"DATABASE={st.setting_db['mssql_toolstock']['NAME']};"
                f"UID={st.setting_db['mssql_toolstock']['USER']};"
                f"PWD={st.setting_db['mssql_toolstock']['PASS']};"
                f"TrustServerCertificate=yes;"
            )

            # Probar conexión
            await self._test_connection()

            self._initialized = True
            self.logger.info("✅ Pool de conexiones SQL Server inicializado")

        except Exception as e:
            self.logger.error(f"❌ Error inicializando pool SQL Server: {e}")
            raise

    async def _test_connection(self):
        """Probar conexión al servidor"""
        def test():
            conn = pyodbc.connect(self.connection_string, timeout=10)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()

        await asyncio.to_thread(test)
        self.logger.info("✅ Conexión SQL Server verificada")

    async def close_pool(self):
        """Cerrar pool de conexiones"""
        self._initialized = False
        self.logger.info("Pool SQL Server cerrado")

    async def _execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """
        Ejecutar query y retornar resultados como lista de diccionarios

        Args:
            query: SQL query
            params: Parámetros para query (opcional)

        Returns:
            Lista de diccionarios con resultados
        """
        def execute():
            conn = pyodbc.connect(self.connection_string, timeout=30)
            cursor = conn.cursor()

            try:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                # Obtener nombres de columnas
                columns = [column[0] for column in cursor.description]

                # Convertir a lista de diccionarios
                results = []
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))

                return results

            finally:
                cursor.close()
                conn.close()

        return await asyncio.to_thread(execute)

    async def get_active_products_for_inventory(self) -> List[Dict]:
        """
        FUNCIONALIDAD 1: Obtener productos activos para sincronización de inventario

        Query:
        SELECT IdArticulo
        FROM [toolstock].[dbo].[Articulos]
        WHERE Estado = 0 AND IdMarcaArticulo IN (2,3)

        Returns:
            Lista de productos: [{'IdArticulo': 'SKU123'}, ...]
        """
        query = """
        SELECT IdArticulo
        FROM [toolstock].[dbo].[Articulos]
        WHERE Estado = 0 
        AND IdMarcaArticulo IN (2, 3)
        ORDER BY IdArticulo
        """

        try:
            results = await self._execute_query(query)

            self.logger.info(
                f"📦 {len(results)} productos activos encontrados para inventario")
            return results

        except Exception as e:
            self.logger.error(f"❌ Error obteniendo productos activos: {e}")
            raise

    async def get_products_for_verification(self) -> List[Dict]:
        """
        FUNCIONALIDAD 2: Obtener productos para verificación de existencia

        Query con precios de coste y datos completos

        Returns:
            Lista de productos con detalles completos
        """
        query = """
        SELECT
            a.IdArticulo,
            a.Descrip AS Descripcion,
            a.Estado,
            a.CodBarras AS CodigoBarras,
            ISNULL(p.Coste, 0) AS Coste
        FROM [toolstock].[dbo].[Articulos] a
        LEFT JOIN (
            SELECT
                IdArticulo,
                Precio AS Coste
            FROM [toolstock].[dbo].[Listas_Precios_Prov_Art]
            WHERE IdLista = 1
        ) p ON p.IdArticulo = a.IdArticulo
        WHERE a.IdMarcaArticulo IN (2, 3)
        ORDER BY a.IdArticulo
        """

        try:
            results = await self._execute_query(query)

            self.logger.info(
                f"📦 {len(results)} productos para verificación encontrados")
            return results

        except Exception as e:
            self.logger.error(
                f"❌ Error obteniendo productos para verificación: {e}")
            raise

    async def get_products_for_pricing_analysis(self) -> List[Dict]:
        """
        FUNCIONALIDAD 3: Obtener productos activos con datos de coste y peso

        Query para análisis de precios y cálculo de PVPM

        Returns:
            Lista de productos con IdArticulo, Descripcion, Peso, Coste
        """
        query = """
        SELECT
            a.IdArticulo,
            a.Descrip AS Descripcion,
            ISNULL(a.Peso, 0) AS Peso,
            ISNULL(p.Coste, 0) AS Coste
        FROM [toolstock].[dbo].[Articulos] a
        LEFT JOIN (
            SELECT
                IdArticulo,
                Precio AS Coste
            FROM [toolstock].[dbo].[Listas_Precios_Prov_Art]
            WHERE IdLista = 1
        ) p ON p.IdArticulo = a.IdArticulo
        WHERE a.Estado = 0 
        AND a.IdMarcaArticulo IN (2, 3)
        ORDER BY a.IdArticulo
        """

        try:
            results = await self._execute_query(query)

            self.logger.info(
                f"💰 {len(results)} productos para análisis de precios encontrados")
            return results

        except Exception as e:
            self.logger.error(
                f"❌ Error obteniendo productos para pricing: {e}")
            raise

    async def get_products_with_status(self, estado: int) -> List[Dict]:
        """
        Obtener productos filtrados por estado

        Args:
            estado: 0 (activo) o 1 (inactivo)

        Returns:
            Lista de productos: [{'IdArticulo': ..., 'Descripcion': ..., 'Estado': ...}, ...]
        """
        query = """
        SELECT
            IdArticulo,
            Descrip AS Descripcion,
            Estado
        FROM [toolstock].[dbo].[Articulos]
        WHERE IdMarcaArticulo IN (2, 3)
        AND Estado = ?
        ORDER BY IdArticulo
        """

        try:
            results = await self._execute_query(query, (estado,))

            self.logger.info(
                f"📦 {len(results)} productos con estado {estado} encontrados")
            return results

        except Exception as e:
            self.logger.error(
                f"❌ Error obteniendo productos con estado {estado}: {e}")
            raise

    async def get_product_by_sku(self, sku: str) -> Optional[Dict]:
        """
        Obtener información de un producto específico por SKU

        Args:
            sku: IdArticulo/SKU del producto

        Returns:
            Dict con datos del producto o None si no existe
        """
        query = """
        SELECT
            a.IdArticulo,
            a.Descrip AS Descripcion,
            a.Estado,
            a.Peso,
            a.CodBarras AS CodigoBarras,
            ISNULL(p.Coste, 0) AS Coste
        FROM [toolstock].[dbo].[Articulos] a
        LEFT JOIN (
            SELECT
                IdArticulo,
                Precio AS Coste
            FROM [toolstock].[dbo].[Listas_Precios_Prov_Art]
            WHERE IdLista = 1
        ) p ON p.IdArticulo = a.IdArticulo
        WHERE a.IdArticulo = ?
        AND a.IdMarcaArticulo IN (2, 3)
        """

        try:
            results = await self._execute_query(query, (sku,))

            if results:
                self.logger.debug(f"✅ Producto {sku} encontrado")
                return results[0]
            else:
                self.logger.debug(f"⚠️ Producto {sku} no encontrado")
                return None

        except Exception as e:
            self.logger.error(f"❌ Error obteniendo producto {sku}: {e}")
            raise

    async def health_check(self) -> Dict:
        """
        Verificar salud de la conexión a SQL Server

        Returns:
            Dict con estado de salud
        """
        try:
            # Intentar query simple
            query = "SELECT GETDATE() AS ServerTime, @@VERSION AS Version"
            result = await self._execute_query(query)

            if result:
                return {
                    'status': 'healthy',
                    'server_time': result[0]['ServerTime'].isoformat() if result[0].get('ServerTime') else None,
                    # Primera línea
                    'version': result[0].get('Version', '').split('\n')[0][:100],
                    'timestamp': datetime.now().isoformat()
                }
            else:
                return {
                    'status': 'unhealthy',
                    'error': 'No response from server',
                    'timestamp': datetime.now().isoformat()
                }

        except Exception as e:
            self.logger.error(f"❌ Health check failed: {e}")
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
