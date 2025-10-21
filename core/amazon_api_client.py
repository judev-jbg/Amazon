import asyncio
import datetime
import aiohttp
from typing import List, Dict, Tuple, Any
from infrastructure.rate_limiter import RateLimiter, APIEndpoint, rate_limited
from infrastructure.decorators.retry_decorator import async_retry
from core.api.amazon_sp_api_wrapper import AmazonSPAPIWrapper

"""
FUNCIONALIDAD:
- Interfaz única para todas las llamadas a Amazon SP-API
- Rate limiting automático
- Retry con backoff exponencial
- Paginación automática
"""


class AmazonAPIClient:
    def __init__(self):
        self.session = None
        self.rate_limiter = RateLimiter(max_requests=100, window=60)
        self.api_wrapper = AmazonSPAPIWrapper()

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_orders_paginated(self, date_from: datetime, date_to: datetime,
                                   markets: List[str]) -> List[dict]:
        """Obtener órdenes con paginación automática y rate limiting"""
        all_orders = []

        for market in markets:
            async with self.rate_limiter:
                orders = await self._get_orders_for_market(
                    market, date_from, date_to
                )
                all_orders.extend(orders)

        return all_orders

    @rate_limited(APIEndpoint.ORDERS)
    @async_retry(max_retries=3, backoff_base=2)
    async def _get_orders_for_market(self, market: str, date_from: datetime,
                                     date_to: datetime) -> List[dict]:
        """
        Obtener ordenes para un mercado específico
        Ahora usa el wrapper limpio en lugar de libs/transform.py
        """
        print("#" * 5, "=" * 70)
        print("#" * 5, " GET ORDERS")

        # Usar wrapper asíncrono
        result = await asyncio.to_thread(
            self.api_wrapper.get_orders,
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
            markets=[market]
        )

        orders, success = result

        if success and orders:
            return orders

        if success and not orders:
            return []

        # Error - verificar rate limit
        if orders and isinstance(orders, list) and len(orders) > 0:
            if isinstance(orders[0], dict) and orders[0].get('code') == 429:
                print("#" * 5, " -Rate limit alcanzado")

        raise Exception("API request failed")

    @rate_limited(APIEndpoint.ORDER_ITEMS)
    @async_retry(max_retries=3, backoff_base=2)
    async def get_order_items(self, order_id: str) -> List[Dict]:
        """Obtener elementos de una orden usando wrapper"""
        result = await asyncio.to_thread(
            self.api_wrapper.get_order_items,
            order_id=order_id
        )

        items, success = result

        if success:
            print(
                f"Elementos de la orden {order_id} recuperados: {len(items)} elemento(s)")
            return items

        raise Exception(f"Error obteniendo items para {order_id}")

    @rate_limited(APIEndpoint.ORDER)
    @async_retry(max_retries=3, backoff_base=2)
    async def get_order(self, order_id: str) -> Dict:
        """Obtener una orden específica usando wrapper"""
        result = await asyncio.to_thread(
            self.api_wrapper.get_order,
            order_id=order_id
        )

        order, success = result

        if success and order:
            print(f"Orden {order_id} recuperada con éxito")
            return order

        raise Exception(f"Error obteniendo orden {order_id}")

    @rate_limited(APIEndpoint.SALES)
    @async_retry(max_retries=3, backoff_base=2)
    async def get_sales_data(self, asin: str, sku: str, market: List[str],
                             interval: Tuple[str, str]) -> List[Dict]:
        """Obtener datos de ventas usando wrapper"""
        result = await asyncio.to_thread(
            self.api_wrapper.get_sales,
            asin=asin,
            sku=sku,
            marketplace=market[0],
            interval=interval
        )

        sales, success = result

        if success:
            print(
                f"Métricas de venta para {asin}/{sku}: {len(sales)} métrica(s)")
            return sales

        raise Exception(f"Error obteniendo ventas para {asin}/{sku}")

    async def get_order_items_batch(self, order_ids: List[str], batch_size: int = 10) -> Dict[str, List[Dict]]:
        """
        Obtener elementos de múltiples órdenes en lotes

        Args:
            order_ids: Lista de IDs de órdenes
            batch_size: Tamaño del lote para procesamiento paralelo

        Returns:
            Diccionario con order_id como clave y lista de items como valor
        """
        result = {}

        # Procesar en lotes para evitar saturar la API
        for i in range(0, len(order_ids), batch_size):
            batch = order_ids[i:i + batch_size]

            # Procesar lote en paralelo
            tasks = [self.get_order_items(order_id) for order_id in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Procesar resultados
            for order_id, items in zip(batch, batch_results):
                if isinstance(items, Exception):
                    print(
                        f"⚠️ Error obteniendo elemento de la orden {order_id}: {items}")
                    result[order_id] = []
                else:
                    result[order_id] = items

            # Pequeña pausa entre lotes
            if i + batch_size < len(order_ids):
                await asyncio.sleep(1)

        return result

    async def get_sales_data_batch(self, items: List[Dict], interval: Tuple[str, str],
                                   batch_size: int = 5) -> List[Dict]:
        """
        Obtener datos de ventas para múltiples items en lotes

        Args:
            items: Lista de items con 'asin', 'sku', 'market'
            interval: Tupla con fechas de inicio y fin
            batch_size: Tamaño del lote

        Returns:
            Lista consolidada de datos de ventas
        """
        all_sales_data = []

        # Procesar en lotes para evitar saturar la API
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]

            # Procesar lote en paralelo
            tasks = [
                self.get_sales_data(
                    asin=item['asin'],
                    sku=item['sku'],
                    market=[item['market']],
                    interval=interval
                )
                for item in batch
            ]

            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Consolidar resultados
            for sales_data in batch_results:
                if isinstance(sales_data, Exception):
                    print(f"⚠️ Error getting sales data: {sales_data}")
                elif sales_data:
                    all_sales_data.extend(sales_data)

            # Pausa entre lotes (Sales API es más restrictiva)
            if i + batch_size < len(items):
                await asyncio.sleep(2)

        return all_sales_data

    async def batch_get_orders(self, order_ids: List[str]) -> List[dict]:
        """
        Obtener múltiples órdenes en paralelo (respetando rate limits)
        """
        tasks = []
        for order_id in order_ids:
            task = self.get_order(order_id)
            tasks.append(task)

        # Ejecutar en lotes para no sobrecargar la API
        batch_size = 10
        all_orders = []

        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            batch_results = await asyncio.gather(*batch, return_exceptions=True)

            for result in batch_results:
                if isinstance(result, Exception):
                    print(f"Error in batch get orders: {result}")
                elif result is not None:
                    all_orders.append(result)

            # Pequeña pausa between batches
            if i + batch_size < len(tasks):
                await asyncio.sleep(1)

        return all_orders

    @rate_limited(APIEndpoint.ORDER)
    @async_retry(max_retries=3, backoff_base=2)
    async def get_order_status(self, order_id: str) -> dict:
        """Obtener solo el status de una orden específica"""
        result = await asyncio.to_thread(
            self.api_wrapper.get_order,
            order_id=order_id
        )

        order, success = result

        if success and order:
            print(f"Orden {order_id} recuperada con éxito")
            return order

        raise Exception(f"Error obteniendo orden {order_id}")

    async def health_check(self) -> Dict[str, Any]:
        """
        Verificar estado de la conexión con Amazon SP-API

        Returns:
            Diccionario con estado de salud de diferentes endpoints
        """
        health_status = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'healthy',
            'endpoints': {}
        }

        # Verificar cada endpoint con requests simples
        for endpoint in APIEndpoint:
            try:
                usage = self.rate_limiter.get_current_usage(endpoint)
                health_status['endpoints'][endpoint.value] = {
                    'status': 'healthy',
                    'rate_limit_usage': usage
                }
            except Exception as e:
                health_status['endpoints'][endpoint.value] = {
                    'status': 'unhealthy',
                    'error': str(e)
                }
                health_status['overall_status'] = 'degraded'

        return health_status
