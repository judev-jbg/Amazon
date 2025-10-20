import asyncio
import datetime
import aiohttp
from typing import List, Dict, Tuple, Any
from infrastructure.rate_limiter import RateLimiter, APIEndpoint, rate_limited
from libs.transform import getOrder, getOrders, getOrderItems, getSales

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
    async def _get_orders_for_market(self, market: str, date_from: datetime, 
                                   date_to: datetime, max_retries: int = 3) -> List[dict]:
        """
        Obtener ordenes para un mercado especifico
        
        Args:
            market: Lista de mercados
            date_from: Fecha de inicio
            date_to: Fecha de fin
            max_retries: Número máximo de reintentos
            
        Returns:
            Lista de elementos de la orden
        """
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            print("#" * 5, "=" * 70)
            print("#" * 5, f" GET ORDERS: Intento {retry_count + 1} de {max_retries}")
           
            try:
                # Usar tu función existente pero con async
                result = await asyncio.to_thread(
                    getOrders,
                    dateInit=date_from.isoformat(),
                    dateEnd=date_to.isoformat(),
                    market=[market]
                )
                
                if result[1] == 1 and not result[0].empty:  # Success
                    return result[0].to_dict('records')

                if result[1] == 1 and result[0].empty:  # Success pero sin datos
                    return []
            
                if result[1] == 0 and result[0].empty: # Error
                    retry_count += 1
                    if retry_count >= max_retries:
                        print("#" * 5, f" Error: Numero maximo de intentos ({max_retries}) alcanzados")
                        print("#" * 5, "=" * 70)
                        return []
    

                    print("#" * 5, f" -Error: Solicitud fallida, esperando {2 ** retry_count} segundos...")
                    await asyncio.sleep(2 ** retry_count)  # Exponential backoff

                
                if result[1] == 0 and not result[0].empty and result[0]['code'].iloc[0]:  # Rate limit alcanzado
                    retry_count += 1
                    if retry_count >= max_retries:
                        print("#" * 5, f" RateLimit: Numero maximo de intentos ({max_retries}) alcanzados")
                        print("#" * 5, "=" * 70)
                        return []
    

                    print("#" * 5, f" -RateLimit: Solicitud fallida, esperando {2 ** retry_count} segundos...")
                    await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                    
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print("#" * 5, f" Exception: Numero maximo de intentos ({max_retries}) alcanzados")
                    print("#" * 5, f" Error: {e}")
                    print("#" * 5, "=" * 70)
                    raise
                
                print("#" * 5, f" Exception: Solicitud fallida, esperando {2 ** retry_count} segundos...")
                await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                
        return []
    
    @rate_limited(APIEndpoint.ORDER_ITEMS)
    async def get_order_items(self, order_id: str, max_retries: int = 3)-> List[Dict]:
        """
        Obtener elementos de una orden específica
        
        Args:
            order_id: ID de la orden de Amazon
            max_retries: Número máximo de reintentos
            
        Returns:
            Lista de elementos de la orden
        """
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Usar la función existente pero de forma asíncrona
                result = await asyncio.to_thread(
                    getOrderItems,
                    orderId=order_id,
                    tagSubjectMail="AmazonAPIClient.get_order_items"
                )
                
                if result[1] == 1:  # Success
                    order_items = result[0].to_dict('records') if not result[0].empty else []
                    
                    print(f"Elementos de la orden {order_id} recuperados con exito: {len(order_items)} elemento(s)")
                    return order_items
                else:
                    # Error en la función legacy
                    raise Exception(f"Error en la funcion para el pedido {order_id}")
                    
            except Exception as e:
                retry_count += 1
                error_msg = str(e)
                
                # Manejar rate limiting específico
                if "429" in error_msg or "throttl" in error_msg.lower():
                    await self.rate_limiter.handle_rate_limit_error(APIEndpoint.ORDER_ITEMS)
                    continue
                
                if retry_count >= max_retries:
                    print(f"❌ Failed to get order items for {order_id} after {max_retries} retries: {e}")
                    raise
                
                # Backoff exponencial para otros errores
                await asyncio.sleep(2 ** retry_count)
        
        return []
    
    @rate_limited(APIEndpoint.ORDER)
    async def get_order(self, order_id: str, max_retries: int = 3)-> List[Dict]:
        """
        Obtener una orden específica
        
        Args:
            order_id: ID de la orden de Amazon
            max_retries: Número máximo de reintentos
            
        Returns:
            Orden en formato dict
        """
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Usar la función existente pero de forma asíncrona
                result = await asyncio.to_thread(
                    getOrder,
                    orderId=order_id,
                    tagSubjectMail="AmazonAPIClient.get_order"
                )
                
                if result[1] == 1:  # Success
                    order = result[0].to_dict('records')[0] if not result[0].empty else None
                    
                    print(f"Orden {order_id} recuperada con exito.")
                    return order
                else:
                    # Error en la función legacy
                    raise Exception(f"Error en la funcion get_order para el pedido {order_id}")
                    
            except Exception as e:
                retry_count += 1
                error_msg = str(e)
                
                # Manejar rate limiting específico
                if "429" in error_msg or "throttl" in error_msg.lower():
                    await self.rate_limiter.handle_rate_limit_error(APIEndpoint.ORDER_ITEMS)
                    continue
                
                if retry_count >= max_retries:
                    print(f"❌ Failed to get order items for {order_id} after {max_retries} retries: {e}")
                    raise
                
                # Backoff exponencial para otros errores
                await asyncio.sleep(2 ** retry_count)
        
        return None

    @rate_limited(APIEndpoint.SALES)
    async def get_sales_data(self, asin: str, sku: str, market: List[str], 
                           interval: Tuple[str, str], max_retries: int = 3)-> List[Dict]:
        """
        Obtener datos de ventas para un ASIN/SKU específico
        
        Args:
            asin: ASIN del producto
            sku: SKU del producto
            market: Lista de mercados
            interval: Tupla con fechas de inicio y fin
            max_retries: Número máximo de reintentos
            
        Returns:
            Lista de métricas de ventas
        """
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Usar la función existente pero de forma asíncrona
                result = await asyncio.to_thread(
                    getSales,
                    asinp=asin,
                    skup=sku,
                    market=market,
                    intervalp=interval,
                    tagSubjectMail="AmazonAPIClient.get_sales_data"
                )
                
                if result[1] == 1:  # Success
                    sales_data = result[0].to_dict('records') if not result[0].empty else []
                    
                    print(f"Metricas de venta para {asin}/{sku} recuperadas con exito: {len(sales_data)} metrica(s)")
                    return sales_data
                else:
                    # Error en la función legacy
                    raise Exception(f"Legacy function returned error for {asin}/{sku}")
                    
            except Exception as e:
                retry_count += 1
                error_msg = str(e)
                
                # Manejar rate limiting específico
                if "429" in error_msg or "throttl" in error_msg.lower():
                    await self.rate_limiter.handle_rate_limit_error(APIEndpoint.SALES)
                    continue
                
                if retry_count >= max_retries:
                    print(f"❌ Failed to get sales data for {asin}/{sku} after {max_retries} retries: {e}")
                    raise
                
                # Backoff exponencial para otros errores
                await asyncio.sleep(2 ** retry_count)
        
        return []

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
                    print(f"⚠️ Error obteniendo elemento de la orden {order_id}: {items}")
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
    
    async def get_order_status(self, order_id: str) -> dict:
        """Obtener solo el status de una orden específica"""
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                async with self.rate_limiter:
                    # Usar función existente pero filtrar respuesta
                    result = await asyncio.to_thread(
                        getOrder,
                        orderId=order_id,
                        tagSubjectMail=self.__class__.__name__
                    )
                    
                    if result[1] == 1 and not result[0].empty:  # Success
                        order_data = result[0].iloc[0].to_dict()
                        
                        return {
                            'amazonOrderId': order_data.get('amazonOrderId_o', order_id),
                            'orderStatus': order_data.get('orderStatus_o'),
                            'lastUpdateDate': order_data.get('lastUpdateDate_o')
                        }
                        
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    raise
                    
                await asyncio.sleep(2 ** retry_count)
        
        return {}
    
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