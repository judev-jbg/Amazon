import asyncio
from contextlib import asynccontextmanager
import logging
from core.database_manager import DatabaseManager
from core.amazon_api_client import AmazonAPIClient
from core.extraction_strategies import DailyFullExtraction, IncrementalExtraction, StatusUpdateExtraction, WeeklyCatchUpExtraction
from config import setting as st
from infrastructure.error_handling import EnhancedErrorHandler
from infrastructure.metrics_collector import MetricsCollector
from datetime import datetime
from typing import List
from models.extraction_config import ExtractionConfig, ExtractType
from infrastructure.base.async_service import AsyncService

"""
FUNCIONALIDAD:
- Orquesta todo el proceso de extracción
- Coordina estrategias, API client y base de datos
- Maneja el contexto de errores
- Registra métricas de rendimiento
- Procesa órdenes en lotes
"""


class OrderExtractionService(AsyncService):
    def __init__(self):
        super().__init__()
        self.db_manager = DatabaseManager()
        self.api_client = AmazonAPIClient()
        self.error_handler = EnhancedErrorHandler()
        self.metrics = MetricsCollector()
        # Registrar dependencias para AsyncService
        self.register_dependency(self.db_manager)
        self.register_dependency(self.error_handler)

        self.logger.info("🚀 OrderExtractionService inicializado")

    @asynccontextmanager
    async def error_context(self, process_mode: str, market_id: str = None):
        """Context manager para manejo de errores"""
        try:
            yield
        except Exception as e:
            context = {
                'process_mode': process_mode,
                'market_id': market_id,
                'timestamp': datetime.now()
            }
            await self.error_handler.handle_error(e, context)
            raise  # Re-raise para que el proceso principal pueda decidir qué hacer

    async def extract_orders(self, config: ExtractionConfig):
        """FLUJO PRINCIPAL"""
        context = {
            'process_mode': config.extract_type.value,
            'date_from': config.date_from.isoformat(),
            'date_to': config.date_to.isoformat(),
            'markets': config.markets
        }
        try:
            async with self.lifecycle():
                self.logger.info("Componentes inicializados correctamente")

                # 1. Iniciar métricas
                await self.metrics.record_process_start(config)
                self.logger.info("Metricas inicializadas correctamente")

                # 2. Obtener estrategia según tipo de extracción
                strategy = self._get_extraction_strategy(config.extract_type)
                self.logger.info(
                    f"Estrategia obtenida correctamente: {strategy.__class__.__name__}")

                # 3. Extraer órdenes usando la estrategia
                orders = await strategy.extract(config)
                if not orders:
                    self.logger.info("No se encuentran ordenes para procesar")
                    await self.metrics.record_process_success(config, len(orders))
                    await self._ensure_finished()
                    self.logger.info("Componentes finalizados correctamente")
                    return True

                # 4. Procesar en lotes
                if config.extract_type == ExtractType.STATUS_UPDATE:
                    await self._process_status_updates(orders, config)
                else:
                    await self._process_orders_batch(orders, config)

                # 5. Registrar éxito
                await self.metrics.record_process_success(config, len(orders))

                # Success notification (solo para procesos importantes)
                if config.extract_type == ExtractType.DAILY_FULL:
                    await self._send_success_notification(config, len(orders))

                await self._ensure_finished()
                self.logger.info("Componentes finalizados correctamente")

                return True

        except Exception as e:

            # error_handler para manejo completo
            await self.error_handler.handle_error(e, context)

            # Registrar error en métricas
            await self.metrics.record_process_error(config, e)

            # Decidir si reintentar
            if self._should_retry(e):
                await asyncio.sleep(30)
                return await self.extract_orders(config)
            else:
                raise

    def _should_retry(self, error: Exception) -> bool:
        """Determinar si se debe reintentar"""
        retry_errors = ['ConnectionError', 'TimeoutError', '429']
        error_str = str(error)
        return any(retry_error in error_str for retry_error in retry_errors)

    def _get_extraction_strategy(self, extract_type: ExtractType):
        strategies = {
            ExtractType.DAILY_FULL: DailyFullExtraction(self.api_client),
            ExtractType.INCREMENTAL: IncrementalExtraction(self.api_client, self.db_manager),
            ExtractType.STATUS_UPDATE: StatusUpdateExtraction(self.api_client, self.db_manager),
            ExtractType.WEEKLY_CATCH_UP: WeeklyCatchUpExtraction(self.api_client, self.db_manager),
        }
        return strategies[extract_type]

    async def _process_orders_batch(self, orders: List[dict], config: ExtractionConfig):
        """Procesa órdenes, items y ventas en lotes"""

        batch_size = config.batch_size
        for i in range(0, len(orders), batch_size):
            batch = orders[i:i + config.batch_size]

            print(
                f"Procesamiento por lotes {i//batch_size + 1} : {len(batch)} ordenes")

            # 1. UPSERT órdenes
            await self.db_manager.orders.upsert_orders(batch)

            if config.extract_type == ExtractType.STATUS_UPDATE:
                return

            # 2. Procesar items de cada orden
            all_order_items = []
            all_sales = []

            for order in batch:
                order_id = order.get('amazonOrderId')
                if not order_id:
                    continue

                try:
                    # Obtener items de la orden
                    order_items = await self.api_client.get_order_items(
                        order_id
                    )

                    if order_items:
                        all_order_items.extend(order_items)

                        # Obtener ventas para cada item único (ASIN/SKU)
                        unique_items = self._get_unique_items(order_items)

                        for item in unique_items:
                            sales_data = await self.api_client.get_sales_data(
                                asin=item['asin'],
                                sku=item['sku'],
                                market=[order.get('marketplaceId')],
                                interval=(config.date_from.isoformat(
                                ) + "-00:00", config.date_to.isoformat() + "-00:00")
                            )

                            if sales_data:
                                all_sales.extend(sales_data)

                except Exception as e:
                    print(f"Error procesando orden {order_id}: {e}")
                    # Continuar con las siguientes órdenes
                    continue

            # 3. UPSERT items y ventas si hay datos
            if all_order_items:
                await self.db_manager.order_items.upsert_order_items(all_order_items)

            # if all_sales:
            #     await self.db_manager.sales.upsert_sales(all_sales)

            print(
                f"Procesamiento por lotes completados: \n{len(batch)} ordenes. \n{len(all_order_items)} elementos de ordenes. \n{len(all_sales)} estadisticas de ventas.")

    def _get_unique_items(self, order_items: List[dict]) -> List[dict]:
        """Obtener items únicos por ASIN/SKU para evitar duplicar llamadas de ventas"""
        seen = set()
        unique_items = []

        for item in order_items:
            key = (item.get('asin'), item.get('sku'))
            if key not in seen:
                seen.add(key)
                unique_items.append(item)

        return unique_items

    async def _process_status_updates(self, orders: List[dict], config: ExtractionConfig):
        """Procesar actualizaciones de estado solamente"""
        if not orders:
            return

        # Actualizar solo status, no hacer upsert completo
        await self.db_manager.orders.update_order_status_only(orders)

    async def _send_success_notification(self, config: ExtractionConfig, order_count: int):
        """Enviar notificación de éxito (solo para procesos importantes)"""
        if order_count > 0:
            subject = f"[EXITO] Amazon Management: [Orders] Modo → {config.extract_type.value}"
            html_body = f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
                <div style="max-width: 600px; margin: 0 auto;">
                    <h2 style="color: #4caf50;">✔️ Proceso [Orders] en modo {config.extract_type.value} completado correctamente</h2>
                    <p><strong>Tipo:</strong> {config.extract_type.value}</p>
                    <p><strong>Órdenes procesadas:</strong> {order_count}</p>
                    <p><strong>Fecha:</strong> {datetime.now()}</p>
                    <p><strong>Mercados:</strong> {', '.join(config.markets)}</p>
                </div>
            </body>
            </html>
            """

            await self.error_handler.email_client.send_email(
                subject=subject,
                html_body=html_body,
                recipients=st.setting_email_recipients['success']
            )
