import asyncio
from contextlib import asynccontextmanager
from core.database_manager import DatabaseManager
from core.amazon_api_client import AmazonAPIClient
from core.extraction_strategies import DailyFullExtraction,IncrementalExtraction,StatusUpdateExtraction, WeeklyCatchUpExtraction
from config import setting as st
from infrastructure.error_handling import EnhancedErrorHandler
from infrastructure.metrics_collector import MetricsCollector
from datetime import datetime
from typing import List
from models.extraction_config import ExtractionConfig, ExtractType

"""
FUNCIONALIDAD:
- Orquesta todo el proceso de extracción
- Coordina estrategias, API client y base de datos
- Maneja el contexto de errores
- Registra métricas de rendimiento
- Procesa órdenes en lotes
"""

class OrderExtractionService:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.api_client = AmazonAPIClient()
        self.error_handler = EnhancedErrorHandler()
        self.metrics = MetricsCollector()
        
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
        async with self.error_context(config.extract_type.value):
            try:
                # 1. Iniciar métricas
                await self.metrics.record_process_start(config)

                # 2. Obtener estrategia según tipo de extracción   
                strategy = self._get_extraction_strategy(config.extract_type)

                # 3. Extraer órdenes usando la estrategia
                orders = await strategy.extract(config)

                # # 4. Procesar en lotes
                # await self._process_orders_batch(orders, config)

                # # 5. Registrar éxito
                # await self.metrics.record_process_success(config, len(orders))
                
                # # Success notification (solo para procesos importantes)
                # if config.extract_type == ExtractType.DAILY_FULL:
                #     await self._send_success_notification(config, len(orders))
                
            except Exception as e:
                # Error metrics
                print(e)
                await self.metrics.record_error(e)
                
                # El error ya fue manejado por el context manager
                # Aquí podemos decidir si reintentar o fallar
                if self._should_retry(e):
                    await asyncio.sleep(30)  # Wait before retry
                    return await self.extract_orders(config)  # Retry once
                else:
                    raise
    
    def _get_extraction_strategy(self, extract_type: ExtractType):
        strategies = {
            ExtractType.DAILY_FULL: DailyFullExtraction(self.api_client),
            ExtractType.INCREMENTAL: IncrementalExtraction(self.api_client),
            ExtractType.STATUS_UPDATE: StatusUpdateExtraction(self.api_client),
            ExtractType.WEEKLY_CATCH_UP: WeeklyCatchUpExtraction(self.api_client)
        }
        return strategies[extract_type]
    
    async def _process_orders_batch(self, orders: List[dict], config: ExtractionConfig):
        """Procesa órdenes en lotes para eficiencia"""
        for i in range(0, len(orders), config.batch_size):
            batch = orders[i:i + config.batch_size]
            
            # Usar UPSERT en lugar de DELETE + INSERT
            await self.db_manager.upsert_orders(batch)
            
            # Procesar items y sales en paralelo
            await asyncio.gather(
                self._process_order_items(batch),
                self._process_sales_data(batch)
            )
    
    async def _send_success_notification(self, config: ExtractionConfig, order_count: int):
        """Enviar notificación de éxito (solo para procesos importantes)"""
        if order_count > 0:
            subject = f"✅ Amazon Orders - {config.extract_type.value} completed successfully"
            html_body = f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
                <div style="max-width: 600px; margin: 0 auto;">
                    <h2 style="color: #4caf50;">✅ Proceso Completado Exitosamente</h2>
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