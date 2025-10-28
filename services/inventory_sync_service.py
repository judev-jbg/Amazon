# services/inventory_sync_service.py
"""
Servicio para sincronización de inventario con Amazon
Actualiza stock (quantity) basado en productos de SQL Server
"""

from datetime import datetime
from typing import List, Dict
from infrastructure.base.async_service import AsyncService
from infrastructure.error_handling import EnhancedErrorHandler
from infrastructure.repositories.mssql_product_repository import MSSQLProductRepository
from core.api.amazon_listings_api_wrapper import AmazonListingsAPIWrapper
from infrastructure.metrics_collector import MetricsCollector
from config import setting as st


class InventoryUpdateMode:
    """Modos de actualización de inventario"""
    ACTIVATE = "activate"    # Quantity = 10
    DEACTIVATE = "deactivate"  # Quantity = 0
    SCHEDULED_FRIDAY = "scheduled_friday"  # Viernes 17h → 10
    SCHEDULED_MONDAY = "scheduled_monday"  # Lunes 5h → 0


class InventorySyncService(AsyncService):
    """
    Servicio para actualizar stock en Amazon basado en productos del ERP

    Ejecución:
    - Automática: Viernes 17h (Q=10), Lunes 5h (Q=0)
    - Manual: Activar (Q=10) / Desactivar (Q=0)
    """

    def __init__(self):
        super().__init__()
        self.mssql_repo = MSSQLProductRepository()
        self.listings_api = AmazonListingsAPIWrapper()
        self.error_handler = EnhancedErrorHandler()
        self.metrics = MetricsCollector()

        # Registrar dependencias
        self.register_dependency(self.mssql_repo)
        self.register_dependency(self.error_handler)

        self.logger.info("🚀 InventorySyncService inicializado")

    async def sync_inventory(self, mode: str) -> bool:
        """
        Sincronizar inventario según modo de ejecución

        Args:
            mode: activate, deactivate, scheduled_friday, scheduled_monday

        Returns:
            bool: True si exitoso
        """
        context = {
            'process_mode': f'inventory_sync_{mode}',
            'timestamp': datetime.now()
        }

        try:
            async with self.lifecycle():
                self.logger.info(
                    f"🔄 Iniciando sincronización de inventario: {mode}")

                # 1. Determinar cantidad según modo
                target_quantity = self._get_target_quantity(mode)

                # 2. Obtener productos activos del ERP
                products = await self.mssql_repo.get_active_products_for_inventory()

                if not products:
                    self.logger.info("No hay productos para sincronizar")
                    return True

                self.logger.info(
                    f"📦 {len(products)} productos para actualizar a Q={target_quantity}")

                # 3. Actualizar en Amazon (batch)
                results = await self._update_amazon_inventory(
                    products=products,
                    quantity=target_quantity
                )

                # 4. Procesar resultados
                success_count = results['success']
                failed_count = results['failed']

                self.logger.info(
                    f"✅ Actualizados: {success_count} | ❌ Fallidos: {failed_count}"
                )

                # 5. Notificar solo si hubo cambios
                if success_count > 0:
                    await self._send_success_notification(
                        mode=mode,
                        quantity=target_quantity,
                        success_count=success_count,
                        failed_count=failed_count,
                        failed_skus=results.get('failed_skus', [])
                    )

                # 6. Registrar métricas
                await self.metrics.record_process_complementary_success(
                    f'inventory_sync_{mode}',
                    success_count,
                    0,
                    failed_count
                )

                return failed_count == 0

        except Exception as e:
            await self.error_handler.handle_error(e, context)
            return False

    def _get_target_quantity(self, mode: str) -> int:
        """Determinar cantidad objetivo según modo"""
        quantity_map = {
            InventoryUpdateMode.ACTIVATE: 10,
            InventoryUpdateMode.DEACTIVATE: 0,
            InventoryUpdateMode.SCHEDULED_FRIDAY: 10,
            InventoryUpdateMode.SCHEDULED_MONDAY: 0
        }
        return quantity_map.get(mode, 0)

    async def _update_amazon_inventory(
        self,
        products: List[Dict],
        quantity: int
    ) -> Dict:
        """
        Actualizar inventario en Amazon (batch processing)

        Args:
            products: Lista de productos del ERP
            quantity: Cantidad objetivo

        Returns:
            Dict con resultados: {success: int, failed: int, failed_skus: []}
        """
        success_count = 0
        failed_count = 0
        failed_skus = []

        # Procesar en lotes de 10 para no saturar API
        batch_size = 10

        for i in range(0, len(products), batch_size):
            batch = products[i:i + batch_size]

            for product in batch:
                sku = product['IdArticulo']

                try:
                    # Actualizar en Amazon
                    result = await self.listings_api.update_quantity(
                        sku=sku,
                        quantity=quantity
                    )

                    if result['success']:
                        success_count += 1
                        self.logger.debug(f"✅ {sku}: Q={quantity}")
                    else:
                        failed_count += 1
                        failed_skus.append(sku)
                        self.logger.warning(f"❌ {sku}: {result.get('error')}")

                except Exception as e:
                    failed_count += 1
                    failed_skus.append(sku)
                    self.logger.error(f"❌ Error actualizando {sku}: {e}")

        return {
            'success': success_count,
            'failed': failed_count,
            'failed_skus': failed_skus
        }

    async def _send_success_notification(
        self,
        mode: str,
        quantity: int,
        success_count: int,
        failed_count: int,
        failed_skus: List[str]
    ):
        """Notificar éxito con detalles"""
        action = "Activación" if quantity == 10 else "Desactivación"

        subject = f"[ÉXITO] Sincronización Inventario Amazon: {action}"

        failed_section = ""
        if failed_count > 0:
            failed_section = f"""
            <div style="background: #fff3cd; padding: 15px; border-radius: 8px; margin-top: 15px;">
                <h3>⚠️ Productos Fallidos ({failed_count})</h3>
                <ul>
                    {''.join([f'<li>{sku}</li>' for sku in failed_skus[:20]])}
                    {f'<li>... y {len(failed_skus) - 20} más</li>' if len(failed_skus) > 20 else ''}
                </ul>
            </div>
            """

        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <div style="max-width: 600px; margin: 0 auto;">
                <h2 style="color: #4caf50;">✅ {action} de Stock Completada</h2>
                
                <div style="background: #f5f5f5; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3>📊 Resumen</h3>
                    <p><strong>Modo:</strong> {mode}</p>
                    <p><strong>Cantidad aplicada:</strong> {quantity}</p>
                    <p><strong>Productos actualizados:</strong> {success_count}</p>
                    <p><strong>Productos fallidos:</strong> {failed_count}</p>
                    <p><strong>Fecha:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                
                {failed_section}
                
                <p style="margin-top: 20px; color: #666;">
                    Powered by Amazon Inventory Sync | {datetime.now().isoformat()}
                </p>
            </div>
        </body>
        </html>
        """

        await self.error_handler.email_client.send_email(
            subject=subject,
            html_body=html_body,
            recipients=st.setting_email_recipients['success']
        )
