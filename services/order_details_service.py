
import logging
import pandas as pd
from config import setting as st
from pathlib import Path
from typing import List
from datetime import datetime
from infrastructure.data_validator import DataValidator
from infrastructure.error_handling import EnhancedErrorHandler
from infrastructure.metrics_collector import MetricsCollector
from core.database_manager import DatabaseManager
from services.file_processor import FileProcessor
from infrastructure.base.async_service import AsyncService


class OrderDetailsService(AsyncService):
    """
    Servicio para procesar detalles de órdenes desde archivos Excel

    """

    def __init__(self):
        super().__init__()
        self.db_manager = DatabaseManager()
        self.validator = DataValidator(self.db_manager)
        self.file_processor = FileProcessor()
        self.error_handler = EnhancedErrorHandler()
        self.metrics = MetricsCollector()
        # Registrar dependencias
        self.register_dependency(self.db_manager)
        self.register_dependency(self.error_handler)
        self.logger = logging.getLogger("AmazonManagement")
        self.logger.info("🚀 OrderDetailsService inicializado")

    async def process_order_details(self) -> bool:
        """
        Proceso principal para OrderDetails

        Returns:
            bool: True si el proceso fue exitoso
        """
        start_time = datetime.now()
        process_context = {
            'process_mode': 'order_details',
            'start_time': start_time
        }

        try:
            async with self.lifecycle():
                self.logger.info("Componentes inicializados correctamente")

                # 1. Registrar inicio del proceso
                await self.metrics.record_process_complementary_start('order_details')

                # 2. Obtener archivo de datos
                file_path = await self._get_order_details_file()
                if not file_path:
                    self.logger.info(
                        "No se encontró archivo de OrderDetails para procesar")
                    await self._ensure_finished()
                    self.logger.info("Componentes finalizados correctamente")
                    return True

                process_context['file_path'] = str(file_path)

                # 3. Leer y procesar archivo
                df_raw = await self.file_processor.read_excel_file(file_path)
                if df_raw.empty:
                    raise ValueError(
                        f"El archivo {file_path} está vacío o no se pudo leer")

                # 4. Transformar columnas según configuración
                df_transformed = self._transform_columns(df_raw)

                # 5. Validación robusta
                df_to_insert, df_to_update, validation_errors = await self.validator.validate_order_details(df_transformed)

                # 6. Log errores de validación
                if validation_errors:
                    for error in validation_errors:
                        self.logger.warning(error)
                        # await self.error_handler.handle_warning(error, process_context)

                # 7. Procesar datos
                insert_count = await self._process_inserts(df_to_insert) if not df_to_insert.empty else 0
                update_count = await self._process_updates(df_to_update) if not df_to_update.empty else 0

                # 8. Actualizar referencias (como en el código original)
                if insert_count > 0:
                    await self.db_manager.order_details.update_asin_references()

                # 9. Actualizar datos de Prestashop
                # await self._update_prestashop_references()

                # 10. Registrar métricas de éxito
                await self.metrics.record_process_complementary_success(
                    'order_details',
                    insert_count,
                    update_count,
                    len(validation_errors)
                )

                # 11. Notificación de éxito
                self.logger.info(
                    f"\n✔️ OrderDetails procesado"
                    f"\nInsertados: {insert_count}"
                    f"\nActualizados: {update_count}"
                    f"\nErrores: {len(validation_errors)}"
                )
                await self._send_success_notification(insert_count, update_count, validation_errors)

                await self._ensure_finished()
                self.logger.info("Componentes finalizados correctamente")

                return True

        except Exception as e:
            await self.error_handler.handle_error(e, process_context)
            return False

    async def _get_order_details_file(self) -> Path:
        """Obtener archivo de OrderDetails más reciente"""
        import config.setting as st

        file_path = Path(st.workbookPathOrdersDetail["ordersDetail"])

        if file_path.exists() and file_path.suffix in ['.xlsx', '.xls']:
            return file_path

        return None

    def _transform_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformar nombres de columnas según configuración"""
        import config.setting as st

        # Usar la configuración existente
        if hasattr(st, 'columnsOrdersDetails'):
            df_transformed = df.rename(columns=st.columnsOrdersDetails)

            # Mantener solo las columnas que necesitamos
            available_columns = [
                col for col in st.columnsOrdersDetails.values() if col in df_transformed.columns]
            df_transformed = df_transformed[available_columns]

            return df_transformed

        return df

    async def _process_inserts(self, df_to_insert: pd.DataFrame) -> int:
        """Procesar registros para insertar"""
        if df_to_insert.empty:
            self.logger.info(
                "El dataframe esta vacio, no se continua con la insercion en la BD")
            return 0

        # Añadir timestamps
        df_to_insert['loadDate'] = datetime.now().date()
        df_to_insert['loadDateTime'] = datetime.now()

        self.logger.info(
            f"El dataframe tiene {len(df_to_insert)} registros para insertar en la BD")
        await self.db_manager.order_details.insert_order_details(df_to_insert)
        self.logger.info("Proceso de insercion finalizado con exito")
        return len(df_to_insert)

    async def _process_updates(self, df_to_update: pd.DataFrame) -> int:
        """Procesar registros para actualizar"""
        if df_to_update.empty:
            return 0

        # Añadir timestamp de actualización
        df_to_update['lastDateTimeUpdated'] = datetime.now()

        await self.db_manager.order_details.update_order_details(df_to_update)
        return len(df_to_update)

    # TODO: METODO PARA ELIMINAR
    async def _update_prestashop_references(self):
        """Actualizar referencias de Prestashop (como en el código original)"""
        orders_without_ps_ref = await self.db_manager.order_details.get_orders_without_ps_reference()

        if not orders_without_ps_ref.empty:
            await self.db_manager.order_details.update_prestashop_order_references(orders_without_ps_ref)

    async def _send_success_notification(self, insert_count: int, update_count: int, validation_errors: List[str]):
        """Enviar notificación de éxito"""
        if insert_count > 0 or update_count > 0:
            subject = f"[EXITO] Amazon Management: Order Details"
            html_body = f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
                <div style="max-width: 600px; margin: 0 auto;">
                    <h2 style="color: #4caf50;">✔️ Order Details ha finalizado correctamente</h2>
                    <p><strong>Registros insertados:</strong> {insert_count}</p>
                    <p><strong>Registros actualizados:</strong> {update_count}</p>
                    <p><strong>Errores de validación:</strong> {len(validation_errors)}</p>
                    <p><strong>Fecha:</strong> {datetime.now()}</p>
                    
                    {f"<h3>⚠️ Errores de Validación:</h3><ul>{''.join([f'<li>{error}</li>' for error in validation_errors[:10]])}</ul>" if validation_errors else ""}
                </div>
            </body>
            </html>
            """

            await self.error_handler.email_client.send_email(
                subject=subject,
                html_body=html_body,
                # Configurar según tus necesidades
                recipients=st.setting_email_recipients['success']
            )
