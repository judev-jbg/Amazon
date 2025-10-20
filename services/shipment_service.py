# services/shipment_service.py
import logging
import pandas as pd
from pathlib import Path
from typing import List
from datetime import datetime
import shutil
from config import setting as st
from infrastructure.error_handling import EnhancedErrorHandler
from infrastructure.metrics_collector import MetricsCollector
from core.database_manager import DatabaseManager
from services.file_processor import FileProcessor
from infrastructure.base.async_service import AsyncService


class ShipmentService(AsyncService):
    """
    Servicio para procesar actualizaciones de envíos

    """

    def __init__(self):
        super().__init__()
        self.db_manager = DatabaseManager()
        self.file_processor = FileProcessor()
        self.error_handler = EnhancedErrorHandler()
        self.metrics = MetricsCollector()
        # Registrar dependencias para AsyncService
        self.register_dependency(self.db_manager)
        self.register_dependency(self.error_handler)

        self.logger.info("🚀 ShipmentService inicializado")

    async def process_shipment_updates(self) -> bool:
        """
        Proceso principal para actualizaciones de envío

        Returns:
            bool: True si el proceso fue exitoso
        """
        start_time = datetime.now()
        process_context = {
            'process_mode': 'shipment_update',
            'start_time': start_time
        }

        try:
            async with self.lifecycle():
                self.logger.info("Componentes inicializados correctamente")

                # 1. Registrar inicio
                await self.metrics.record_process_complementary_start('shipment_update')

                # 2. Buscar archivos de envío
                shipment_files = await self._get_shipment_files()

                if not shipment_files:
                    self.logger.info(
                        "No se encontraron archivos de envío para procesar")
                    await self._ensure_finished()
                    self.logger.info("Componentes finalizados correctamente")
                    return True

                total_processed = 0

                # 3. Procesar cada archivo
                for file_path in shipment_files:
                    try:
                        process_context['current_file'] = str(file_path)

                        # Leer archivo
                        df_shipments = await self.file_processor.read_shipment_file(file_path)

                        if df_shipments.empty:
                            await self.error_handler.handle_warning(f"Archivo vacío: {file_path}", process_context)
                            continue

                        # Validar datos
                        df_validated = self._validate_shipment_data(
                            df_shipments)

                        # Actualizar bases de datos
                        success = await self._update_shipment_databases(df_validated, process_context)

                        if success:
                            # Mover archivo procesado
                            await self._move_processed_file(file_path)
                            total_processed += len(df_validated)

                    except Exception as file_error:
                        await self.error_handler.handle_error(file_error, {
                            **process_context,
                            'file_path': str(file_path)
                        })
                        continue

                # 4. Registrar métricas finales
                await self.metrics.record_process_complementary_success(
                    'shipment_update',
                    0,  # No hay inserts separados
                    total_processed,
                    0   # No hay errores de validación aquí
                )

                # 5. Notificación de éxito
                if total_processed > 0:
                    await self._send_success_notification(total_processed, len(shipment_files))

                await self._ensure_finished()
                self.logger.info("Componentes finalizados correctamente")

                return True

        except Exception as e:
            await self.error_handler.handle_error(e, process_context)
            return False

    async def _get_shipment_files(self) -> List[Path]:
        """Obtener archivos de envío para procesar"""
        import config.setting as st

        shipment_dir = Path(st.workbookPathShipmentGLS)

        if not shipment_dir.exists():
            return []

        # Buscar archivos Excel
        shipment_files = list(shipment_dir.glob("*.xlsx"))
        return shipment_files

    def _validate_shipment_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validar datos de envío"""
        # Validar columnas requeridas
        required_columns = ["codbar", "Expedicion", "Referencia", "DptoDst"]

        missing_columns = [
            col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(
                f"Columnas faltantes en archivo de envío: {missing_columns}")

        # Limpiar datos
        df_clean = df.copy()

        # Remover filas vacías
        df_clean = df_clean.dropna(subset=required_columns, how='all')

        # Limpiar strings
        for col in required_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str).str.strip()

        return df_clean

    async def _update_shipment_databases(self, df_shipments: pd.DataFrame, context: dict) -> bool:
        """Actualizar las 3 bases de datos con información de envío"""
        try:
            # 1. Actualizar ordersdetail
            update_success_details = await self.db_manager.shipments.update_shipment_order_details(df_shipments)

            # 2. Actualizar orders
            update_success_orders = await self.db_manager.shipments.update_shipment_orders(df_shipments)

            # 3. Actualizar Prestashop ps_order_carrier
            update_success_prestashop = await self.db_manager.shipments.update_shipment_prestashop(df_shipments)

            # Verificar que todas las actualizaciones fueron exitosas
            all_success = update_success_details and update_success_orders and update_success_prestashop

            if not all_success:
                error_msg = f"Error actualizando bases de datos - Details: {update_success_details}, Orders: {update_success_orders}, Prestashop: {update_success_prestashop}"
                await self.error_handler.handle_error(Exception(error_msg), context)

            return all_success

        except Exception as e:
            await self.error_handler.handle_error(e, context)
            return False

    async def _move_processed_file(self, file_path: Path):
        """Mover archivo procesado a directorio de éxito"""
        import config.setting as st

        try:
            success_dir = Path(
                st.setting_load["ordersDetail"]["dir_to_move_file_success"])
            success_dir.mkdir(exist_ok=True)

            # Generar nombre único
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_name = f"processed_{timestamp}_{file_path.name}"
            destination = success_dir / new_name

            # Mover archivo
            shutil.move(str(file_path), destination)

        except Exception as e:
            await self.error_handler.handle_warning(f"Error moviendo archivo procesado: {e}")

    async def _send_success_notification(self, records_processed: int, files_processed: int):
        """Enviar notificación de éxito"""
        subject = f"[EXITO] Amazon Management: Shipment Updates"
        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <div style="max-width: 600px; margin: 0 auto;">
                <h2 style="color: #4caf50;">✔️ Actualizaciones de Envío (expediciones) completadas correctamente</h2>
                <p><strong>Archivos procesados:</strong> {files_processed}</p>
                <p><strong>Registros actualizados:</strong> {records_processed}</p>
                <p><strong>Fecha:</strong> {datetime.now()}</p>
                
                <h3>📋 Tablas Actualizadas:</h3>
                <ul>
                    <li>ordersdetail - Tracking y códigos de barras</li>
                    <li>orders - Números de seguimiento</li>
                    <li>ps_order_carrier - Tracking y códigos de barras</li>
                </ul>
            </div>
        </body>
        </html>
        """

        await self.error_handler.email_client.send_email(
            subject=subject,
            html_body=html_body,
            recipients=st.setting_email_recipients['success']
        )
