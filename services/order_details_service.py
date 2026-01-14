
import logging
import pandas as pd
from config import setting as st
from pathlib import Path
from typing import List, Tuple
from datetime import datetime
import shutil
from infrastructure.data_validator import DataValidator
from infrastructure.error_handling import EnhancedErrorHandler
from infrastructure.metrics_collector import MetricsCollector
from core.database_manager import DatabaseManager
from services.file_processor import FileProcessor
from infrastructure.base.async_service import AsyncService


class OrderDetailsService(AsyncService):
    """
    Servicio para procesar detalles de órdenes desde archivos .txt mensuales
    Procesa múltiples archivos TSV incrementales generados por RPA
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
        Proceso principal para OrderDetails - procesa múltiples archivos .txt del mes actual

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

                # 2. Obtener archivos .txt del mes actual
                txt_files = self._get_monthly_txt_files()
                if not txt_files:
                    self.logger.info(
                        "No se encontraron archivos .txt para procesar en el mes actual")
                    await self._ensure_finished()
                    self.logger.info("Componentes finalizados correctamente")
                    return True

                self.logger.info(f"Se encontraron {len(txt_files)} archivos .txt para procesar")

                # Contadores totales
                total_insert_count = 0
                total_update_count = 0
                total_validation_errors = []
                files_processed = []
                files_failed = []

                # 3. Procesar cada archivo
                for file_path in txt_files:
                    try:
                        self.logger.info(f"Procesando archivo: {file_path.name}")
                        process_context['current_file'] = str(file_path)

                        # Leer archivo .txt (TSV)
                        df_raw = pd.read_csv(file_path, sep='\t', encoding='utf-8')
                        if df_raw.empty:
                            self.logger.warning(
                                f"El archivo {file_path.name} está vacío")
                            continue

                        # Transformar columnas según configuración
                        df_transformed = self._transform_columns(df_raw)

                        # Validación robusta
                        df_to_insert, df_to_update, validation_errors = await self.validator.validate_order_details(df_transformed)

                        # Log errores de validación
                        if validation_errors:
                            total_validation_errors.extend(validation_errors)
                            for error in validation_errors[:5]:  # Solo primeros 5 por archivo
                                self.logger.warning(f"[{file_path.name}] {error}")

                        # Procesar datos
                        insert_count = await self._process_inserts(df_to_insert) if not df_to_insert.empty else 0
                        update_count = await self._process_updates(df_to_update) if not df_to_update.empty else 0

                        total_insert_count += insert_count
                        total_update_count += update_count

                        self.logger.info(
                            f"Archivo {file_path.name}: {insert_count} insertados, {update_count} actualizados")

                        # Mover archivo a carpeta procesados
                        self._move_to_processed(file_path)
                        files_processed.append(file_path.name)

                    except Exception as e:
                        self.logger.error(
                            f"Error procesando archivo {file_path.name}: {str(e)}")
                        files_failed.append(file_path.name)
                        # Continuar con el siguiente archivo
                        continue

                # 4. Actualizar referencias ASIN si hubo inserciones
                if total_insert_count > 0:
                    await self.db_manager.order_details.update_asin_references()

                # 5. Registrar métricas de éxito
                await self.metrics.record_process_complementary_success(
                    'order_details',
                    total_insert_count,
                    total_update_count,
                    len(total_validation_errors)
                )

                # 6. Notificación de éxito/error
                self.logger.info(
                    f"\n✔️ OrderDetails procesado"
                    f"\nArchivos procesados: {len(files_processed)}"
                    f"\nArchivos fallidos: {len(files_failed)}"
                    f"\nTotal insertados: {total_insert_count}"
                    f"\nTotal actualizados: {total_update_count}"
                    f"\nTotal errores validación: {len(total_validation_errors)}"
                )
                await self._send_success_notification(
                    total_insert_count,
                    total_update_count,
                    total_validation_errors,
                    files_processed,
                    files_failed
                )

                await self._ensure_finished()
                self.logger.info("Componentes finalizados correctamente")

                return True

        except Exception as e:
            await self.error_handler.handle_error(e, process_context)
            return False

    def _get_monthly_txt_files(self) -> List[Path]:
        """
        Obtener archivos .txt del mes actual
        Ruta: C:\\...\\PEDIDOS\\YYYY\\MM\\*.txt
        """
        now = datetime.now()
        year = str(now.year)
        month = str(now.month).zfill(2)

        # Construir ruta base
        base_path = Path(st.ROOT_DIR) / "source" / "PEDIDOS" / year / month

        if not base_path.exists():
            self.logger.warning(f"La carpeta {base_path} no existe")
            return []

        # Buscar archivos .txt
        txt_files = sorted(base_path.glob("*.txt"))

        self.logger.info(f"Buscando archivos en: {base_path}")
        self.logger.info(f"Archivos .txt encontrados: {len(txt_files)}")

        return txt_files

    def _move_to_processed(self, file_path: Path):
        """
        Mover archivo procesado a carpeta procesados/YYYY/MM
        """
        now = datetime.now()
        year = str(now.year)
        month = str(now.month).zfill(2)

        # Construir carpeta destino
        processed_folder = Path(st.ROOT_DIR) / "source" / "PEDIDOS" / "procesados" / year / month

        # Crear carpeta si no existe
        processed_folder.mkdir(parents=True, exist_ok=True)

        # Mover archivo
        destination = processed_folder / file_path.name
        shutil.move(str(file_path), str(destination))

        self.logger.info(f"Archivo movido a: {destination}")

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

    async def _send_success_notification(
        self,
        insert_count: int,
        update_count: int,
        validation_errors: List[str],
        files_processed: List[str],
        files_failed: List[str]
    ):
        """Enviar notificación de éxito/error con detalle de archivos procesados"""
        if insert_count > 0 or update_count > 0 or files_processed:
            # Determinar tipo de notificación
            if files_failed:
                subject = f"[ADVERTENCIA] Amazon Management: Order Details - Algunos archivos fallaron"
                color = "#ff9800"
                icon = "⚠️"
            else:
                subject = f"[EXITO] Amazon Management: Order Details"
                color = "#4caf50"
                icon = "✔️"

            # Construir lista de archivos procesados
            files_processed_html = "<ul>" + \
                "".join([f"<li>{file}</li>" for file in files_processed]) + "</ul>" \
                if files_processed else "<p>Ninguno</p>"

            # Construir lista de archivos fallidos
            files_failed_html = "<ul>" + \
                "".join([f"<li>{file}</li>" for file in files_failed]) + "</ul>" \
                if files_failed else "<p>Ninguno</p>"

            html_body = f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
                <div style="max-width: 600px; margin: 0 auto;">
                    <h2 style="color: {color};">{icon} Order Details - Procesamiento completado</h2>

                    <h3>📊 Resumen de procesamiento:</h3>
                    <p><strong>Archivos procesados exitosamente:</strong> {len(files_processed)}</p>
                    <p><strong>Archivos fallidos:</strong> {len(files_failed)}</p>
                    <p><strong>Total registros insertados:</strong> {insert_count}</p>
                    <p><strong>Total registros actualizados:</strong> {update_count}</p>
                    <p><strong>Total errores de validación:</strong> {len(validation_errors)}</p>
                    <p><strong>Fecha procesamiento:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

                    <h3>📁 Archivos procesados exitosamente:</h3>
                    {files_processed_html}

                    {f"<h3 style='color: #f44336;'>❌ Archivos fallidos:</h3>{files_failed_html}" if files_failed else ""}

                    {f"<h3>⚠️ Errores de Validación (primeros 10):</h3><ul>{''.join([f'<li>{error}</li>' for error in validation_errors[:10]])}</ul>" if validation_errors else ""}
                </div>
            </body>
            </html>
            """

            recipients = st.setting_email_recipients['success'] if not files_failed else st.setting_email_recipients['warnings']

            await self.error_handler.email_client.send_email(
                subject=subject,
                html_body=html_body,
                recipients=recipients
            )
