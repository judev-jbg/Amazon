
"""
Servicio de verificación de productos entre ERP y Amazon
Genera 3 archivos de análisis:
1. crear_en_amazon.json - Productos del ERP no en Amazon
2. crear_en_erp.json - SKUs de Amazon no en ERP
3. eliminar_de_amazon.json - Productos inactivos en ERP pero activos en Amazon
"""

import json
import decimal
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Set
from infrastructure.base.async_service import AsyncService
from infrastructure.error_handling import EnhancedErrorHandler
from infrastructure.repositories.mssql_product_repository import MSSQLProductRepository
from core.api.amazon_catalog_api_wrapper import AmazonCatalogAPIWrapper
from infrastructure.metrics_collector import MetricsCollector
import config.setting as st


class ProductVerificationService(AsyncService):
    """
    Verifica existencia de productos entre ERP y Amazon
    Ejecución: Cada hora vía Task Scheduler
    """

    # Productos a excluir de verificación (razones comerciales)
    EXCLUDED_PRODUCTS = st.EXCLUDED_PRODUCTS

    def __init__(self):
        super().__init__()
        self.mssql_repo = MSSQLProductRepository()
        self.catalog_api = AmazonCatalogAPIWrapper()
        self.error_handler = EnhancedErrorHandler()
        self.metrics = MetricsCollector()

        # Directorio para archivos de salida
        self.output_dir = Path("output/product_verification")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Registrar dependencias
        self.register_dependency(self.mssql_repo)
        self.register_dependency(self.error_handler)

        self.logger = logging.getLogger("AmazonManagement")
        self.logger.info("🚀 ProductVerificationService inicializado")

    async def verify_products(self) -> bool:
        """
        Proceso principal de verificación

        Returns:
            bool: True si exitoso
        """
        context = {
            'process_mode': 'product_verification',
            'timestamp': datetime.now()
        }

        try:
            async with self.lifecycle():
                self.logger.info("🔍 Iniciando verificación de productos")

                # 1. Obtener productos del ERP
                erp_products = await self.mssql_repo.get_products_for_verification()

                # 2. Filtrar productos excluidos
                erp_products_filtered = self._filter_excluded(erp_products)

                # 3. Obtener SKUs de Amazon (todos los activos)
                amazon_skus = await self.catalog_api.get_all_seller_skus()

                # 4. ANÁLISIS 1: Productos ERP → Amazon
                missing_in_amazon = await self._find_missing_in_amazon(
                    erp_products_filtered,
                    amazon_skus
                )

                # 5. ANÁLISIS 2: Productos Amazon → ERP
                missing_in_erp, should_delete_from_amazon = await self._find_discrepancies_from_amazon(
                    amazon_skus,
                    erp_products  # Usar lista completa (sin filtrar)
                )

                # 6. Generar archivos
                files_generated = self._generate_output_files(
                    missing_in_amazon=missing_in_amazon,
                    missing_in_erp=missing_in_erp,
                    should_delete=should_delete_from_amazon
                )

                # 7. Registrar métricas
                await self.metrics.record_process_complementary_success(
                    'product_verification',
                    len(missing_in_amazon),
                    len(missing_in_erp),
                    len(should_delete_from_amazon)
                )

                # 8. Notificar resultados
                total_discrepancies = (
                    len(missing_in_amazon) +
                    len(missing_in_erp) +
                    len(should_delete_from_amazon)
                )

                if total_discrepancies > 0:
                    await self._send_verification_notification(
                        missing_in_amazon=len(missing_in_amazon),
                        missing_in_erp=len(missing_in_erp),
                        should_delete=len(should_delete_from_amazon),
                        files=files_generated
                    )
                else:
                    self.logger.info("✅ No se encontraron discrepancias")

                return True

        except Exception as e:
            await self.error_handler.handle_error(e, context)
            return False

    def _filter_excluded(self, products: List[Dict]) -> List[Dict]:
        """Filtrar productos en lista de exclusión"""
        return [
            p for p in products
            if p['IdArticulo'] not in self.EXCLUDED_PRODUCTS
        ]

    async def _find_missing_in_amazon(
        self,
        erp_products: List[Dict],
        amazon_skus: Set[str]
    ) -> List[Dict]:
        """
        Encontrar productos del ERP que NO existen en Amazon

        Returns:
            Lista de productos a crear en Amazon
        """
        missing = []

        for product in erp_products:
            sku = product['IdArticulo']

            if sku not in amazon_skus:
                missing.append({
                    'IdArticulo': product['IdArticulo'],
                    'Descripcion': product['Descripcion'],
                    'Estado': product['Estado'],
                    'CodigoBarras': product.get('CodigoBarras'),
                    'Coste': product.get('Coste', 0)
                })

        self.logger.info(
            f"📦 {len(missing)} productos no encontrados en Amazon")
        return missing

    async def _find_discrepancies_from_amazon(
        self,
        amazon_skus: Set[str],
        erp_products: List[Dict]
    ) -> tuple:
        """
        Encontrar:
        1. SKUs de Amazon que NO existen en ERP
        2. SKUs de Amazon que existen en ERP pero están inactivos (Estado=1)

        Returns:
            Tuple[List, List]: (missing_in_erp, should_delete_from_amazon)
        """
        # Crear mapeo ERP: SKU → producto
        erp_map = {p['IdArticulo']: p for p in erp_products}

        missing_in_erp = []
        should_delete = []

        for sku in amazon_skus:
            if sku not in erp_map:
                # No existe en ERP → crear en ERP
                missing_in_erp.append({'sku': sku})
            else:
                erp_product = erp_map[sku]
                # Existe pero está inactivo → eliminar de Amazon
                if erp_product['Estado'] == 1:
                    should_delete.append({
                        'IdArticulo': sku,
                        'Descripcion': erp_product['Descripcion'],
                        'Estado': erp_product['Estado'],
                        'sku': sku
                    })

        self.logger.info(f"🔄 {len(missing_in_erp)} SKUs no encontrados en ERP")
        self.logger.info(
            f"🗑️ {len(should_delete)} productos a eliminar de Amazon")

        return missing_in_erp, should_delete

    def _generate_output_files(
        self,
        missing_in_amazon: List[Dict],
        missing_in_erp: List[Dict],
        should_delete: List[Dict]
    ) -> Dict[str, Path]:
        """
        Generar 3 archivos JSON

        Returns:
            Dict con rutas de archivos generados
        """
        def decimal_default(obj):
            """Converter para manejar objetos Decimal"""
            if isinstance(obj, decimal.Decimal):
                return float(obj)
            raise TypeError(
                f"Object of type {type(obj).__name__} is not JSON serializable")

        timestamp = datetime.now().strftime("%Y%m%d")
        files = {}

        # 1. Crear en Amazon
        if missing_in_amazon:
            file_path = self.output_dir / f"crear_en_amazon_{timestamp}.json"
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(missing_in_amazon, f, indent=2,
                          ensure_ascii=False, default=decimal_default)
            files['crear_en_amazon'] = file_path
            self.logger.info(f"📄 Generado: {file_path}")

        # 2. Crear en ERP
        if missing_in_erp:
            file_path = self.output_dir / f"crear_en_erp_{timestamp}.json"
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(missing_in_erp, f, indent=2,
                          ensure_ascii=False, default=decimal_default)
            files['crear_en_erp'] = file_path
            self.logger.info(f"📄 Generado: {file_path}")

        # 3. Eliminar de Amazon
        if should_delete:
            file_path = self.output_dir / \
                f"eliminar_de_amazon_{timestamp}.json"
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(should_delete, f, indent=2, ensure_ascii=False,
                          default=decimal_default)
            files['eliminar_de_amazon'] = file_path
            self.logger.info(f"📄 Generado: {file_path}")

        return files

    async def _send_verification_notification(
        self,
        missing_in_amazon: int,
        missing_in_erp: int,
        should_delete: int,
        files: Dict[str, Path]
    ):
        """Notificar resultados de verificación"""
        subject = "[VERIFICACIÓN] Productos Amazon - Discrepancias Encontradas"

        files_section = "<ul>"
        for file_type, file_path in files.items():
            files_section += f"<li><strong>{file_type}</strong>: {file_path.name}</li>"
        files_section += "</ul>"

        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <div style="max-width: 600px; margin: 0 auto;">
                <h2 style="color: #ff9800;">⚠️ Verificación de Productos Completada</h2>
                
                <div style="background: #f5f5f5; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3>📊 Resumen de Discrepancias</h3>
                    <p><strong>Productos no en Amazon:</strong> {missing_in_amazon}</p>
                    <p><strong>SKUs no en ERP:</strong> {missing_in_erp}</p>
                    <p><strong>Productos a eliminar de Amazon:</strong> {should_delete}</p>
                    <p><strong>Fecha:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                
                <div style="background: #fff3cd; padding: 15px; border-radius: 8px;">
                    <h3>📁 Archivos Generados</h3>
                    {files_section}
                    <p><em>Ubicación: output/product_verification/</em></p>
                </div>
                
                <p style="margin-top: 20px; color: #666;">
                    Powered by Amazon Product Verification | {datetime.now().isoformat()}
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
