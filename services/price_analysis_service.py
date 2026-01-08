# services/price_analysis_service.py
"""
Sistema de análisis de precios competitivo para Amazon
Objetivos:
1. Ganar buybox
2. Vender al menor precio posible >= PVPM
3. Mantener 3 EUR diferencia con competencia (cuando sea posible)
4. CRÍTICO: Nunca vender por debajo de PVPM
"""
import asyncio
from decimal import Decimal
import decimal
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
from infrastructure.base.async_service import AsyncService
from infrastructure.error_handling import EnhancedErrorHandler
from infrastructure.repositories.mssql_product_repository import MSSQLProductRepository
from core.api.amazon_pricing_api_wrapper import AmazonPricingAPIWrapper
from core.calculators.pvpm_calculator import PVPMCalculator
from core.calculators.pricing_strategy import PricingStrategyCalculator
from infrastructure.metrics_collector import MetricsCollector
import config.setting as st


@dataclass
class PriceAnalysisResult:
    """Resultado del análisis de un producto"""
    sku: str
    asin: str
    pvpm: float
    current_price: float
    competitor_buybox_price: Optional[float]
    lowest_competitor_price: Optional[float]
    recommendation: str  # 'keep', 'win_buybox', 'lower_price', 'critical_below_pvpm'
    new_price: Optional[float]
    savings_potential: Optional[float]

    def to_dict(self):
        return asdict(self)


class PriceAnalysisService(AsyncService):
    """
    Servicio de análisis de precios competitivo

    Estrategia de ejecución:
    - Intervalo recomendado: Cada 4-6 horas
    - Batch processing: 20 productos por lote
    - Caching: 2 horas de validez
    - Rate limiting: Respetar límites de Pricing API
    """

    def __init__(self):
        super().__init__()
        self.mssql_repo = MSSQLProductRepository()
        self.pricing_api = AmazonPricingAPIWrapper()
        self.pvpm_calculator = PVPMCalculator()
        self.strategy_calculator = PricingStrategyCalculator()
        self.error_handler = EnhancedErrorHandler()
        self.metrics = MetricsCollector()

        # Directorio para archivos de salida
        self.output_dir = Path("output/price_analysis")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Cache de análisis (evitar procesar lo mismo múltiples veces)
        self._analysis_cache = {}
        self._cache_expiry = timedelta(hours=2)

        # Registrar dependencias
        self.register_dependency(self.mssql_repo)
        self.register_dependency(self.error_handler)

        self.logger = logging.getLogger("AmazonManagement")
        self.logger.info("🚀 PriceAnalysisService inicializado")

    async def analyze_prices(self, force_refresh: bool = False) -> bool:
        """
        Proceso principal de análisis de precios

        Args:
            force_refresh: Ignorar cache y forzar análisis completo

        Returns:
            bool: True si exitoso
        """
        context = {
            'process_mode': 'price_analysis',
            'timestamp': datetime.now(),
            'force_refresh': force_refresh
        }

        try:
            async with self.lifecycle():
                self.logger.info("💰 Iniciando análisis de precios")

                # 1. Obtener productos del ERP con datos de coste
                products = await self.mssql_repo.get_products_for_pricing_analysis()

                if not products:
                    self.logger.info("No hay productos para analizar")
                    return True

                self.logger.info(f"📦 {len(products)} productos a analizar")

                # 2. Calcular PVPM para cada producto
                products_with_pvpm = self._calculate_pvpm_batch(products)

                # 3. Obtener precios de Amazon (batch + rate limiting)
                analysis_results = await self._analyze_products_batch(
                    products_with_pvpm,
                    force_refresh
                )

                # 4. Clasificar resultados
                classification = self._classify_results(analysis_results)

                # 5. Generar archivos
                files_generated = self._generate_analysis_files(classification)

                # 6. Registrar métricas
                await self.metrics.record_process_complementary_success(
                    'price_analysis',
                    classification['buybox_opportunities'],
                    classification['lower_price_opportunities'],
                    classification['critical_below_pvpm']
                )

                # 7. Notificar resultados
                total_opportunities = (
                    classification['buybox_opportunities'] +
                    classification['lower_price_opportunities']
                )

                if classification['critical_below_pvpm'] > 0 or total_opportunities > 0:
                    await self._send_analysis_notification(
                        classification=classification,
                        files=files_generated
                    )
                else:
                    self.logger.info(
                        "✅ No se encontraron oportunidades de mejora")

                return True

        except Exception as e:
            await self.error_handler.handle_error(e, context)
            return False

    def _calculate_pvpm_batch(self, products: List[Dict]) -> List[Dict]:
        """
        Calcular PVPM para todos los productos

        Returns:
            Lista de productos con campo 'pvpm' agregado
        """
        products_with_pvpm = []

        for product in products:
            try:
                pvpm = self.pvpm_calculator.calculate_pvpm(
                    coste=product.get('Coste', 0),
                    peso=product.get('Peso', 0)
                )

                product['pvpm'] = round(pvpm, 2)
                products_with_pvpm.append(product)

            except Exception as e:
                self.logger.error(
                    f"Error calculando PVPM para {product['IdArticulo']}: {e}"
                )
                # Continuar con siguientes productos
                continue

        self.logger.info(
            f"✅ PVPM calculado para {len(products_with_pvpm)} productos")
        return products_with_pvpm

    async def _analyze_products_batch(
        self,
        products: List[Dict],
        force_refresh: bool
    ) -> List[PriceAnalysisResult]:
        """
        Analizar precios de productos en lotes
        Implementa rate limiting y caching
        """
        results = []
        batch_size = 20  # Procesar 20 productos por lote

        for i in range(0, len(products), batch_size):
            batch = products[i:i + batch_size]

            self.logger.info(
                f"📊 Procesando lote {i//batch_size + 1}/{(len(products)-1)//batch_size + 1}"
            )

            for product in batch:
                sku = product['IdArticulo']

                # Verificar cache
                if not force_refresh and self._is_cached(sku):
                    result = self._get_from_cache(sku)
                    results.append(result)
                    self.logger.debug(f"✓ Cache hit: {sku}")
                    continue

                # Analizar producto
                try:
                    result = await self._analyze_single_product(product)
                    if result:
                        results.append(result)
                        self._save_to_cache(sku, result)

                except Exception as e:
                    self.logger.error(f"Error analizando {sku}: {e}")
                    continue

            # Pequeña pausa entre lotes (rate limiting)
            if i + batch_size < len(products):
                await asyncio.sleep(10)

        return results

    async def _analyze_single_product(self, product: Dict) -> Optional[PriceAnalysisResult]:
        """
        Analizar un producto individual
        """
        try:
            sku = product['IdArticulo']
            pvpm = product['pvpm']

            # 1. Obtener pricing info de Amazon
            self.logger.info(f"🔍 Analizando producto {sku}")
            pricing_info = await self.pricing_api.get_competitive_pricing(sku)

            if not pricing_info['success']:
                self.logger.warning(f"No se pudo obtener pricing para {sku}")
                return None

            asin = pricing_info.get('asin')
            current_price = pricing_info.get('your_price')
            buybox_price = pricing_info.get('buybox_price')
            competitors = pricing_info.get('competitors', [])

            # 2. Aplicar estrategia de pricing
            recommendation = self.strategy_calculator.calculate_optimal_price(
                pvpm=pvpm,
                current_price=current_price,
                buybox_price=buybox_price,
                competitors=competitors
            )

            # 3. Crear resultado
            result = PriceAnalysisResult(
                sku=sku,
                asin=asin,
                pvpm=pvpm,
                current_price=current_price,
                competitor_buybox_price=buybox_price,
                lowest_competitor_price=min(
                    competitors) if competitors else None,
                recommendation=recommendation['action'],
                new_price=recommendation.get('new_price'),
                savings_potential=recommendation.get('savings')
            )

            return result
        except Exception as e:
            self.logger.error(
                f"Error analizando producto {product.get('IdArticulo')}: {e}")
            return None

    def _classify_results(self, results: List[PriceAnalysisResult]) -> Dict:
        """
        Clasificar resultados en 3 categorías
        """
        below_pvpm = []
        buybox_opportunities = []
        lower_price_opportunities = []

        if results is None:
            self.logger.warning("No hay resultados para clasificar")
            return {
                'below_pvpm': below_pvpm,
                'below_pvpm_count': 0,
                'buybox_opportunities': 0,
                'buybox_list': buybox_opportunities,
                'lower_price_opportunities': 0,
                'lower_price_list': lower_price_opportunities,
                'critical_below_pvpm': 0
            }

        for result in results:

            if result is None:
                continue

            if not result.current_price:
                self.logger.warning(
                    f"Producto {result.sku} no tiene precio actual")
                continue

            if not result.pvpm:
                self.logger.warning(
                    f"Producto {result.sku} no tiene PVPM calculado")
                continue

            # CRÍTICO: Precio por debajo de PVPM
            if Decimal(result.current_price) < Decimal(result.pvpm):
                below_pvpm.append(result)

            # Oportunidad de ganar buybox
            elif result.recommendation == 'win_buybox':
                buybox_opportunities.append(result)

            # Oportunidad de bajar precio (sin buybox)
            elif result.recommendation == 'lower_price':
                lower_price_opportunities.append(result)

        return {
            'below_pvpm': below_pvpm,
            'below_pvpm_count': len(below_pvpm),
            'buybox_opportunities': len(buybox_opportunities),
            'buybox_list': buybox_opportunities,
            'lower_price_opportunities': len(lower_price_opportunities),
            'lower_price_list': lower_price_opportunities,
            'critical_below_pvpm': len(below_pvpm)
        }

    def _generate_analysis_files(self, classification: Dict) -> Dict[str, Path]:
        """
        Generar 3 archivos JSON con recomendaciones
        """

        def decimal_default(obj):
            """Converter para manejar objetos Decimal"""
            if isinstance(obj, decimal.Decimal):
                return float(obj)
            raise TypeError(
                f"Object of type {type(obj).__name__} is not JSON serializable")

        timestamp = datetime.now().strftime("%Y%m%d")
        files = {}

        # 1. Productos por debajo de PVPM (CRÍTICO)
        if classification['below_pvpm']:
            file_path = self.output_dir / f"below_pvpm_{timestamp}.json"
            data = [
                {
                    'sku': r.sku,
                    'asin': r.asin,
                    'precio_actual': r.current_price,
                    'pvpm': r.pvpm,
                    'diferencia': round(Decimal(r.pvpm) - Decimal(r.current_price), 2)
                }
                for r in classification['below_pvpm']
            ]
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False,
                          default=decimal_default)
            files['below_pvpm'] = file_path

        # 2. Oportunidades de ganar buybox
        if classification['buybox_list']:
            file_path = self.output_dir / \
                f"buybox_opportunities_{timestamp}.json"
            data = [r.to_dict() for r in classification['buybox_list']]
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False,
                          default=decimal_default)
            files['buybox_opportunities'] = file_path

        # 3. Oportunidades de bajar precio
        if classification['lower_price_list']:
            file_path = self.output_dir / \
                f"lower_price_opportunities_{timestamp}.json"
            data = [r.to_dict() for r in classification['lower_price_list']]
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False,
                          default=decimal_default)
            files['lower_price_opportunities'] = file_path

        self.logger.info(f"📁 Generados {len(files)} archivos de análisis")
        return files

    def _is_cached(self, sku: str) -> bool:
        """Verificar si análisis está en cache y es válido"""
        if sku not in self._analysis_cache:
            return False

        cached_time = self._analysis_cache[sku]['timestamp']
        age = datetime.now() - cached_time

        return age < self._cache_expiry

    def _get_from_cache(self, sku: str) -> PriceAnalysisResult:
        """Obtener resultado del cache"""
        return self._analysis_cache[sku]['result']

    def _save_to_cache(self, sku: str, result: PriceAnalysisResult):
        """Guardar resultado en cache"""
        self._analysis_cache[sku] = {
            'result': result,
            'timestamp': datetime.now()
        }

    async def _send_analysis_notification(
        self,
        classification: Dict,
        files: Dict[str, Path]
    ):
        """Notificar resultados del análisis"""
        critical_alert = ""
        if classification['critical_below_pvpm'] > 0:
            critical_alert = f"""
            <div style="background: #f44336; color: white; padding: 15px; border-radius: 8px; margin: 15px 0;">
                <h3>🚨 ALERTA CRÍTICA</h3>
                <p><strong>{classification['critical_below_pvpm']} productos</strong> 
                están vendiendo por debajo del PVPM. Requiere acción inmediata.</p>
            </div>
            """

        subject = "[ANÁLISIS] Precios Amazon - Oportunidades Detectadas"
        if classification['critical_below_pvpm'] > 0:
            subject = "[CRÍTICO] Precios por debajo de PVPM detectados"

        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <div style="max-width: 600px; margin: 0 auto;">
                <h2 style="color: #2196F3;">💰 Análisis de Precios Completado</h2>
                
                {critical_alert}
                
                <div style="background: #f5f5f5; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3>📊 Oportunidades Detectadas</h3>
                    <p><strong>Ganar Buybox:</strong> {classification['buybox_opportunities']} productos</p>
                    <p><strong>Bajar Precio:</strong> {classification['lower_price_opportunities']} productos</p>
                    <p><strong>⚠️ Por debajo PVPM:</strong> {classification['critical_below_pvpm']} productos</p>
                    <p><strong>Fecha:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                
                <div style="background: #e3f2fd; padding: 15px; border-radius: 8px;">
                    <h3>📁 Archivos Generados</h3>
                    <ul>
                        {"".join([f"<li>{k}: {v.name}</li>" for k, v in files.items()])}
                    </ul>
                    <p><em>Ubicación: output/price_analysis/</em></p>
                </div>
            </div>
        </body>
        </html>
        """

        recipients = st.setting_email_recipients['critical'] if classification[
            'critical_below_pvpm'] > 0 else st.setting_email_recipients['success']

        await self.error_handler.email_client.send_email(
            subject=subject,
            html_body=html_body,
            recipients=recipients
        )
