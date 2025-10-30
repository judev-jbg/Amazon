"""
Wrapper para Amazon Product Pricing API
Obtiene precios competitivos y buybox info
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from sp_api.api import Products
from sp_api.base import SellingApiException
import config.setting as st


class AmazonPricingAPIWrapper:
    """
    Wrapper para Product Pricing API

    Rate Limits (según Amazon):
    - GetCompetitivePricing: 0.5 requests/second
    - GetPricing: 0.5 requests/second

    IMPORTANTE: Usar con rate limiting estricto
    """

    def __init__(self):
        self.credentials = self._load_credentials()
        self.logger = logging.getLogger("AmazonManagement")
        self._request_semaphore = asyncio.Semaphore(1)
        self._last_request_time = None
        self._min_delay_between_requests = 2.1
        self._consecutive_quota_errors = 0
        self._backoff_until = None

    def _load_credentials(self) -> Dict:
        """Cargar credenciales desde configuración"""
        return dict(
            refresh_token=st.setting_cred_api_amz['refresh_token'],
            lwa_app_id=st.setting_cred_api_amz['lwa_app_id'],
            lwa_client_secret=st.setting_cred_api_amz['lwa_client_secret'],
            aws_secret_key=st.setting_cred_api_amz['aws_secret_key'],
            aws_access_key=st.setting_cred_api_amz['aws_access_key'],
            role_arn=st.setting_cred_api_amz['role_arn']
        )

    async def get_competitive_pricing(
        self,
        sku: str,
        marketplace_id: str = 'A1RKKUPIHCS9HS',
        max_retries: int = 3
    ) -> Dict:
        """
        Obtener información de precios competitivos para un SKU
        CON RATE LIMITING ESTRICTO

        Args:
            sku: SKU del producto
            marketplace_id: ID del marketplace
            max_retries: Número máximo de reintentos

        Returns:
            Dict con pricing info o error
        """
        # Usar semáforo para serializar requests
        async with self._request_semaphore:
            # Verificar si estamos en backoff
            if self._backoff_until and datetime.now() < self._backoff_until:
                wait_seconds = (self._backoff_until -
                                datetime.now()).total_seconds()
                self.logger.warning(
                    f"En backoff por QuotaExceeded, esperando {wait_seconds:.1f}s"
                )
                await asyncio.sleep(wait_seconds)
                self._backoff_until = None

            # Respetar delay mínimo entre requests
            await self._enforce_rate_limit()

            # Intentar request con retry
            for attempt in range(max_retries):
                try:
                    result = await self._execute_pricing_request(sku, marketplace_id)

                    # Éxito: resetear contador de errores
                    self._consecutive_quota_errors = 0

                    return result

                except SellingApiException as ex:
                    if self._is_quota_exceeded(ex):
                        self._consecutive_quota_errors += 1

                        # Backoff exponencial más agresivo
                        backoff_seconds = min(
                            60, 10 * (2 ** self._consecutive_quota_errors))

                        self.logger.error(
                            f"QuotaExceeded para {sku} (intento {attempt + 1}/{max_retries}), "
                            f"backoff {backoff_seconds}s"
                        )

                        # Establecer backoff global
                        self._backoff_until = datetime.now() + timedelta(seconds=backoff_seconds)

                        if attempt < max_retries - 1:
                            await asyncio.sleep(backoff_seconds)
                        else:
                            return {
                                'success': False,
                                'sku': sku,
                                'error': 'QuotaExceeded - Rate limit alcanzado después de múltiples reintentos'
                            }
                    else:
                        # Otro error de API
                        return self._handle_api_exception(sku, ex)

                except Exception as e:
                    self.logger.error(f"Error inesperado para {sku}: {e}")
                    return {
                        'success': False,
                        'sku': sku,
                        'error': str(e)
                    }

            # Si llegamos aquí, fallaron todos los reintentos
            return {
                'success': False,
                'sku': sku,
                'error': 'Máximo de reintentos alcanzado'
            }

    async def _enforce_rate_limit(self):
        """
        Forzar delay mínimo entre requests (2.1 segundos)
        """
        if self._last_request_time:
            elapsed = (datetime.now() -
                       self._last_request_time).total_seconds()

            if elapsed < self._min_delay_between_requests:
                sleep_time = self._min_delay_between_requests - elapsed
                self.logger.debug(
                    f"Rate limiting: esperando {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)

        # Actualizar timestamp del último request
        self._last_request_time = datetime.now()

    async def _execute_pricing_request(self, sku: str, marketplace_id: str) -> Dict:
        """
        Ejecutar request a Pricing API (sin retry, solo una vez)
        """
        try:
            self.logger.debug(f"Consultando pricing para {sku}")

            # Ejecutar en thread pool porque sp_api es síncrono
            pricing_api = Products(credentials=self.credentials)

            # Llamada síncrona envuelta en asyncio.to_thread

            response = await asyncio.to_thread(
                pricing_api.get_listings_offer,
                seller_sku=sku,
                item_condition='New',
                marketplace_id=marketplace_id
            )

            payload = response.payload
            self.logger.info(f"Response para {sku}: {response}")

            if not payload:
                return {
                    'success': False,
                    'sku': sku,
                    'error': 'No pricing data available'
                }

            # Extraer datos del response
            product = payload[0] if isinstance(payload, list) else payload

            # ASIN
            asin = product.get('ASIN', '')

            # Tu precio actual
            your_price = self._extract_your_price(product)

            # Precio del buybox
            buybox_price, we_have_buybox = self._extract_buybox_price(product)

            # Precios de competidores
            competitors = self._extract_competitor_prices(
                product, exclude_buybox_price=buybox_price)

            self.logger.debug(
                f"✅ {sku}: Your={your_price}, Buybox={buybox_price}, WeHaveBuybox={we_have_buybox}, "
                f"Competitors={len(competitors)}"
            )

            return {
                'success': True,
                'sku': sku,
                'asin': asin,
                'your_price': your_price,
                'buybox_price': buybox_price,
                'competitors': competitors,
                'error': None
            }

        except Exception as e:
            self.logger.error(f"Error ejecutando request para {sku}: {e}")
            return {
                'success': False,
                'sku': sku,
                'error': str(e)
            }

    def _is_quota_exceeded(self, exception: SellingApiException) -> bool:
        """Verificar si el error es QuotaExceeded"""
        error_str = str(exception)
        return 'QuotaExceeded' in error_str or (hasattr(exception, 'code') and exception.code == 429)

    def _handle_api_exception(self, sku: str, exception: SellingApiException) -> Dict:
        """Manejar excepciones de API no-quota"""
        self.logger.error(f"Amazon API error para {sku}: {exception}")

        error_msg = str(exception)
        if hasattr(exception, 'code'):
            if exception.code == 404:
                error_msg = "SKU no encontrado"
            else:
                error_msg = f"API Error (code {exception.code})"

        return {
            'success': False,
            'sku': sku,
            'error': error_msg
        }

    def _extract_your_price(self, product: Dict) -> Optional[float]:
        """Extraer tu precio actual del producto"""
        try:
            # Buscar en diferentes ubicaciones del response
            price_data = product.get('Offers', [])

            for offer in price_data:
                if offer.get('MyOffer', False):

                    listing_price = offer.get('ListingPrice', {})
                    amount = listing_price.get('Amount')
                    if amount:
                        self.logger.info(f"Extraído your_price: {amount}")
                        return float(amount)

            return None

        except Exception as e:
            self.logger.warning(f"Error extrayendo your_price: {e}")
            return None

    def _extract_buybox_price(self, product: Dict) -> Optional[Tuple[float, bool]]:
        """Extraer precio del actual poseedor del buybox"""
        try:
            # Buscar en Offers con IsBuyBoxWinner=True
            price_data = product.get('Offers', [])

            for offer in price_data:
                if offer.get('IsBuyBoxWinner', False):
                    listing_price = offer.get('ListingPrice', {})
                    amount = listing_price.get('Amount')
                    if amount:
                        self.logger.info(f"Extraído buybox_price: {amount}")
                        is_my_buybox = offer.get('MyOffer', False)
                        return float(amount), is_my_buybox

            return None

        except Exception as e:
            self.logger.warning(f"Error extrayendo buybox_price: {e}")
            return None

    def _extract_competitor_prices(self, product: Dict, exclude_buybox_price: Optional[float] = None) -> List[float]:
        """Extraer lista de precios de competidores"""
        try:
            prices = []

            # Buscar en Offers
            offers = product.get('Offers', [])

            for offer in offers:
                # Excluir tu propio precio y el buybox (ya contabilizado)
                if offer.get('MyOffer', False):
                    continue

                listing_price = offer.get('ListingPrice', {})
                amount = listing_price.get('Amount')

                if amount:
                    price_float = float(amount)
                    # Excluir específicamente el precio BuyBox que ya capturamos
                    if exclude_buybox_price is not None and price_float == exclude_buybox_price:
                        continue
                    prices.append(price_float)

            # Eliminar duplicados y ordenar
            prices = sorted(list(set(prices)))

            return prices

        except Exception as e:
            self.logger.warning(f"Error extrayendo competitor_prices: {e}")
            return []

    async def get_pricing_batch(
        self,
        skus: List[str],
        marketplace_id: str = 'A1RKKUPIHCS9HS'
    ) -> List[Dict]:
        """
        Obtener pricing para múltiples SKUs
        Rate limiting automático (no necesita sleep externo)

        Args:
            skus: Lista de SKUs
            marketplace_id: ID del marketplace

        Returns:
            Lista de resultados de pricing
        """
        results = []

        total = len(skus)
        self.logger.info(f"Procesando {total} SKUs con rate limiting estricto")

        for idx, sku in enumerate(skus, 1):
            self.logger.debug(f"Procesando SKU {idx}/{total}: {sku}")

            result = await self.get_competitive_pricing(sku, marketplace_id)
            results.append(result)

            # Log progreso cada 10 SKUs
            if idx % 10 == 0:
                success_count = sum(1 for r in results if r.get('success'))
                self.logger.info(
                    f"Progreso: {idx}/{total} SKUs procesados "
                    f"({success_count} exitosos)"
                )

        # Resumen final
        success_count = sum(1 for r in results if r.get('success'))
        failed_count = total - success_count

        self.logger.info(
            f"Batch completado: {success_count} exitosos, {failed_count} fallidos"
        )

        return results
