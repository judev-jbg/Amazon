"""
Wrapper para Amazon Product Pricing API
Obtiene precios competitivos y buybox info
"""
import logging
from typing import Dict, List, Optional
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
        self.logger = logging.getLogger(self.__class__.__name__)

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
        marketplace_id: str = 'A1RKKUPIHCS9HS'
    ) -> Dict:
        """
        Obtener información de precios competitivos para un SKU

        Args:
            sku: SKU del producto
            marketplace_id: ID del marketplace

        Returns:
            Dict: {
                'success': bool,
                'asin': str,
                'your_price': float,
                'buybox_price': float,
                'competitors': [float],
                'error': str
            }
        """
        try:
            pricing_api = Products(credentials=self.credentials)

            # Obtener competitive pricing
            response = pricing_api.get_competitive_pricing_for_sku(
                seller_sku=sku,
                marketplace_id=marketplace_id
            )

            payload = response.payload

            if not payload:
                return {
                    'success': False,
                    'error': 'No pricing data available'
                }

            # Extraer datos del response
            product = payload[0] if isinstance(payload, list) else payload

            # ASIN
            asin = product.get('ASIN', '')

            # Tu precio actual
            your_price = self._extract_your_price(product)

            # Precio del buybox
            buybox_price = self._extract_buybox_price(product)

            # Precios de competidores
            competitors = self._extract_competitor_prices(product)

            self.logger.debug(
                f"✅ {sku}: Your={your_price}, Buybox={buybox_price}, "
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

        except SellingApiException as ex:
            self.logger.error(f"Amazon API error para {sku}: {ex}")

            error_msg = str(ex)
            if hasattr(ex, 'code'):
                if ex.code == 429:
                    error_msg = "Rate limit alcanzado"
                elif ex.code == 404:
                    error_msg = "SKU no encontrado"

            return {
                'success': False,
                'sku': sku,
                'error': error_msg
            }

        except Exception as e:
            self.logger.error(f"Error inesperado para {sku}: {e}")

            return {
                'success': False,
                'sku': sku,
                'error': str(e)
            }

    def _extract_your_price(self, product: Dict) -> Optional[float]:
        """Extraer tu precio actual del producto"""
        try:
            # Buscar en diferentes ubicaciones del response
            price_data = product.get('Product', {}).get('Offers', [])

            for offer in price_data:
                if offer.get('IsFeaturedMerchant', False):
                    listing_price = offer.get('ListingPrice', {})
                    amount = listing_price.get('Amount')
                    if amount:
                        return float(amount)

            # Fallback: buscar en CompetitivePricing
            competitive_prices = product.get(
                'Product', {}).get('CompetitivePricing', {})
            competitive_prices_list = competitive_prices.get(
                'CompetitivePrices', [])

            for price_data in competitive_prices_list:
                if price_data.get('belongsToRequester', False):
                    price = price_data.get('Price', {})
                    landed_price = price.get('LandedPrice', {})
                    amount = landed_price.get('Amount')
                    if amount:
                        return float(amount)

            return None

        except Exception as e:
            self.logger.warning(f"Error extrayendo your_price: {e}")
            return None

    def _extract_buybox_price(self, product: Dict) -> Optional[float]:
        """Extraer precio del actual poseedor del buybox"""
        try:
            # Buscar en Offers con IsBuyBoxWinner=True
            price_data = product.get('Product', {}).get('Offers', [])

            for offer in price_data:
                if offer.get('IsBuyBoxWinner', False):
                    listing_price = offer.get('ListingPrice', {})
                    amount = listing_price.get('Amount')
                    if amount:
                        return float(amount)

            # Fallback: buscar en CompetitivePricing
            competitive_pricing = product.get(
                'Product', {}).get('CompetitivePricing', {})
            buybox_prices = competitive_pricing.get('BuyBoxPrices', [])

            if buybox_prices:
                landed_price = buybox_prices[0].get('LandedPrice', {})
                amount = landed_price.get('Amount')
                if amount:
                    return float(amount)

            return None

        except Exception as e:
            self.logger.warning(f"Error extrayendo buybox_price: {e}")
            return None

    def _extract_competitor_prices(self, product: Dict) -> List[float]:
        """Extraer lista de precios de competidores"""
        try:
            prices = []

            # Buscar en CompetitivePrices
            competitive_pricing = product.get(
                'Product', {}).get('CompetitivePricing', {})
            competitive_prices = competitive_pricing.get(
                'CompetitivePrices', [])

            for price_data in competitive_prices:
                # Excluir tu propio precio
                if price_data.get('belongsToRequester', False):
                    continue

                price = price_data.get('Price', {})
                landed_price = price.get('LandedPrice', {})
                amount = landed_price.get('Amount')

                if amount:
                    prices.append(float(amount))

            # También buscar en Offers
            offers = product.get('Product', {}).get('Offers', [])

            for offer in offers:
                # Excluir tu propio precio y el buybox (ya contabilizado)
                if offer.get('IsFeaturedMerchant', False) or offer.get('IsBuyBoxWinner', False):
                    continue

                listing_price = offer.get('ListingPrice', {})
                amount = listing_price.get('Amount')

                if amount:
                    prices.append(float(amount))

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
        Implementa rate limiting interno (0.5 req/sec = 2 segundos por request)

        Args:
            skus: Lista de SKUs
            marketplace_id: ID del marketplace

        Returns:
            Lista de resultados de pricing
        """
        import asyncio

        results = []

        for sku in skus:
            result = await self.get_competitive_pricing(sku, marketplace_id)
            results.append(result)

            # Rate limiting: 0.5 requests/second = esperar 2 segundos
            await asyncio.sleep(2.1)  # 2.1s para estar seguros

        return results
