# core/api/amazon_catalog_api_wrapper.py
"""
Wrapper para Amazon Catalog Items API
Verifica existencia de productos
"""
import logging
from typing import Set, Dict
from sp_api.base import SellingApiException
import config.setting as st


class AmazonCatalogAPIWrapper:
    """
    Wrapper para Catalog Items API
    Responsabilidad: Verificar existencia y obtener listados de productos
    """

    def __init__(self):
        self.credentials = self._load_credentials()
        self.logger = logging.getLogger("AmazonManagement")

        # Cache para evitar requests repetidas
        self._sku_cache = None
        self._cache_timestamp = None

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

    async def get_all_seller_skus(
        self,
        marketplace_id: str = 'A1RKKUPIHCS9HS',
        force_refresh: bool = False
    ) -> Set[str]:
        """
        Obtener todos los SKUs del vendedor en Amazon
        Usa ListingsItems API en lugar de Catalog para obtener solo nuestros productos

        Args:
            marketplace_id: ID del marketplace
            force_refresh: Forzar actualización del cache

        Returns:
            Set de SKUs
        """
        try:
            from sp_api.api import ListingsItems
            from datetime import datetime, timedelta

            # Usar cache si está disponible y no es muy antiguo (< 1 hora)
            if not force_refresh and self._sku_cache and self._cache_timestamp:
                age = datetime.now() - self._cache_timestamp
                if age < timedelta(hours=1):
                    self.logger.info(
                        f"📦 Usando cache: {len(self._sku_cache)} SKUs")
                    return self._sku_cache

            self.logger.info("🔄 Obteniendo SKUs de Amazon...")

            skus = set()

            # Usar ListingsItems para obtener solo productos del seller
            listings_api = ListingsItems(credentials=self.credentials)

            # Obtener listados (paginado)
            next_token = None
            page_count = 0

            while True:
                page_count += 1
                self.logger.debug(f"Procesando página {page_count}...")

                params = {
                    'sellerId': st.setting_cred_api_amz.get('seller_id'),
                    'marketplaceIds': [marketplace_id]
                }

                if next_token:
                    params['pageToken'] = next_token

                response = listings_api.search_listings_items(**params)

                # Extraer SKUs
                items = response.payload.get('items', [])
                for item in items:
                    sku = item.get('sku')
                    if sku:
                        skus.add(sku)

                # Verificar si hay más páginas
                next_token = response.next_token
                if not next_token:
                    break

            if not skus:
                self.logger.warning("⚠️ No se encontraron SKUs en Amazon")
            else:
                self.logger.info(f"✅ {len(skus)} SKUs obtenidos de Amazon")

            # Actualizar cache
            self._sku_cache = skus
            self._cache_timestamp = datetime.now()

            return skus

        except SellingApiException as ex:
            self.logger.error(f"Amazon API error obteniendo SKUs: {ex}")

            # Si falla, retornar cache antiguo si existe
            if self._sku_cache:
                self.logger.warning(
                    "Usando cache antiguo debido a error de API")
                return self._sku_cache

            return set()

        except Exception as e:
            self.logger.error(f"Error inesperado obteniendo SKUs: {e}")
            return set()

    async def check_sku_exists(
        self,
        sku: str,
        marketplace_id: str = 'A1RKKUPIHCS9HS'
    ) -> bool:
        """
        Verificar si un SKU específico existe en Amazon

        Args:
            sku: SKU a verificar
            marketplace_id: ID del marketplace

        Returns:
            bool: True si existe
        """
        try:
            from sp_api.api import ListingsItems

            listings_api = ListingsItems(credentials=self.credentials)

            response = listings_api.get_listings_item(
                sellerId=st.setting_cred_api_amz.get('seller_id'),
                sku=sku,
                marketplaceIds=[marketplace_id]
            )

            # Si no lanza excepción, el SKU existe
            return response.payload is not None

        except SellingApiException as ex:
            if hasattr(ex, 'code') and ex.code == 404:
                return False

            self.logger.error(f"Error verificando SKU {sku}: {ex}")
            return False

        except Exception as e:
            self.logger.error(f"Error inesperado verificando SKU {sku}: {e}")
            return False

    def clear_cache(self):
        """Limpiar cache de SKUs"""
        self._sku_cache = None
        self._cache_timestamp = None
        self.logger.info("🗑️ Cache de SKUs limpiado")
