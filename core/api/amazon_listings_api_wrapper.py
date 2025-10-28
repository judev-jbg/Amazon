# core/api/amazon_listings_api_wrapper.py
"""
Wrapper limpio para Amazon Listings Items API
Gestiona actualizaciones de inventario (quantity) y atributos de productos
"""
import logging
import asyncio
from typing import Dict, Tuple, Optional, List
from sp_api.api import ListingsItems
from sp_api.base import SellingApiException, Marketplaces
import config.setting as st


class AmazonListingsAPIWrapper:
    """
    Wrapper para Listings Items API
    Responsabilidad: Actualizar atributos de productos (principalmente stock/quantity)

    Endpoints principales:
    - patchListingsItem: Actualizar atributos específicos (PATCH)
    - getListingsItem: Obtener información de listing (GET)
    - putListingsItem: Crear/reemplazar listing completo (PUT)
    """

    def __init__(self):
        self.credentials = self._load_credentials()
        self.logger = logging.getLogger(self.__class__.__name__)

        # Mapeo de marketplace IDs a enum Marketplaces
        self.marketplace_map = {
            'A1RKKUPIHCS9HS': Marketplaces.ES,  # España
            'A1PA6795UKMFR9': Marketplaces.DE,  # Alemania
            'APJ6JRA9NG5V4': Marketplaces.IT,   # Italia
            'A1805IZSGTT6HS': Marketplaces.NL,  # Países Bajos
            'AMEN7PMS3EDWL': Marketplaces.BE,   # Bélgica
        }

        # Seller ID
        self.seller_id = st.setting_cred_api_amz.get('seller_id')
        if not self.seller_id:
            raise ValueError(
                "seller_id no configurado en setting_cred_api_amz")

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

    def _get_marketplace_enum(self, marketplace_id: str) -> str:
        """
        Obtener marketplace string para API

        Args:
            marketplace_id: ID del marketplace (ej: A1RKKUPIHCS9HS)

        Returns:
            String del marketplace ID
        """
        # La API espera el ID directamente, no el enum
        return marketplace_id

    async def update_quantity(
        self,
        sku: str,
        quantity: int,
        marketplace_id: str = 'A1RKKUPIHCS9HS'  # España por defecto
    ) -> Dict:
        """
        Actualizar cantidad disponible de un producto

        Args:
            sku: SKU del producto (seller_sku)
            quantity: Nueva cantidad (típicamente 0 o 10)
            marketplace_id: ID del marketplace

        Returns:
            Dict: {
                'success': bool,
                'message': str,
                'sku': str,
                'quantity': int,
                'error': Optional[str]
            }
        """
        try:
            self.logger.debug(
                f"Actualizando {sku} → Q={quantity} en marketplace {marketplace_id}")

            # Preparar payload para PATCH operation
            patches = [
                {
                    "op": "replace",
                    "path": "/attributes/fulfillment_availability",
                    "value": [
                        {
                            "fulfillment_channel_code": "DEFAULT",
                            "quantity": quantity
                        }
                    ]
                }
            ]

            # Crear instancia de API
            def update_listing():
                listings_api = ListingsItems(credentials=self.credentials)

                response = listings_api.patch_listings_item(
                    sellerId=self.seller_id,
                    sku=sku,
                    marketplaceIds=[marketplace_id],
                    body={
                        "productType": "PRODUCT",
                        "patches": patches
                    }
                )
                return response

            # Ejecutar en thread pool (sp_api es síncrono)
            response = await asyncio.to_thread(update_listing)

            self.logger.info(f"✅ {sku} actualizado a Q={quantity}")

            return {
                'success': True,
                'message': f'SKU {sku} actualizado correctamente',
                'sku': sku,
                'quantity': quantity,
                'error': None
            }

        except SellingApiException as ex:
            self.logger.error(f"❌ Amazon API error para {sku}: {ex}")

            # Manejo de errores específicos
            error_msg = str(ex)
            error_code = None

            if hasattr(ex, 'code'):
                error_code = ex.code
                if ex.code == 429:
                    error_msg = "Rate limit alcanzado"
                elif ex.code == 404:
                    error_msg = "SKU no encontrado en Amazon"
                elif ex.code == 400:
                    error_msg = "Bad request - verificar formato de datos"
                elif ex.code == 403:
                    error_msg = "Sin permisos - verificar credenciales"

            return {
                'success': False,
                'message': f'Error actualizando {sku}',
                'error': error_msg,
                'error_code': error_code,
                'sku': sku,
                'quantity': quantity
            }

        except Exception as e:
            self.logger.error(f"❌ Error inesperado para {sku}: {e}")

            return {
                'success': False,
                'message': f'Error inesperado: {sku}',
                'error': str(e),
                'error_code': None,
                'sku': sku,
                'quantity': quantity
            }

    async def get_current_quantity(
        self,
        sku: str,
        marketplace_id: str = 'A1RKKUPIHCS9HS'
    ) -> Tuple[Optional[int], bool]:
        """
        Obtener cantidad actual de un producto
        Útil para validación y comparación

        Args:
            sku: SKU del producto
            marketplace_id: ID del marketplace

        Returns:
            Tuple[Optional[int], bool]: (quantity, success)
        """
        try:
            self.logger.debug(f"Obteniendo quantity actual para {sku}")

            def get_listing():
                listings_api = ListingsItems(credentials=self.credentials)

                response = listings_api.get_listings_item(
                    sellerId=self.seller_id,
                    sku=sku,
                    marketplaceIds=[marketplace_id],
                    includedData=['attributes']
                )
                return response

            # Ejecutar en thread pool
            response = await asyncio.to_thread(get_listing)

            # Extraer quantity del response
            if hasattr(response, 'payload') and response.payload:
                attributes = response.payload.get('attributes', {})
                availability = attributes.get('fulfillment_availability', [])

                if availability and len(availability) > 0:
                    quantity = availability[0].get('quantity', 0)
                    self.logger.debug(
                        f"✅ Quantity actual de {sku}: {quantity}")
                    return quantity, True

            self.logger.warning(f"⚠️ No se pudo obtener quantity para {sku}")
            return None, False

        except SellingApiException as ex:
            if hasattr(ex, 'code') and ex.code == 404:
                self.logger.warning(f"⚠️ SKU {sku} no encontrado en Amazon")
            else:
                self.logger.error(
                    f"❌ Error obteniendo quantity para {sku}: {ex}")
            return None, False

        except Exception as e:
            self.logger.error(
                f"❌ Error inesperado obteniendo quantity {sku}: {e}")
            return None, False

    async def update_quantity_batch(
        self,
        updates: List[Dict],
        batch_size: int = 10,
        delay_between_batches: float = 1.0
    ) -> Dict:
        """
        Actualizar múltiples productos en lotes

        Args:
            updates: Lista de dicts con 'sku' y 'quantity'
            batch_size: Tamaño del lote
            delay_between_batches: Delay en segundos entre lotes

        Returns:
            Dict con resumen de resultados
        """
        results = {
            'total': len(updates),
            'successful': 0,
            'failed': 0,
            'details': []
        }

        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]

            self.logger.info(
                f"📦 Procesando lote {i//batch_size + 1}/{(len(updates)-1)//batch_size + 1}"
            )

            # Procesar lote
            for update in batch:
                sku = update.get('sku')
                quantity = update.get('quantity')
                marketplace_id = update.get('marketplace_id', 'A1RKKUPIHCS9HS')

                result = await self.update_quantity(sku, quantity, marketplace_id)

                if result['success']:
                    results['successful'] += 1
                else:
                    results['failed'] += 1

                results['details'].append(result)

            # Delay entre lotes para rate limiting
            if i + batch_size < len(updates):
                await asyncio.sleep(delay_between_batches)

        self.logger.info(
            f"✅ Batch completado: {results['successful']} éxitos, "
            f"{results['failed']} fallos"
        )

        return results

    async def verify_sku_exists(
        self,
        sku: str,
        marketplace_id: str = 'A1RKKUPIHCS9HS'
    ) -> bool:
        """
        Verificar si un SKU existe en Amazon

        Args:
            sku: SKU a verificar
            marketplace_id: ID del marketplace

        Returns:
            bool: True si existe, False si no
        """
        try:
            def check_listing():
                listings_api = ListingsItems(credentials=self.credentials)

                response = listings_api.get_listings_item(
                    sellerId=self.seller_id,
                    sku=sku,
                    marketplaceIds=[marketplace_id]
                )
                return response

            # Ejecutar en thread pool
            response = await asyncio.to_thread(check_listing)

            # Si no lanza excepción, el SKU existe
            if response and hasattr(response, 'payload'):
                self.logger.debug(f"✅ SKU {sku} existe en Amazon")
                return True

            return False

        except SellingApiException as ex:
            if hasattr(ex, 'code') and ex.code == 404:
                self.logger.debug(f"⚠️ SKU {sku} NO existe en Amazon")
                return False

            # Otros errores - asumir que existe pero hay problema de acceso
            self.logger.warning(f"⚠️ Error verificando {sku}: {ex}")
            return False

        except Exception as e:
            self.logger.error(f"❌ Error inesperado verificando {sku}: {e}")
            return False

    async def health_check(self) -> Dict:
        """
        Verificar salud de la conexión con Listings API

        Returns:
            Dict con estado de salud
        """
        try:
            # Intentar obtener listado de items (primera página)
            def check_connection():
                listings_api = ListingsItems(credentials=self.credentials)

                response = listings_api.get_listings_items(
                    sellerId=self.seller_id,
                    marketplaceIds=['A1RKKUPIHCS9HS'],
                    pageSize=1
                )
                return response

            response = await asyncio.to_thread(check_connection)

            return {
                'status': 'healthy',
                'api': 'ListingsItems',
                'timestamp': asyncio.get_event_loop().time(),
                'message': 'Conexión exitosa'
            }

        except Exception as e:
            self.logger.error(f"❌ Health check failed: {e}")
            return {
                'status': 'unhealthy',
                'api': 'ListingsItems',
                'timestamp': asyncio.get_event_loop().time(),
                'error': str(e)
            }
