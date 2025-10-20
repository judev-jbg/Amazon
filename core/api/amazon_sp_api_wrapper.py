
"""
Wrapper limpio para llamadas a Amazon SP-API
Separa la lógica de API calls de la transformación de datos
"""
import logging
from typing import List, Dict, Tuple, Optional
from sp_api.api import Orders, Sales
from sp_api.base import SellingApiException, Granularity
from sp_api.util import throttle_retry, load_all_pages

import config.setting as st
from core.transformers.amazon_order_transformer import AmazonOrderTransformer
from core.transformers.amazon_item_transformer import AmazonItemTransformer
from core.transformers.amazon_sales_transformer import AmazonSalesTransformer


class AmazonSPAPIWrapper:
    """
    Wrapper para Amazon SP-API que encapsula llamadas y maneja errores.
    Solo responsabilidad: Hacer llamadas HTTP y retornar DataFrames/Dicts sin procesar.
    """

    def __init__(self):
        self.credentials = self._load_credentials()
        self.logger = logging.getLogger(self.__class__.__name__)

        # Transformadores
        self.order_transformer = AmazonOrderTransformer()
        self.item_transformer = AmazonItemTransformer()
        self.sales_transformer = AmazonSalesTransformer()

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

    def get_orders(
        self,
        date_from: str,
        date_to: str,
        markets: List[str]
    ) -> Tuple[List[Dict], bool]:
        """
        Obtener órdenes con paginación automática

        Args:
            date_from: Fecha inicio ISO format
            date_to: Fecha fin ISO format
            markets: Lista de marketplace IDs

        Returns:
            Tupla (lista de órdenes transformadas, success flag)
        """
        try:
            self.logger.debug(f"Fetching orders from {date_from} to {date_to}")

            raw_orders = []

            @throttle_retry()
            @load_all_pages()
            def load_all_orders(**kwargs):
                return Orders(credentials=self.credentials).get_orders(**kwargs)

            # Obtener todas las páginas
            for page in load_all_orders(
                CreatedAfter=date_from,
                CreatedBefore=date_to,
                MarketplaceIds=markets
            ):
                orders = getattr(page, 'payload', {}).get("Orders", [])
                if orders:
                    raw_orders.extend(orders)
                    self.logger.debug(
                        f"Fetched {len(orders)} orders from page")

            # Transformar a formato interno
            transformed = self.order_transformer.transform_orders_batch(
                raw_orders)

            self.logger.info(f"Successfully fetched {len(transformed)} orders")
            return transformed, True

        except SellingApiException as ex:
            self.logger.error(f"Amazon API error: {ex}")

            # Retornar código de error si es rate limit
            if hasattr(ex, 'code') and ex.code == 429:
                return [{'code': 429}], False

            return [], False

        except Exception as e:
            self.logger.error(f"Unexpected error fetching orders: {e}")
            return [], False

    def get_order(self, order_id: str) -> Tuple[Optional[Dict], bool]:
        """
        Obtener una orden específica

        Args:
            order_id: ID de la orden

        Returns:
            Tupla (orden transformada, success flag)
        """
        try:
            self.logger.debug(f"Fetching order {order_id}")

            response = Orders(credentials=self.credentials).get_order(order_id)
            raw_order = response.payload

            # Transformar
            transformed = self.order_transformer.transform_order(raw_order)

            self.logger.info(f"Successfully fetched order {order_id}")
            return transformed, True

        except SellingApiException as ex:
            self.logger.error(f"Amazon API error for order {order_id}: {ex}")

            if hasattr(ex, 'code') and ex.code == 429:
                return {'code': 429}, False

            return None, False

        except Exception as e:
            self.logger.error(f"Error fetching order {order_id}: {e}")
            return None, False

    def get_order_items(self, order_id: str) -> Tuple[List[Dict], bool]:
        """
        Obtener items de una orden

        Args:
            order_id: ID de la orden

        Returns:
            Tupla (lista de items transformados, success flag)
        """
        try:
            self.logger.debug(f"Fetching items for order {order_id}")

            response = Orders(
                credentials=self.credentials).get_order_items(order_id)
            raw_items = response.payload.get("OrderItems", [])

            # Transformar
            transformed = self.item_transformer.transform_order_items_batch(
                raw_items,
                order_id
            )

            self.logger.info(
                f"Fetched {len(transformed)} items for order {order_id}")
            return transformed, True

        except SellingApiException as ex:
            self.logger.error(
                f"Amazon API error for order items {order_id}: {ex}")

            if hasattr(ex, 'code') and ex.code == 429:
                return [{'code': 429}], False

            return [], False

        except Exception as e:
            self.logger.error(f"Error fetching order items {order_id}: {e}")
            return [], False

    def get_sales(
        self,
        asin: str,
        sku: str,
        marketplace: str,
        interval: Tuple[str, str]
    ) -> Tuple[List[Dict], bool]:
        """
        Obtener métricas de ventas

        Args:
            asin: ASIN del producto
            sku: SKU del producto
            marketplace: ID del marketplace
            interval: Tupla (fecha_inicio, fecha_fin)

        Returns:
            Tupla (lista de métricas transformadas, success flag)
        """
        try:
            self.logger.debug(f"Fetching sales for {asin}/{sku}")

            response = Sales(credentials=self.credentials).get_order_metrics(
                interval=interval,
                granularity=Granularity.HOUR,
                asin=asin,
                marketplaceIds=[marketplace]
            )

            raw_metrics = response.payload

            # Transformar
            transformed = self.sales_transformer.transform_sales_batch(
                raw_metrics,
                asin,
                sku,
                marketplace
            )

            self.logger.info(
                f"Fetched {len(transformed)} sales metrics for {asin}")
            return transformed, True

        except SellingApiException as ex:
            self.logger.error(f"Amazon API error for sales {asin}: {ex}")

            if hasattr(ex, 'code') and ex.code == 429:
                return [{'code': 429}], False

            return [], False

        except Exception as e:
            self.logger.error(f"Error fetching sales {asin}: {e}")
            return [], False
