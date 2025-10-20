
"""
Transformador especializado para órdenes de Amazon
Responsabilidad única: Transformar respuestas de API a formato interno
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import config.setting as st


class AmazonOrderTransformer:
    """Transforma datos de órdenes desde Amazon SP-API a formato de BD"""

    def __init__(self, utc_offset_hours: int = None):
        """
        Args:
            utc_offset_hours: Diferencia horaria UTC (default: configuración)
        """
        self.utc_offset_hours = utc_offset_hours or getattr(
            st, 'difHoursUtc', 1)

    def transform_order(self, raw_order: Dict) -> Dict:
        """
        Transformar una orden individual desde formato Amazon API

        Args:
            raw_order: Orden en formato Amazon SP-API

        Returns:
            Orden en formato para base de datos
        """
        return {
            'purchaseDate': self._parse_datetime(raw_order.get('PurchaseDate')),
            'purchaseDateEs': self._parse_datetime_local(raw_order.get('PurchaseDate')),
            'salesChannel': raw_order.get('SalesChannel'),
            'amazonOrderId': raw_order.get('AmazonOrderId'),
            'buyerEmail': self._extract_buyer_email(raw_order),
            'earliestShipDate': self._parse_datetime(raw_order.get('EarliestShipDate')),
            'latestShipDate': self._parse_datetime(raw_order.get('LatestShipDate')),
            'earliestDeliveryDate': self._parse_datetime(raw_order.get('EarliestDeliveryDate')),
            'latestDeliveryDate': self._parse_datetime(raw_order.get('LatestDeliveryDate')),
            'lastUpdateDate': self._parse_datetime(raw_order.get('LastUpdateDate')),
            'isBusinessOrder': raw_order.get('IsBusinessOrder', False),
            'marketplaceId': raw_order.get('MarketplaceId'),
            'numberOfItemsShipped': raw_order.get('NumberOfItemsShipped', 0),
            'numberOfItemsUnshipped': raw_order.get('NumberOfItemsUnshipped', 0),
            'orderStatus': raw_order.get('OrderStatus'),
            'totalOrderCurrencyCode': self._extract_currency_code(raw_order),
            'totalOrderAmount': self._extract_order_amount(raw_order),
            'city': self._extract_shipping_field(raw_order, 'City'),
            'countryCode': self._extract_shipping_field(raw_order, 'CountryCode'),
            'postalCode': self._extract_shipping_field(raw_order, 'PostalCode'),
            'stateOrRegion': self._extract_shipping_field(raw_order, 'StateOrRegion'),
            'loadDate': datetime.now().date(),
            'loadDateTime': datetime.now()
        }

    def transform_orders_batch(self, raw_orders: List[Dict]) -> List[Dict]:
        """
        Transformar múltiples órdenes

        Args:
            raw_orders: Lista de órdenes en formato Amazon

        Returns:
            Lista de órdenes transformadas
        """
        return [self.transform_order(order) for order in raw_orders]

    def _parse_datetime(self, dt_str: Optional[str]) -> Optional[str]:
        """
        Parser para fechas de Amazon - formato UTC

        Args:
            dt_str: String de fecha en formato ISO (e.g., "2024-01-15T10:30:00Z")

        Returns:
            Fecha formateada sin timezone o None
        """
        if not dt_str:
            return None

        # Remover 'T' y 'Z' para formato MySQL
        return dt_str.replace('T', ' ').replace('Z', '')

    def _parse_datetime_local(self, dt_str: Optional[str]) -> Optional[datetime]:
        """
        Parser para fechas con conversión a timezone local

        Args:
            dt_str: String de fecha en formato ISO

        Returns:
            Datetime ajustado a timezone local
        """
        if not dt_str:
            return None

        # Parsear y ajustar por timezone
        clean_str = dt_str.replace('T', ' ').replace('Z', '')
        utc_dt = datetime.strptime(clean_str, '%Y-%m-%d %H:%M:%S')
        local_dt = utc_dt + timedelta(hours=self.utc_offset_hours)

        return local_dt

    def _extract_buyer_email(self, order: Dict) -> Optional[str]:
        """Extraer email del comprador de forma segura"""
        buyer_info = order.get('BuyerInfo', {})
        return buyer_info.get('BuyerEmail') if buyer_info else None

    def _extract_currency_code(self, order: Dict) -> str:
        """Extraer código de moneda del total de la orden"""
        order_total = order.get('OrderTotal', {})
        return order_total.get('CurrencyCode', 'S/D') if order_total else 'S/D'

    def _extract_order_amount(self, order: Dict) -> float:
        """Extraer monto total de la orden"""
        order_total = order.get('OrderTotal', {})
        return float(order_total.get('Amount', 0)) if order_total else 0.0

    def _extract_shipping_field(self, order: Dict, field_name: str) -> str:
        """
        Extraer campo de dirección de envío de forma segura

        Args:
            order: Orden completa
            field_name: Nombre del campo a extraer (City, CountryCode, etc.)

        Returns:
            Valor del campo o "S/D" si no existe
        """
        shipping_address = order.get('ShippingAddress', {})
        return shipping_address.get(field_name, 'S/D') if shipping_address else 'S/D'
