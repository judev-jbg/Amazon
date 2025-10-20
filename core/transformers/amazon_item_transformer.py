"""
Transformador especializado para items de órdenes
"""
from datetime import datetime
from typing import Dict, List, Optional


class AmazonItemTransformer:
    """Transforma items de órdenes desde Amazon SP-API"""

    def transform_order_item(self, raw_item: Dict, order_id: str) -> Dict:
        """
        Transformar un item individual de orden

        Args:
            raw_item: Item en formato Amazon API
            order_id: ID de la orden a la que pertenece

        Returns:
            Item transformado para BD
        """
        return {
            'orderId': order_id,
            'orderItemId': raw_item.get('OrderItemId'),
            'asin': raw_item.get('ASIN'),
            'sku': raw_item.get('SellerSKU'),
            'title': raw_item.get('Title'),
            'conditionItem': raw_item.get('ConditionId'),
            'nItems': self._extract_number_of_items(raw_item),
            'qOrdered': raw_item.get('QuantityOrdered', 0),
            'qShipped': raw_item.get('QuantityShipped', 0),
            'reasonCancel': self._extract_cancel_reason(raw_item),
            'isRequestedCancel': self._extract_cancel_flag(raw_item),
            'itemPriceCurrencyCode': self._extract_price_currency(raw_item),
            'itemPriceCurrencyAmount': self._extract_price_amount(raw_item),
            'itemTaxCurrencyCode': self._extract_tax_currency(raw_item),
            'itemTaxCurrencyAmount': self._extract_tax_amount(raw_item),
            'loadDate': datetime.now().date(),
            'loadDateTime': datetime.now()
        }

    def transform_order_items_batch(self, raw_items: List[Dict], order_id: str) -> List[Dict]:
        """
        Transformar múltiples items de una orden

        Args:
            raw_items: Lista de items en formato Amazon
            order_id: ID de la orden

        Returns:
            Lista de items transformados
        """
        return [self.transform_order_item(item, order_id) for item in raw_items]

    def _extract_number_of_items(self, item: Dict) -> int:
        """Extraer número de items del ProductInfo"""
        product_info = item.get('ProductInfo', {})
        return product_info.get('NumberOfItems', 1) if product_info else 1

    def _extract_cancel_reason(self, item: Dict) -> str:
        """Extraer razón de cancelación si existe"""
        buyer_cancel = item.get('BuyerRequestedCancel', {})
        return buyer_cancel.get('BuyerCancelReason', 'S/D') if buyer_cancel else 'S/D'

    def _extract_cancel_flag(self, item: Dict) -> int:
        """Extraer flag de cancelación solicitada"""
        buyer_cancel = item.get('BuyerRequestedCancel', {})
        if not buyer_cancel:
            return 0

        is_cancelled = buyer_cancel.get('IsBuyerRequestedCancel', 'false')
        # Convertir string boolean a int
        return int(str(is_cancelled).replace('false', '0').replace('true', '1'))

    def _extract_price_currency(self, item: Dict) -> str:
        """Extraer código de moneda del precio"""
        item_price = item.get('ItemPrice', {})
        return item_price.get('CurrencyCode', 'S/D') if item_price else 'S/D'

    def _extract_price_amount(self, item: Dict) -> float:
        """Extraer monto del precio"""
        item_price = item.get('ItemPrice', {})
        return float(item_price.get('Amount', 0)) if item_price else 0.0

    def _extract_tax_currency(self, item: Dict) -> str:
        """Extraer código de moneda del impuesto"""
        item_tax = item.get('ItemTax', {})
        return item_tax.get('CurrencyCode', 'S/D') if item_tax else 'S/D'

    def _extract_tax_amount(self, item: Dict) -> float:
        """Extraer monto del impuesto"""
        item_tax = item.get('ItemTax', {})
        return float(item_tax.get('Amount', 0)) if item_tax else 0.0
