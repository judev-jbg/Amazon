"""
Transformador especializado para datos de ventas
"""
from datetime import datetime, timedelta
from typing import Dict, List
import config.setting as st


class AmazonSalesTransformer:
    """Transforma datos de ventas desde Amazon SP-API"""

    def __init__(self, utc_offset_hours: int = None):
        self.utc_offset_hours = utc_offset_hours or getattr(
            st, 'difHoursUtc', 1)

    def transform_sale_metric(
        self,
        raw_metric: Dict,
        asin: str,
        sku: str,
        marketplace_id: str
    ) -> Dict:
        """
        Transformar una métrica individual de ventas

        Args:
            raw_metric: Métrica en formato Amazon API
            asin: ASIN del producto
            sku: SKU del producto
            marketplace_id: ID del marketplace

        Returns:
            Métrica transformada para BD
        """
        interval = raw_metric.get('interval', '')

        return {
            'asin': asin,
            'sku': sku,
            'marketplaceId': marketplace_id,
            'saleDateTime': self._parse_sale_datetime(interval),
            'saleDate': self._extract_sale_date(interval),
            'intervalHour': self._extract_hour(interval),
            'saleDateEs': self._parse_sale_date_local(interval),
            'intervalHourEs': self._extract_hour_local(interval),
            'qOrders': raw_metric.get('orderCount', 0),
            'avgPriceUndCurrencyCode': self._extract_avg_price_currency(raw_metric),
            'avgPriceUndAmount': self._extract_avg_price_amount(raw_metric),
            'undSold': raw_metric.get('unitCount', 0),
            'totalPriceSoldCurrencyCode': self._extract_total_sales_currency(raw_metric),
            'totalPriceSoldAmount': self._extract_total_sales_amount(raw_metric),
            'loadDate': datetime.now().date(),
            'loadDateTime': datetime.now()
        }

    def transform_sales_batch(
        self,
        raw_metrics: List[Dict],
        asin: str,
        sku: str,
        marketplace_id: str
    ) -> List[Dict]:
        """
        Transformar múltiples métricas de ventas
        Solo incluye métricas con ventas (unitCount > 0)
        """
        return [
            self.transform_sale_metric(metric, asin, sku, marketplace_id)
            for metric in raw_metrics
            if metric.get('unitCount', 0) > 0
        ]

    def _parse_sale_datetime(self, interval: str) -> str:
        """
        Parsear datetime de ventas (formato: "2024-01-15T10:00:00Z")

        Returns:
            Datetime en formato "YYYY-MM-DD HH:MM"
        """
        if not interval:
            return ''

        # Extraer hasta los minutos
        clean = interval[:16].replace('T', ' ')
        return clean

    def _extract_sale_date(self, interval: str) -> str:
        """Extraer solo la fecha (YYYY-MM-DD)"""
        if not interval:
            return ''
        return interval[:10]

    def _extract_hour(self, interval: str) -> str:
        """Extraer solo la hora (HH:MM)"""
        if not interval or len(interval) < 16:
            return ''
        return interval[11:16]

    def _parse_sale_date_local(self, interval: str) -> str:
        """Parsear fecha ajustada a timezone local"""
        if not interval:
            return ''

        try:
            # Parsear y ajustar
            clean = interval[:16].replace('T', ' ')
            utc_dt = datetime.strptime(clean, '%Y-%m-%d %H:%M')
            local_dt = utc_dt + timedelta(hours=self.utc_offset_hours)
            return local_dt.strftime('%Y-%m-%d')
        except:
            return ''

    def _extract_hour_local(self, interval: str) -> str:
        """Extraer hora ajustada a timezone local"""
        if not interval:
            return ''

        try:
            clean = interval[:16].replace('T', ' ')
            utc_dt = datetime.strptime(clean, '%Y-%m-%d %H:%M')
            local_dt = utc_dt + timedelta(hours=self.utc_offset_hours)
            return local_dt.strftime('%H:%M')
        except:
            return ''

    def _extract_avg_price_currency(self, metric: Dict) -> str:
        """Extraer código de moneda del precio promedio"""
        avg_price = metric.get('averageUnitPrice', {})
        return avg_price.get('currencyCode', 'S/D') if avg_price else 'S/D'

    def _extract_avg_price_amount(self, metric: Dict) -> float:
        """Extraer monto del precio promedio"""
        avg_price = metric.get('averageUnitPrice', {})
        return float(avg_price.get('amount', 0)) if avg_price else 0.0

    def _extract_total_sales_currency(self, metric: Dict) -> str:
        """Extraer código de moneda de ventas totales"""
        total_sales = metric.get('totalSales', {})
        return total_sales.get('currencyCode', 'S/D') if total_sales else 'S/D'

    def _extract_total_sales_amount(self, metric: Dict) -> float:
        """Extraer monto de ventas totales"""
        total_sales = metric.get('totalSales', {})
        return float(total_sales.get('amount', 0)) if total_sales else 0.0
