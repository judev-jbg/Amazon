"""
Calculador de Precio de Venta Público Mínimo (PVPM)
Formula: pvpm = (((coste / margen) * IVA) + (coste_envío * IVA)) / IVA
"""
from decimal import Decimal
import logging
import config.setting as st

# Configuración de costes de envío por peso
SHIPPING_CONFIG = st.SHIPPING_CONFIG


class PVPMCalculator:
    """Calcula el Precio de Venta Público Mínimo"""

    def __init__(self):
        self.margen = st.PRICING_PARAMS['margen_coste']  # 75%
        self.iva = st.PRICING_PARAMS['iva']              # 21%
        self.logger = logging.getLogger("AmazonManagement")

    def calculate_pvpm(self, coste: float, peso: float) -> float:
        """
        Calcular PVPM

        Args:
            coste: Coste del producto
            peso: Peso en kg

        Returns:
            float: PVPM calculado
        """
        # 1. Calcular coste de envío según peso
        coste_envio = self._calculate_shipping_cost(peso)

        # 2. Aplicar fórmula
        pvpm = (((Decimal(coste) / Decimal(self.margen)) * Decimal(self.iva)) +
                (Decimal(coste_envio) * Decimal(self.iva))) / Decimal(self.iva)

        return round(pvpm, 2)

    def _calculate_shipping_cost(self, peso: float) -> float:
        """
        Calcular coste de envío según peso

        Args:
            peso: Peso en kg

        Returns:
            float: Coste de envío
        """
        # Si peso es 0 o None, usar default
        if not peso or peso <= 0:
            return SHIPPING_CONFIG['default_cost']

        # Buscar en tiers
        for tier in SHIPPING_CONFIG['weight_tiers']:
            if peso <= tier['max_weight']:
                return Decimal(tier['cost'])

        # Más de 20kg: 9.25 + (peso-20) * 0.47
        return (
            Decimal(SHIPPING_CONFIG['over_20kg_base']) +
            Decimal((peso - 20)) * Decimal(SHIPPING_CONFIG['over_20kg_rate'])
        )
