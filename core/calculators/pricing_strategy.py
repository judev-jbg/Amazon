"""
Estrategia de pricing competitivo
Objetivos:
1. Ganar buybox
2. Mantener 3 EUR diferencia (si PVPM lo permite)
3. Nunca por debajo de PVPM
"""
from decimal import Decimal
import logging
from typing import Dict, List, Optional

import config.setting as st


class PricingStrategyCalculator:
    """
    Calcula el precio óptimo basado en competencia y restricciones
    """

    def __init__(self):
        self.logger = logging.getLogger("AmazonManagement")

    # EUR de diferencia deseada con competencia
    TARGET_MARGIN = st.PRICING_PARAMS['target_margin_eur']

    def calculate_optimal_price(
        self,
        pvpm: float,
        current_price: float,
        buybox_price: Optional[float],
        competitors: List[float]
    ) -> Dict:
        """
        Calcular precio óptimo y acción recomendada

        Args:
            pvpm: Precio mínimo de venta
            current_price: Precio actual
            buybox_price: Precio del actual poseedor del buybox
            competitors: Lista de precios de competidores

        Returns:
            Dict: {
                'action': 'keep'|'win_buybox'|'lower_price'|'critical_below_pvpm',
                'new_price': float,
                'reason': str,
                'savings': float
            }
        """
        try:

            # CRÍTICO: Verificar si estamos por debajo de PVPM
            if Decimal(current_price) < Decimal(pvpm):
                return {
                    'action': 'critical_below_pvpm',
                    'new_price': pvpm,
                    'reason': 'Precio actual por debajo de PVPM',
                    'savings': None
                }

            # Filtrar competidores válidos (>= PVPM)
            valid_competitors = [
                p for p in competitors if Decimal(p) >= Decimal(pvpm)]

            if not valid_competitors:
                # No hay competencia válida
                return {
                    'action': 'keep',
                    'new_price': None,
                    'reason': 'Sin competencia válida en rango de PVPM',
                    'savings': None
                }

            lowest_competitor = min(valid_competitors)

            # ESTRATEGIA 1: ¿Podemos ganar el buybox?
            if buybox_price and Decimal(buybox_price) >= Decimal(pvpm):
                # Calcular precio ideal: buybox - 3 EUR
                ideal_price = Decimal(buybox_price) - self.TARGET_MARGIN

                # Verificar restricción de PVPM
                if ideal_price >= Decimal(pvpm):
                    # Podemos aplicar diferencia de 3 EUR
                    new_price = round(ideal_price, 2)

                    if new_price < current_price:
                        return {
                            'action': 'win_buybox',
                            'new_price': new_price,
                            'reason': f'Ganar buybox con {self.TARGET_MARGIN} EUR diferencia',
                            'savings': round(Decimal(current_price) - Decimal(new_price), 2)
                        }
                else:
                    # No podemos mantener 3 EUR, pero podemos ganar buybox
                    # Precio: justo por debajo de buybox pero >= PVPM
                    new_price = max(Decimal(pvpm), Decimal(
                        buybox_price) - Decimal(0.01))
                    new_price = round(new_price, 2)

                    if new_price < current_price:
                        return {
                            'action': 'win_buybox',
                            'new_price': new_price,
                            'reason': 'Ganar buybox (PVPM no permite 3 EUR diferencia)',
                            'savings': round(Decimal(current_price) - Decimal(new_price), 2)
                        }

            # ESTRATEGIA 2: ¿Podemos bajar precio sin ganar buybox?
            # Objetivo: Ser más competitivos que el precio actual
            if Decimal(lowest_competitor) < Decimal(current_price):
                # Intentar bajar hasta el competidor más bajo - pequeño margen
                target_price = Decimal(lowest_competitor) - Decimal(0.01)

                # Verificar PVPM
                if target_price >= Decimal(pvpm):
                    return {
                        'action': 'lower_price',
                        'new_price': round(target_price, 2),
                        'reason': 'Reducir precio para ser más competitivo',
                        'savings': round(current_price - target_price, 2)
                    }

            # ESTRATEGIA 3: Mantener precio actual
            return {
                'action': 'keep',
                'new_price': None,
                'reason': 'Precio actual es óptimo',
                'savings': None
            }
        except Exception as e:
            # Manejo de errores genéricos
            self.logger.error(f"Error calculando estrategia de pricing: {e}")
            return {
                'action': 'error',
                'new_price': None,
                'reason': f'Error en cálculo: {str(e)}',
                'savings': None
            }
