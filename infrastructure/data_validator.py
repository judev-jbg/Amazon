"""
Validador de datos refactorizado usando Chain of Responsibility
"""
import logging
import pandas as pd
from typing import List, Tuple

from infrastructure.validation.validation_chain_builder import ValidationChainBuilder
from infrastructure.error_handling import EnhancedErrorHandler


class DataValidator:
    """
    Validador robusto usando cadena de responsabilidad
    Ahora es mucho más simple y componible
    """

    def __init__(self, db_manager):
        EnhancedErrorHandler()
        self.db_manager = db_manager
        self.logger = logging.getLogger("DataValidator")

    async def validate_order_details(
        self,
        df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
        """
        Validar y limpiar datos de OrderDetails

        Returns:
            - df_to_insert: Registros nuevos para insertar
            - df_to_update: Registros existentes para actualizar
            - validation_errors: Lista de errores encontrados
        """
        # 1. Construir cadena de validación
        validation_chain = self._build_order_details_validation_chain()

        # 2. Ejecutar validaciones
        df_clean, validation_errors = await validation_chain.validate(df)

        # 3. Comparar con base de datos existente
        df_to_insert, df_to_update = await self._compare_with_database(df_clean)

        return df_to_insert, df_to_update, validation_errors

    def _build_order_details_validation_chain(self):
        """
        Construir cadena de validación específica para OrderDetails

        Returns:
            Primera regla de la cadena
        """
        return (ValidationChainBuilder()
                .remove_empty_rows()
                .clean_strings()
                .validate_required(['orderId', 'orderItemId', 'purchaseDate'])
                .convert_numeric([
                    'numberOfItems', 'quantityPurchased', 'itemPrice', 'itemTax',
                    'shippingPrice', 'shippingTax', 'isBusinessOrder',
                    'isAmazonInvoiced', 'isBuyerRequestedCancellation'
                ])
                .normalize_dates(['purchaseDate', 'paymentsDate'])
                .generate_keys(['orderId', 'purchaseDate', 'orderItemId'])
                .remove_duplicates()
                .build()
                )

    async def _compare_with_database(
        self,
        df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Comparar con registros existentes en base de datos

        Args:
            df: DataFrame ya validado

        Returns:
            Tupla (df_to_insert, df_to_update)
        """
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        # Obtener registros existentes
        existing_records = await self.db_manager.order_details.get_existing_order_details(
            df['unique_key'].tolist()
        )

        # Determinar qué hacer con cada registro
        existing_keys = set(
            existing_records['unique_key'].tolist()
        ) if not existing_records.empty else set()

        new_keys = set(df['unique_key'].tolist())

        keys_to_insert = new_keys - existing_keys
        keys_to_update = new_keys & existing_keys

        df_to_insert = df[df['unique_key'].isin(keys_to_insert)].copy()
        df_to_update = df[df['unique_key'].isin(keys_to_update)].copy()

        # Para los registros a actualizar, verificar si realmente hay cambios
        if not df_to_update.empty and not existing_records.empty:
            df_to_update = self._filter_actual_changes(
                df_to_update, existing_records)

        self.logger.info(f"Registros para insertar:   {len(df_to_insert)}")
        self.logger.info(f"Registros para actualizar: {len(df_to_update)}")

        return df_to_insert, df_to_update

    def _filter_actual_changes(
        self,
        df_new: pd.DataFrame,
        df_existing: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Filtrar solo registros que realmente han cambiado

        Args:
            df_new: DataFrame con datos nuevos
            df_existing: DataFrame con datos existentes

        Returns:
            DataFrame con solo los registros que cambiaron
        """
        # Campos críticos que pueden cambiar
        change_fields = [
            'isAmazonInvoiced',
            'isBuyerRequestedCancellation',
            'buyerRequestedCancelReason'
        ]

        merged = df_new.merge(
            df_existing,
            on='unique_key',
            suffixes=('_new', '_existing')
        )

        changed_mask = pd.Series([False] * len(merged))

        for field in change_fields:
            new_col = f'{field}_new'
            existing_col = f'{field}_existing'

            if new_col in merged.columns and existing_col in merged.columns:
                # Comparar valores
                both_not_null = ~merged[new_col].isna(
                ) & ~merged[existing_col].isna()
                values_different = merged[new_col] != merged[existing_col]
                one_is_null = merged[new_col].isna(
                ) != merged[existing_col].isna()

                field_changes = (
                    both_not_null & values_different) | one_is_null
                changed_mask |= field_changes

        changed_keys = merged[changed_mask]['unique_key'].tolist()
        return df_new[df_new['unique_key'].isin(changed_keys)]
