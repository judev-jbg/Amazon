import logging
import pandas as pd
from typing import List, Tuple
from infrastructure.error_handling import EnhancedErrorHandler


class DataValidator:
    """Validador robusto para evitar duplicados y problemas de formato"""
    
    def __init__(self, db_manager):
        EnhancedErrorHandler()
        self.db_manager = db_manager
        self.logger = logging.getLogger("AmazonManagement")
        
    async def validate_order_details(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
        """
        Validar y limpiar datos de OrderDetails
        
        Returns:
            - df_to_insert: Registros nuevos para insertar
            - df_to_update: Registros existentes para actualizar
            - validation_errors: Lista de errores encontrados
        """
        validation_errors = []
        
        # 1. Limpiar y normalizar datos
        df_clean = self._clean_dataframe(df)
        
        # 2. Validar campos obligatorios
        df_clean, missing_errors = self._validate_required_fields(df_clean)
        validation_errors.extend(missing_errors)
        
        # 3. Normalizar fechas (problema principal con Google Sheets)
        df_clean, date_errors = self._normalize_dates(df_clean)
        validation_errors.extend(date_errors)
        
        # 4. Generar claves únicas
        df_clean = self._generate_unique_keys(df_clean)
        
        # 5. Detectar duplicados internos en el archivo
        df_clean, duplicate_errors = self._remove_internal_duplicates(df_clean)
        validation_errors.extend(duplicate_errors)
        
        # 6. Comparar con base de datos existente
        df_to_insert, df_to_update = await self._compare_with_database(df_clean)
        
        return df_to_insert, df_to_update, validation_errors
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpiar datos del dataframe"""
        df_clean = df.copy()
        
        # Remover filas completamente vacías
        df_clean = df_clean.dropna(how='all')
        
        # Limpiar strings
        string_columns = df_clean.select_dtypes(include=['object']).columns
        for col in string_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str).str.strip()
                df_clean[col] = df_clean[col].replace(['nan', 'NaN', 'None', ''], None)
        
        # Limpiar números
        numeric_columns = ['numberOfItems', 'quantityPurchased', 'itemPrice', 'itemTax', 
                          'shippingPrice', 'shippingTax', 'isBusinessOrder', 'isAmazonInvoiced', 
                          'isBuyerRequestedCancellation']
        for col in numeric_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        
        return df_clean
    
    def _validate_required_fields(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Validar campos obligatorios"""
        required_fields = ['orderId', 'orderItemId', 'purchaseDate']
        errors = []
        
        for field in required_fields:
            if field not in df.columns:
                errors.append(f"Campo obligatorio faltante: {field}")
                continue
                
            null_count = df[field].isnull().sum()
            if null_count > 0:
                errors.append(f"Campo {field} tiene {null_count} valores nulos")
        
        # Filtrar filas con campos obligatorios nulos
        for field in required_fields:
            if field in df.columns:
                df = df[df[field].notna()]
        
        return df, errors
    
    def _normalize_dates(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Normalizar fechas - CRÍTICO para evitar duplicados"""
        date_columns = ['purchaseDate','paymentsDate']
        errors = []
        
        for col in date_columns:
            if col not in df.columns:
                continue
                
            try:
                original_col = df[col].copy()
                mask_empty = original_col.isnull() | (original_col == '') | (original_col.astype(str).str.strip() == '')
                mask_valid = ~mask_empty
                if mask_valid.sum() == 0:  # Si no hay valores válidos, continuar
                    continue
                df[col] = pd.NaT
                valid_values = original_col[mask_valid]

                # Intentar múltiples formatos
                converted_dates = pd.to_datetime(valid_values, errors='coerce')
                if converted_dates.dt.tz is not None:
                    converted_dates = converted_dates.dt.tz_localize(None)
                mask_still_null = converted_dates.isnull()
                
                # Si hay errores, intentar formatos específicos
                if mask_still_null.any() and (col + '_original') in df.columns:
                    # Formato Excel
                    excel_values = df.loc[mask_valid, col + '_original'][mask_still_null]
                    excel_converted = pd.to_datetime(excel_values, format='%Y-%m-%d %H:%M:%S', errors='coerce')
                    converted_dates.loc[mask_still_null] = excel_converted
                    
                    # Formato Google Sheets para los que aún no se convirtieron
                    mask_still_null = converted_dates.isnull()
                    if mask_still_null.any():
                        sheets_values = df.loc[mask_valid, col + '_original'][mask_still_null]
                        sheets_converted = pd.to_datetime(sheets_values, format='%m/%d/%Y %H:%M:%S', errors='coerce')
                        converted_dates.loc[mask_still_null] = sheets_converted
                
                # Asignar las fechas convertidas solo a las posiciones válidas
                df.loc[mask_valid, col] = converted_dates
                
                # Convertir solo los valores no nulos a formato estándar
                mask_converted = df[col].notna()
                if mask_converted.any():
                    df.loc[mask_converted, col] = df.loc[mask_converted, col].dt.strftime('%Y-%m-%d %H:%M:%S')
                
            except Exception as e:
                errors.append(f"Error normalizando fecha en columna {col}: {str(e)}")
        
        return df, errors
    
    def _generate_unique_keys(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generar claves únicas para cada registro"""
        df['unique_key'] = (
            df['orderId'].astype(str) + '|' + 
            df['purchaseDate'].astype(str) + '|' + 
            df['orderItemId'].astype(str)
        )
        return df
    
    def _remove_internal_duplicates(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Remover duplicados dentro del mismo archivo"""
        errors = []
        initial_count = len(df)
        
        # Detectar duplicados
        duplicate_mask = df.duplicated(subset=['unique_key'], keep='first')
        duplicate_count = duplicate_mask.sum()
        
        if duplicate_count > 0:
            errors.append(f"Se encontraron {duplicate_count} duplicados internos, se mantendrá el primero")
            df = df[~duplicate_mask]
        
        return df, errors
    
    async def _compare_with_database(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Comparar con registros existentes en base de datos"""
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()
        
        # Obtener registros existentes
        existing_records = await self.db_manager.get_existing_order_details(
            df['unique_key'].tolist()
        )
        
        existing_keys = set(existing_records['unique_key'].tolist()) if not existing_records.empty else set()
        new_keys = set(df['unique_key'].tolist())
        
        # Separar registros nuevos vs existentes
        keys_to_insert = new_keys - existing_keys
        keys_to_update = new_keys & existing_keys
        
        df_to_insert = df[df['unique_key'].isin(keys_to_insert)].copy()
        df_to_update = df[df['unique_key'].isin(keys_to_update)].copy()
        
        # Para los registros a actualizar, comparar si realmente hay cambios
        if not df_to_update.empty and not existing_records.empty:
            df_to_update = self._filter_actual_changes(df_to_update, existing_records)

        self.logger.info(f"Registros para insertar:     {len(df_to_insert)}")
        self.logger.info(f"Registros para actualizar:   {len(df_to_update)}")
        
        return df_to_insert, df_to_update
    
    def _filter_actual_changes(self, df_new: pd.DataFrame, df_existing: pd.DataFrame) -> pd.DataFrame:
        """Filtrar solo registros que realmente han cambiado"""
        # Comparar campos críticos que pueden cambiar
        change_fields = ['isAmazonInvoiced', 'isBuyerRequestedCancellation', 'buyerRequestedCancelReason']
        
        merged = df_new.merge(df_existing, on='unique_key', suffixes=('_new', '_existing'))
        
        changed_mask = pd.Series([False] * len(merged))
        changes_detail = []

        for field in change_fields:
            if f'{field}_new' in merged.columns and f'{field}_existing' in merged.columns:
                new_col = merged[f'{field}_new']
                existing_col = merged[f'{field}_existing']

                both_not_null = ~new_col.isna() & ~existing_col.isna()
                values_different = new_col != existing_col
                one_is_null = new_col.isna() != existing_col.isna()
                
                field_changes = (both_not_null & values_different) | one_is_null
                
                changed_mask |= field_changes

                if field_changes.any():
                    changed_rows = merged[field_changes]
                    for _, row in changed_rows.iterrows():
                        changes_detail.append({
                            'unique_key': row['unique_key'],
                            'field': field,
                            'old_value': row[f'{field}_existing'],
                            'new_value': row[f'{field}_new'],
                            'old_is_null': pd.isna(row[f'{field}_existing']),
                            'new_is_null': pd.isna(row[f'{field}_new'])
                        })

        if changes_detail:
            print(f"\n[DEBUG] Se encontraron {len(changes_detail)} cambios:")
            print("-" * 80)
            
            # Agrupar por unique_key para mejor visualización
            from collections import defaultdict
            changes_by_key = defaultdict(list)
            for change in changes_detail:
                changes_by_key[change['unique_key']].append(change)
            
            for unique_key, key_changes in changes_by_key.items():
                print(f"\n📌 unique_key: {unique_key}")
                for change in key_changes:
                    old_display = 'NULL' if change['old_is_null'] else change['old_value']
                    new_display = 'NULL' if change['new_is_null'] else change['new_value']
                    print(f"   • Campo '{change['field']}':")
                    print(f"     - Valor anterior: {old_display}")
                    print(f"     - Valor nuevo:    {new_display}")
            
        changed_keys = merged[changed_mask]['unique_key'].tolist()
        return df_new[df_new['unique_key'].isin(changed_keys)]