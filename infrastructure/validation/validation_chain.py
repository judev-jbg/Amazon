"""
Chain of Responsibility pattern para validación de datos
Cada regla es independiente y puede ser reutilizada
"""
import logging
import pandas as pd
from abc import ABC, abstractmethod
from typing import List, Tuple, Optional


class ValidationRule(ABC):
    """Regla de validación base usando Chain of Responsibility"""

    def __init__(self, name: str = None):
        self.next_rule: Optional['ValidationRule'] = None
        self.name = name or self.__class__.__name__
        self.logger = logging.getLogger(self.name)

    def set_next(self, rule: 'ValidationRule') -> 'ValidationRule':
        """
        Establecer siguiente regla en la cadena

        Args:
            rule: Siguiente regla a ejecutar

        Returns:
            La regla pasada (para encadenar llamadas)
        """
        self.next_rule = rule
        return rule

    async def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """
        Ejecutar validación y pasar al siguiente si existe

        Args:
            df: DataFrame a validar

        Returns:
            Tupla (DataFrame validado, lista de errores acumulados)
        """
        # Ejecutar validación específica
        df_validated, errors = await self._validate_impl(df)

        # Log errores encontrados
        if errors:
            for error in errors:
                self.logger.warning(f"[{self.name}] {error}")

        # Pasar al siguiente en la cadena
        if self.next_rule:
            df_next, next_errors = await self.next_rule.validate(df_validated)
            return df_next, errors + next_errors

        return df_validated, errors

    @abstractmethod
    async def _validate_impl(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """
        Implementación específica de la validación
        Debe ser implementada por cada regla concreta
        """
        pass


class RemoveEmptyRowsRule(ValidationRule):
    """Regla: Remover filas completamente vacías"""

    async def _validate_impl(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        initial_count = len(df)
        df_clean = df.dropna(how='all')
        removed_count = initial_count - len(df_clean)

        errors = []
        if removed_count > 0:
            errors.append(
                f"Se removieron {removed_count} filas completamente vacías")

        return df_clean, errors


class CleanStringFieldsRule(ValidationRule):
    """Regla: Limpiar campos de tipo string"""

    async def _validate_impl(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        df_clean = df.copy()

        # Identificar columnas string
        string_columns = df_clean.select_dtypes(include=['object']).columns

        for col in string_columns:
            if col in df_clean.columns:
                # Strip whitespace
                df_clean[col] = df_clean[col].astype(str).str.strip()
                # Reemplazar valores tipo "nan"
                df_clean[col] = df_clean[col].replace(
                    ['nan', 'NaN', 'None', ''], None)

        return df_clean, []


class ValidateRequiredFieldsRule(ValidationRule):
    """Regla: Validar que existan campos obligatorios"""

    def __init__(self, required_fields: List[str]):
        super().__init__()
        self.required_fields = required_fields

    async def _validate_impl(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        errors = []

        # Verificar columnas faltantes
        for field in self.required_fields:
            if field not in df.columns:
                errors.append(f"Campo obligatorio faltante: {field}")
                continue

            # Verificar valores nulos
            null_count = df[field].isnull().sum()
            if null_count > 0:
                errors.append(
                    f"Campo {field} tiene {null_count} valores nulos")

        # Filtrar filas con campos obligatorios nulos
        available_required = [
            f for f in self.required_fields if f in df.columns]
        df_clean = df.dropna(subset=available_required)

        return df_clean, errors


class NormalizeDateFieldsRule(ValidationRule):
    """Regla: Normalizar campos de fecha"""

    def __init__(self, date_columns: List[str]):
        super().__init__()
        self.date_columns = date_columns

    async def _validate_impl(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        df_clean = df.copy()
        errors = []

        for col in self.date_columns:
            if col not in df_clean.columns:
                continue

            try:
                # Identificar valores vacíos
                original_col = df_clean[col].copy()
                mask_empty = (
                    original_col.isnull() |
                    (original_col == '') |
                    (original_col.astype(str).str.strip() == '')
                )
                mask_valid = ~mask_empty

                if mask_valid.sum() == 0:
                    continue

                # Inicializar columna con NaT
                df_clean[col] = pd.NaT
                valid_values = original_col[mask_valid]

                # Intentar conversión
                converted_dates = pd.to_datetime(valid_values, errors='coerce')

                # Remover timezone si existe
                if converted_dates.dt.tz is not None:
                    converted_dates = converted_dates.dt.tz_localize(None)

                # Asignar valores convertidos
                df_clean.loc[mask_valid, col] = converted_dates

                # Convertir a string para MySQL
                mask_converted = df_clean[col].notna()
                if mask_converted.any():
                    df_clean.loc[mask_converted, col] = df_clean.loc[
                        mask_converted, col
                    ].dt.strftime('%Y-%m-%d %H:%M:%S')

                # Contar errores de conversión
                failed_count = converted_dates.isnull().sum()
                if failed_count > 0:
                    errors.append(
                        f"Columna {col}: {failed_count} fechas no pudieron ser convertidas"
                    )

            except Exception as e:
                errors.append(
                    f"Error normalizando fecha en columna {col}: {str(e)}")

        return df_clean, errors


class GenerateUniqueKeysRule(ValidationRule):
    """Regla: Generar claves únicas para deduplicación"""

    def __init__(self, key_fields: List[str], key_column_name: str = 'unique_key'):
        super().__init__()
        self.key_fields = key_fields
        self.key_column_name = key_column_name

    async def _validate_impl(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        df_clean = df.copy()

        # Generar clave única concatenando campos
        df_clean[self.key_column_name] = (
            df_clean[self.key_fields[0]].astype(str)
        )

        for field in self.key_fields[1:]:
            df_clean[self.key_column_name] += '|' + df_clean[field].astype(str)

        return df_clean, []


class RemoveInternalDuplicatesRule(ValidationRule):
    """Regla: Remover duplicados dentro del mismo DataFrame"""

    def __init__(self, unique_key_column: str = 'unique_key', keep: str = 'first'):
        super().__init__()
        self.unique_key_column = unique_key_column
        self.keep = keep

    async def _validate_impl(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        initial_count = len(df)

        # Detectar duplicados
        duplicate_mask = df.duplicated(
            subset=[self.unique_key_column], keep=self.keep)
        duplicate_count = duplicate_mask.sum()

        errors = []
        if duplicate_count > 0:
            errors.append(
                f"Se encontraron {duplicate_count} duplicados internos, "
                f"se mantendrá el {self.keep}"
            )
            df_clean = df[~duplicate_mask]
        else:
            df_clean = df

        return df_clean, errors


class ConvertNumericFieldsRule(ValidationRule):
    """Regla: Convertir campos numéricos"""

    def __init__(self, numeric_columns: List[str]):
        super().__init__()
        self.numeric_columns = numeric_columns

    async def _validate_impl(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        df_clean = df.copy()
        errors = []

        for col in self.numeric_columns:
            if col not in df_clean.columns:
                continue

            try:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            except Exception as e:
                errors.append(f"Error convirtiendo {col} a numérico: {str(e)}")

        return df_clean, errors
