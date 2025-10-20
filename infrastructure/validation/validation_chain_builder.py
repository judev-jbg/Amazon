# infrastructure/validation/validation_chain_builder.py
"""
Builder para construir cadenas de validación fácilmente
"""
from typing import List
from infrastructure.validation.validation_chain import (
    ValidationRule,
    RemoveEmptyRowsRule,
    CleanStringFieldsRule,
    ValidateRequiredFieldsRule,
    NormalizeDateFieldsRule,
    GenerateUniqueKeysRule,
    RemoveInternalDuplicatesRule,
    ConvertNumericFieldsRule
)


class ValidationChainBuilder:
    """
    Builder para construir cadenas de validación de forma fluida

    Example:
        chain = (ValidationChainBuilder()
            .remove_empty_rows()
            .clean_strings()
            .validate_required(['orderId', 'orderItemId'])
            .normalize_dates(['purchaseDate', 'paymentsDate'])
            .generate_keys(['orderId', 'purchaseDate', 'orderItemId'])
            .remove_duplicates()
            .build())
    """

    def __init__(self):
        self.rules: List[ValidationRule] = []

    def remove_empty_rows(self) -> 'ValidationChainBuilder':
        """Agregar regla para remover filas vacías"""
        self.rules.append(RemoveEmptyRowsRule())
        return self

    def clean_strings(self) -> 'ValidationChainBuilder':
        """Agregar regla para limpiar campos string"""
        self.rules.append(CleanStringFieldsRule())
        return self

    def validate_required(self, required_fields: List[str]) -> 'ValidationChainBuilder':
        """
        Agregar regla para validar campos obligatorios

        Args:
            required_fields: Lista de nombres de campos requeridos
        """
        self.rules.append(ValidateRequiredFieldsRule(required_fields))
        return self

    def normalize_dates(self, date_columns: List[str]) -> 'ValidationChainBuilder':
        """
        Agregar regla para normalizar fechas

        Args:
            date_columns: Lista de nombres de columnas de fecha
        """
        self.rules.append(NormalizeDateFieldsRule(date_columns))
        return self

    def generate_keys(
        self,
        key_fields: List[str],
        key_column_name: str = 'unique_key'
    ) -> 'ValidationChainBuilder':
        """
        Agregar regla para generar claves únicas

        Args:
            key_fields: Campos a usar para la clave
            key_column_name: Nombre de la columna de clave
        """
        self.rules.append(GenerateUniqueKeysRule(key_fields, key_column_name))
        return self

    def remove_duplicates(
        self,
        unique_key_column: str = 'unique_key',
        keep: str = 'first'
    ) -> 'ValidationChainBuilder':
        """
        Agregar regla para remover duplicados

        Args:
            unique_key_column: Columna con la clave única
            keep: 'first' o 'last' - cuál registro mantener
        """
        self.rules.append(RemoveInternalDuplicatesRule(
            unique_key_column, keep))
        return self

    def convert_numeric(self, numeric_columns: List[str]) -> 'ValidationChainBuilder':
        """
        Agregar regla para convertir campos numéricos

        Args:
            numeric_columns: Lista de columnas numéricas
        """
        self.rules.append(ConvertNumericFieldsRule(numeric_columns))
        return self

    def add_custom_rule(self, rule: ValidationRule) -> 'ValidationChainBuilder':
        """
        Agregar regla personalizada

        Args:
            rule: Instancia de ValidationRule
        """
        self.rules.append(rule)
        return self

    def build(self) -> ValidationRule:
        """
        Construir la cadena de validación

        Returns:
            Primera regla de la cadena (punto de entrada)

        Raises:
            ValueError: Si no hay reglas agregadas
        """
        if not self.rules:
            raise ValueError("No validation rules added to chain")

        # Encadenar todas las reglas
        for i in range(len(self.rules) - 1):
            self.rules[i].set_next(self.rules[i + 1])

        # Retornar la primera regla (punto de entrada)
        return self.rules[0]
