# models/inventory_models.py
"""
Modelos de datos para gestión de inventario Amazon
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Literal
from enum import Enum


class InventoryUpdateAction(Enum):
    """Acciones de actualización de inventario"""
    ACTIVATE = "activate"          # Quantity = 10
    DEACTIVATE = "deactivate"      # Quantity = 0
    CUSTOM = "custom"              # Quantity personalizada


class InventoryUpdateMode(str, Enum):
    """Modos de ejecución de sincronización"""
    ACTIVATE = "activate"                    # Manual: Activar productos
    DEACTIVATE = "deactivate"                # Manual: Desactivar productos
    SCHEDULED_FRIDAY = "scheduled_friday"    # Auto: Viernes 17h → Q=10
    SCHEDULED_MONDAY = "scheduled_monday"    # Auto: Lunes 5h → Q=0


@dataclass
class InventoryUpdateRequest:
    """Request para actualización de inventario"""
    sku: str
    quantity: int
    action: InventoryUpdateAction
    marketplace_id: str = 'A1RKKUPIHCS9HS'  # España por defecto

    def __post_init__(self):
        """Validar datos"""
        if self.quantity < 0:
            raise ValueError(
                f"Quantity no puede ser negativa: {self.quantity}")

        if not self.sku or not self.sku.strip():
            raise ValueError("SKU no puede estar vacío")


@dataclass
class InventoryUpdateResult:
    """Resultado de actualización de inventario"""
    sku: str
    success: bool
    message: str
    previous_quantity: Optional[int] = None
    new_quantity: Optional[int] = None
    error: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        """Establecer timestamp si no se proporciona"""
        if self.timestamp is None:
            self.timestamp = datetime.now()

    def to_dict(self):
        """Convertir a diccionario"""
        return {
            'sku': self.sku,
            'success': self.success,
            'message': self.message,
            'previous_quantity': self.previous_quantity,
            'new_quantity': self.new_quantity,
            'error': self.error,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None
        }


@dataclass
class InventorySyncSummary:
    """Resumen de sincronización de inventario"""
    mode: str
    total_products: int
    successful_updates: int
    failed_updates: int
    skipped_products: int
    target_quantity: int
    execution_time_seconds: float
    start_time: datetime
    end_time: datetime
    failed_skus: list[str]

    @property
    def success_rate(self) -> float:
        """Calcular tasa de éxito"""
        if self.total_products == 0:
            return 0.0
        return (self.successful_updates / self.total_products) * 100

    def to_dict(self):
        """Convertir a diccionario"""
        return {
            'mode': self.mode,
            'total_products': self.total_products,
            'successful_updates': self.successful_updates,
            'failed_updates': self.failed_updates,
            'skipped_products': self.skipped_products,
            'target_quantity': self.target_quantity,
            'success_rate': round(self.success_rate, 2),
            'execution_time_seconds': round(self.execution_time_seconds, 2),
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat(),
            'failed_skus': self.failed_skus
        }


@dataclass
class ProductInventoryInfo:
    """Información de inventario de un producto"""
    sku: str
    id_articulo: str  # Equivalente en ERP
    current_quantity: Optional[int] = None
    last_updated: Optional[datetime] = None
    marketplace_id: str = 'A1RKKUPIHCS9HS'
    status: Literal['active', 'inactive', 'unknown'] = 'unknown'

    def needs_update(self, target_quantity: int) -> bool:
        """Determinar si el producto necesita actualización"""
        if self.current_quantity is None:
            return True
        return self.current_quantity != target_quantity


# Constantes para cantidades estándar
QUANTITY_ACTIVE = 10
QUANTITY_INACTIVE = 0

# Mapeo de modos a cantidades objetivo
MODE_TO_QUANTITY = {
    InventoryUpdateMode.ACTIVATE: QUANTITY_ACTIVE,
    InventoryUpdateMode.DEACTIVATE: QUANTITY_INACTIVE,
    InventoryUpdateMode.SCHEDULED_FRIDAY: QUANTITY_ACTIVE,
    InventoryUpdateMode.SCHEDULED_MONDAY: QUANTITY_INACTIVE,
}
