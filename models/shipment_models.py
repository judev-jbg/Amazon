from dataclasses import dataclass
from typing import Optional


@dataclass
class ShipmentData:
    codbar: str
    Expedicion: str  # tracking number
    Referencia: str  # internal reference
    DptoDst: str     # order ID
    id_order_ps: Optional[int]
    reference_ps: Optional[str]