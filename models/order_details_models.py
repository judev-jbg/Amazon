from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class OrderDetail:
    orderId: str
    orderItemId: str
    purchaseDate: datetime
    paymentsDate: Optional[datetime]
    buyerEmail: str
    buyerName: str
    buyerPhoneNumber: Optional[str]
    sku: str
    numberOfItems: int
    productName: str
    quantityPurchased: int
    currency: str
    itemPrice: float
    itemTax: float
    shippingPrice: float
    shippingTax: float
    shipServiceLevel: str
    recipientName: str
    shipAddress1: str
    shipAddress2: Optional[str]
    shipAddress3: Optional[str]
    shipCity: str
    shipState: str
    shipPostalCode: str
    shipCountry: str
    shipPhoneNumber: Optional[str]
    billName: str
    billAddress1: str
    billAddress2: Optional[str]
    billAddress3: Optional[str]
    billCity: str
    billState: str
    billPostalCode: str
    billCountry: str
    deliveryStartDate: Optional[datetime]
    deliveryEndDate: Optional[datetime]
    deliveryTimeZone: Optional[str]
    deliveryInstructions: Optional[str]
    salesChannel: str
    orderChannel: str
    orderChannelInstance: Optional[str]
    externalOrderId: Optional[str]
    isBusinessOrder: int
    purchaseOrderNumber: Optional[str]
    priceDesignation: Optional[str]
    buyerCompanyName: Optional[str]
    buyerCstNumber: Optional[str]
    buyerVatNumber: Optional[str]
    buyerTaxRegistrationId: Optional[str]
    buyerTaxRegistrationCountry: Optional[str]
    buyerTaxRegistrationType: Optional[str]
    isAmazonInvoiced: int
    vatExclusiveItemPrice: Optional[float]
    vatExclusiveShippingPrice: Optional[float]
    vatExclusiveGiftwrapPrice: Optional[float]
    shipmentStatus: Optional[str]
    isIba: Optional[int]
    isBuyerRequestedCancellation: int
    buyerRequestedCancelReason: Optional[str]
    
    # Campos calculados/internos
    unique_key: str = None  # Para validación de duplicados
    
    def __post_init__(self):
        """Generar clave única para validación"""
        # Normalizar fechas para evitar problemas de formato
        purchase_date_str = self.purchaseDate.strftime("%Y-%m-%d %H:%M:%S") if self.purchaseDate else ""
        self.unique_key = f"{self.orderId}|{purchase_date_str}|{self.orderItemId}"