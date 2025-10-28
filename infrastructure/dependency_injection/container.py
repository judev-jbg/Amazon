
"""
Dependency Injection Container
Gestiona creación y ciclo de vida de dependencias
"""
import logging
from typing import Dict, Any, Type, Callable

from core.database_manager import DatabaseManager
from core.amazon_api_client import AmazonAPIClient
from infrastructure.error_handling import EnhancedErrorHandler
from infrastructure.metrics_collector import MetricsCollector
from core.order_service import OrderExtractionService
from services.order_details_service import OrderDetailsService
from services.shipment_service import ShipmentService
from utils.unified_order_processor import UnifiedOrderProcessor
from infrastructure.data_validator import DataValidator
from services.file_processor import FileProcessor
from services.inventory_sync_service import InventorySyncService
from infrastructure.repositories.mssql_product_repository import MSSQLProductRepository
from core.api.amazon_listings_api_wrapper import AmazonListingsAPIWrapper
from services.product_verification_service import ProductVerificationService
from core.api.amazon_catalog_api_wrapper import AmazonCatalogAPIWrapper


class DependencyContainer:
    """
    Container para inyección de dependencias
    Implementa Service Locator pattern con lazy loading
    """

    def __init__(self):
        self._services: Dict[str, Any] = {}
        self._factories: Dict[str, Callable] = {}
        self._singletons: Dict[str, Any] = {}
        self.logger = logging.getLogger(self.__class__.__name__)

    def register_singleton(self, interface: Type, factory: Callable) -> None:
        """
        Registrar servicio como singleton (una sola instancia)

        Args:
            interface: Clase o tipo del servicio
            factory: Función que crea la instancia
        """
        key = self._get_key(interface)
        self._factories[key] = factory
        self.logger.debug(f"Registered singleton: {key}")

    def register_transient(self, interface: Type, factory: Callable) -> None:
        """
        Registrar servicio como transient (nueva instancia cada vez)

        Args:
            interface: Clase o tipo del servicio
            factory: Función que crea la instancia
        """
        key = self._get_key(interface)
        self._services[key] = factory
        self.logger.debug(f"Registered transient: {key}")

    def resolve(self, interface: Type) -> Any:
        """
        Resolver dependencia (obtener instancia)

        Args:
            interface: Clase o tipo del servicio

        Returns:
            Instancia del servicio

        Raises:
            KeyError: Si el servicio no está registrado
        """
        key = self._get_key(interface)

        # Verificar si es singleton ya creado
        if key in self._singletons:
            return self._singletons[key]

        # Crear singleton si tiene factory
        if key in self._factories:
            instance = self._factories[key](self)
            self._singletons[key] = instance
            self.logger.debug(f"Created singleton: {key}")
            return instance

        # Crear transient
        if key in self._services:
            instance = self._services[key](self)
            self.logger.debug(f"Created transient: {key}")
            return instance

        raise KeyError(f"Service not registered: {key}")

    def _get_key(self, interface: Type) -> str:
        """Obtener clave string para el servicio"""
        return f"{interface.__module__}.{interface.__name__}"

    @classmethod
    def create_production_container(cls) -> 'DependencyContainer':
        """
        Factory method para crear container con dependencias de producción

        Returns:
            Container configurado para producción
        """
        container = cls()

        # Registrar infraestructura como singletons
        container.register_singleton(
            DatabaseManager,
            lambda c: DatabaseManager()
        )

        container.register_singleton(
            AmazonAPIClient,
            lambda c: AmazonAPIClient()
        )

        container.register_singleton(
            EnhancedErrorHandler,
            lambda c: EnhancedErrorHandler()
        )

        container.register_singleton(
            MetricsCollector,
            lambda c: MetricsCollector()
        )

        # Registrar servicios
        container.register_singleton(
            OrderExtractionService,
            lambda c: OrderExtractionService()
        )

        container.register_singleton(
            OrderDetailsService,
            lambda c: OrderDetailsService()
        )

        container.register_singleton(
            ShipmentService,
            lambda c: ShipmentService()
        )

        container.register_singleton(
            UnifiedOrderProcessor,
            lambda c: UnifiedOrderProcessor()
        )

        container.register_singleton(
            MSSQLProductRepository,
            lambda c: MSSQLProductRepository()
        )

        container.register_singleton(
            AmazonListingsAPIWrapper,
            lambda c: AmazonListingsAPIWrapper()
        )

        container.register_singleton(
            InventorySyncService,
            lambda c: InventorySyncService()
        )

        container.register_singleton(
            AmazonCatalogAPIWrapper,
            lambda c: AmazonCatalogAPIWrapper()
        )

        container.register_singleton(
            ProductVerificationService,
            lambda c: ProductVerificationService()
        )

        # Registrar utilidades como transient
        container.register_transient(
            DataValidator,
            lambda c: DataValidator(c.resolve(DatabaseManager))
        )

        container.register_transient(
            FileProcessor,
            lambda c: FileProcessor()
        )

        container.logger.info("Production container configured")
        return container

    @classmethod
    def create_test_container(cls) -> 'DependencyContainer':
        """
        Factory method para crear container para tests
        Puede sobrescribir dependencias con mocks

        Returns:
            Container configurado para testing
        """
        container = cls()
        container.logger.info("Test container configured")
        return container
