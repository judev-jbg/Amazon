"""
Clase base para servicios asíncronos - elimina código duplicado
"""
import logging
from abc import ABC
from contextlib import asynccontextmanager
from typing import List, Any


class AsyncService(ABC):
    """
    Clase base para servicios asíncronos con ciclo de vida gestionado.

    Proporciona:
    - Gestión automática de inicialización/finalización de dependencias
    - Context manager para uso con 'async with'
    - Logging estructurado
    """

    def __init__(self):
        self._initialized = False
        self._dependencies: List[Any] = []
        self.logger = logging.getLogger(self.__class__.__name__)

    def register_dependency(self, dependency: Any) -> None:
        """
        Registrar dependencia que requiere inicialización.

        Args:
            dependency: Objeto con métodos init_pool() y close_pool()
        """
        if dependency not in self._dependencies:
            self._dependencies.append(dependency)

    async def _ensure_initialized(self) -> None:
        """Inicializar todas las dependencias registradas"""
        if self._initialized:
            return

        self.logger.info(f"Iniciando {self.__class__.__name__}")

        for dep in self._dependencies:
            # Inicializar pool principal si existe
            if hasattr(dep, 'init_pool'):
                await dep.init_pool()
                self.logger.debug(
                    f"Pool inicializado para {dep.__class__.__name__}")

            # Inicializar pool secundario (Prestashop) si existe
            if hasattr(dep, 'init_prestashop_pool'):
                await dep.init_prestashop_pool()
                self.logger.debug(
                    f"Pool Prestashop inicializado para {dep.__class__.__name__}")

            # Inicializar cliente de email si existe
            if hasattr(dep, '_init_email_client'):
                await dep._init_email_client()
                self.logger.debug(
                    f"Email client inicializado para {dep.__class__.__name__}")

        self._initialized = True
        self.logger.info(
            f"{self.__class__.__name__} inicializado correctamente")

    async def _ensure_finished(self) -> None:
        """Finalizar todas las dependencias registradas"""
        if not self._initialized:
            return

        self.logger.info(f"Finalizando {self.__class__.__name__}")

        for dep in reversed(self._dependencies):  # Cerrar en orden inverso
            # Cerrar pool principal
            if hasattr(dep, 'close_pool'):
                await dep.close_pool()
                self.logger.debug(
                    f"Pool cerrado para {dep.__class__.__name__}")

            # Cerrar pool secundario
            if hasattr(dep, 'close_pool_prestashop'):
                await dep.close_pool_prestashop()
                self.logger.debug(
                    f"Pool Prestashop cerrado para {dep.__class__.__name__}")

        self._initialized = False
        self.logger.info(f"{self.__class__.__name__} finalizado correctamente")

    @asynccontextmanager
    async def lifecycle(self):
        """
        Context manager para gestionar ciclo de vida del servicio.

        Usage:
            async with service.lifecycle():
                await service.do_work()
        """
        try:
            await self._ensure_initialized()
            yield self
        finally:
            await self._ensure_finished()

    async def start(self) -> None:
        """Iniciar servicio explícitamente (alternativa a context manager)"""
        await self._ensure_initialized()

    async def stop(self) -> None:
        """Detener servicio explícitamente"""
        await self._ensure_finished()
