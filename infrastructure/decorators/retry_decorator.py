"""
Decorador para retry automático con backoff exponencial
"""
import asyncio
import logging
from functools import wraps
from typing import Tuple, Type, Callable, Any


logger = logging.getLogger(__name__)


def async_retry(
    max_retries: int = 3,
    backoff_base: int = 2,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Callable[[Exception, int], None] = None
):
    """
    Decorador para retry automático con backoff exponencial en funciones async.

    Args:
        max_retries: Número máximo de reintentos
        backoff_base: Base para cálculo exponencial de espera (2^retry_count)
        exceptions: Tupla de excepciones que triggean retry
        on_retry: Callback opcional que se ejecuta en cada retry

    Example:
        @async_retry(max_retries=3, exceptions=(ConnectionError, TimeoutError))
        async def fetch_data():
            # código que puede fallar
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for retry_count in range(max_retries):
                try:
                    return await func(*args, **kwargs)

                except exceptions as e:
                    last_exception = e

                    # Si es el último intento, re-raise
                    if retry_count == max_retries - 1:
                        logger.error(
                            f"{func.__name__}: Máximo de reintentos ({max_retries}) alcanzado",
                            extra={
                                'function': func.__name__,
                                'retry_count': retry_count + 1,
                                'error': str(e)
                            }
                        )
                        raise

                    # Calcular tiempo de espera
                    wait_time = backoff_base ** retry_count

                    logger.warning(
                        f"{func.__name__}: Intento {retry_count + 1}/{max_retries} falló. "
                        f"Reintentando en {wait_time}s...",
                        extra={
                            'function': func.__name__,
                            'retry_count': retry_count + 1,
                            'wait_time': wait_time,
                            'error': str(e)
                        }
                    )

                    # Ejecutar callback si existe
                    if on_retry:
                        on_retry(e, retry_count)

                    # Esperar antes del siguiente intento
                    await asyncio.sleep(wait_time)

            # No debería llegar aquí, pero por seguridad
            raise last_exception if last_exception else RuntimeError(
                "Max retries exceeded")

        return wrapper
    return decorator


def sync_retry(
    max_retries: int = 3,
    backoff_base: int = 2,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
):
    """
    Decorador para retry en funciones síncronas.

    Args:
        max_retries: Número máximo de reintentos
        backoff_base: Base para cálculo exponencial de espera
        exceptions: Tupla de excepciones que triggean retry

    Example:
        @sync_retry(max_retries=3, exceptions=(ValueError,))
        def parse_data():
            # código que puede fallar
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            import time
            last_exception = None

            for retry_count in range(max_retries):
                try:
                    return func(*args, **kwargs)

                except exceptions as e:
                    last_exception = e

                    if retry_count == max_retries - 1:
                        logger.error(
                            f"{func.__name__}: Máximo de reintentos alcanzado"
                        )
                        raise

                    wait_time = backoff_base ** retry_count
                    logger.warning(
                        f"{func.__name__}: Reintentando en {wait_time}s..."
                    )

                    time.sleep(wait_time)

            raise last_exception if last_exception else RuntimeError(
                "Max retries exceeded")

        return wrapper
    return decorator
