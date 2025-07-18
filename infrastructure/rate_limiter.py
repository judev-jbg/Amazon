# infrastructure/rate_limiter.py
"""
FUNCIONALIDAD:
- Controla la velocidad de requests a Amazon SP-API
- Implementa rate limiting con ventanas deslizantes
- Maneja diferentes límites por endpoint
- Backoff automático cuando se alcanzan límites
"""

import asyncio
import time
from collections import deque
from typing import Dict, Optional
from dataclasses import dataclass
from enum import Enum

class APIEndpoint(Enum):
    """Endpoints de Amazon SP-API con sus límites específicos"""
    ORDERS = "orders"
    ORDER_ITEMS = "order_items"
    SALES = "sales"
    OFFERS = "offers"
    REPORTS = "reports"

@dataclass
class RateLimit:
    """Configuración de límites para cada endpoint"""
    max_requests: int
    window_seconds: int
    burst_limit: Optional[int] = None

class RateLimiter:
    """Rate limiter inteligente para Amazon SP-API"""
    
    # Límites específicos por endpoint según documentación de Amazon
    ENDPOINT_LIMITS = {
        APIEndpoint.ORDERS: RateLimit(max_requests=6, window_seconds=60, burst_limit=20),
        APIEndpoint.ORDER_ITEMS: RateLimit(max_requests=300, window_seconds=60, burst_limit=100),
        APIEndpoint.SALES: RateLimit(max_requests=45, window_seconds=60, burst_limit=15),
        APIEndpoint.OFFERS: RateLimit(max_requests=5, window_seconds=60, burst_limit=10),
        APIEndpoint.REPORTS: RateLimit(max_requests=15, window_seconds=60, burst_limit=10),
    }
    
    def __init__(self, max_requests: int = 100, window: int = 60):
        self.default_limit = RateLimit(max_requests, window)
        self.request_history: Dict[APIEndpoint, deque] = {}
        self.locks: Dict[APIEndpoint, asyncio.Lock] = {}
        self.retry_delays: Dict[APIEndpoint, float] = {}
        
        # Inicializar estructuras para cada endpoint
        for endpoint in APIEndpoint:
            self.request_history[endpoint] = deque()
            self.locks[endpoint] = asyncio.Lock()
            self.retry_delays[endpoint] = 0.0
    
    async def __aenter__(self):
        """Context manager para rate limiting genérico"""
        await self.acquire()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Salir del context manager"""
        pass
    
    async def acquire(self, endpoint: Optional[APIEndpoint] = None):
        """Adquirir permiso para hacer request"""
        if endpoint is None:
            # Rate limiting genérico
            await self._wait_for_generic_limit()
        else:
            # Rate limiting específico por endpoint
            await self._wait_for_endpoint_limit(endpoint)
    
    async def _wait_for_generic_limit(self):
        """Rate limiting genérico (usado en context manager)"""
        # Implementación simple para compatibilidad
        await asyncio.sleep(0.1)  # Delay mínimo
    
    async def _wait_for_endpoint_limit(self, endpoint: APIEndpoint):
        """Rate limiting específico por endpoint"""
        async with self.locks[endpoint]:
            await self._cleanup_old_requests(endpoint)
            
            # Verificar si hay delay de retry pendiente
            if self.retry_delays[endpoint] > 0:
                await asyncio.sleep(self.retry_delays[endpoint])
                self.retry_delays[endpoint] = 0.0
            
            limit = self.ENDPOINT_LIMITS.get(endpoint, self.default_limit)
            request_history = self.request_history[endpoint]
            
            # Si hemos alcanzado el límite, esperar
            if len(request_history) >= limit.max_requests:
                sleep_time = self._calculate_sleep_time(endpoint)
                if sleep_time > 0:
                    print(f"⏱️ Rate limit reached for {endpoint.value}, waiting {sleep_time:.2f}s")
                    await asyncio.sleep(sleep_time)
                    await self._cleanup_old_requests(endpoint)
            
            # Registrar el request actual
            request_history.append(time.time())
    
    async def _cleanup_old_requests(self, endpoint: APIEndpoint):
        """Limpiar requests antiguos fuera de la ventana"""
        limit = self.ENDPOINT_LIMITS.get(endpoint, self.default_limit)
        current_time = time.time()
        cutoff_time = current_time - limit.window_seconds
        
        request_history = self.request_history[endpoint]
        while request_history and request_history[0] < cutoff_time:
            request_history.popleft()
    
    def _calculate_sleep_time(self, endpoint: APIEndpoint) -> float:
        """Calcular tiempo de espera necesario"""
        limit = self.ENDPOINT_LIMITS.get(endpoint, self.default_limit)
        request_history = self.request_history[endpoint]
        
        if not request_history:
            return 0.0
        
        # Tiempo hasta que el request más antiguo salga de la ventana
        oldest_request = request_history[0]
        time_to_wait = limit.window_seconds - (time.time() - oldest_request)
        
        return max(0.0, time_to_wait + 0.1)  # +0.1s buffer
    
    async def handle_rate_limit_error(self, endpoint: APIEndpoint, retry_after: Optional[int] = None):
        """Manejar error 429 (Too Many Requests)"""
        if retry_after:
            delay = retry_after
        else:
            # Backoff exponencial si no se especifica retry_after
            current_delay = self.retry_delays.get(endpoint, 1.0)
            delay = min(current_delay * 2, 300)  # Max 5 minutos
            self.retry_delays[endpoint] = delay
        
        print(f"🚫 Rate limit error for {endpoint.value}, backing off for {delay}s")
        await asyncio.sleep(delay)
    
    def get_current_usage(self, endpoint: APIEndpoint) -> Dict[str, int]:
        """Obtener uso actual de rate limit"""
        limit = self.ENDPOINT_LIMITS.get(endpoint, self.default_limit)
        current_requests = len(self.request_history[endpoint])
        
        return {
            'current_requests': current_requests,
            'max_requests': limit.max_requests,
            'window_seconds': limit.window_seconds,
            'usage_percentage': (current_requests / limit.max_requests) * 100
        }
    
    async def wait_for_quota_reset(self, endpoint: APIEndpoint):
        """Esperar hasta que se resetee completamente la cuota"""
        limit = self.ENDPOINT_LIMITS.get(endpoint, self.default_limit)
        await asyncio.sleep(limit.window_seconds + 1)
        self.request_history[endpoint].clear()
        self.retry_delays[endpoint] = 0.0

# Decorador para rate limiting automático
def rate_limited(endpoint: APIEndpoint):
    """Decorador para aplicar rate limiting automático a métodos"""
    def decorator(func):
        async def wrapper(self, *args, **kwargs):
            # Asumir que el objeto tiene un rate_limiter
            if hasattr(self, 'rate_limiter'):
                await self.rate_limiter.acquire(endpoint)
            return await func(self, *args, **kwargs)
        return wrapper
    return decorator

# Rate limiter global para usar como context manager
class GlobalRateLimiter:
    """Rate limiter singleton para uso global"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = RateLimiter()
        return cls._instance
    
    async def __aenter__(self):
        await self.acquire()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass