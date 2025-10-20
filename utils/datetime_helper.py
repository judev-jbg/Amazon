# utils/datetime_helper.py
"""
FUNCIONALIDAD:
- Manejo inteligente de fechas/horas para Amazon SP-API
- Conversión automática UTC <-> Local (Madrid)
- Aplicación de restricciones de Amazon (2+ minutos antes)
- Cálculo dinámico de diferencia horaria (verano/invierno)
"""

from datetime import datetime, timedelta, timezone, date
from typing import Tuple, Optional
import config.setting as st


class AmazonDateTimeHelper:
    """Helper para manejar fechas/horas según los requerimientos de Amazon SP-API"""

    def __init__(self):
        self.minutes_before_now = getattr(
            st, 'minutesBeforeDateTime', 10)  # Default 10 minutos

    @property
    def utc_offset_hours(self) -> int:
        """
        Calcula la diferencia horaria UTC dinámicamente (verano/invierno)
        Madrid: UTC+1 (invierno) / UTC+2 (verano)
        """
        now_local = datetime.now()
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        diff = now_local.hour - now_utc.hour

        # Ajustar para cambio de día
        if diff < -12:
            diff += 24
        elif diff > 12:
            diff -= 24

        return diff

    @property
    def today_short(self) -> date:
        """Fecha de hoy (equivalente a tu todayShort)"""
        return date.today()

    def get_amazon_safe_datetime(self, dt: Optional[datetime] = None) -> datetime:
        """
        Obtiene datetime seguro para Amazon (al menos 2+ minutos antes de ahora)

        Args:
            dt: DateTime base (default: ahora)

        Returns:
            datetime: Fecha/hora segura para Amazon API
        """
        if dt is None:
            dt = datetime.now()

        # Aplicar offset de seguridad
        safe_dt = dt - timedelta(minutes=self.minutes_before_now)

        # Ajustar por UTC
        utc_adjusted = safe_dt - timedelta(hours=self.utc_offset_hours)

        return utc_adjusted

    def get_daily_full_range(self) -> Tuple[datetime, datetime]:
        """
        Rango para extracción diaria completa

        Returns:
            Tuple[datetime, datetime]: (date_from, date_to)
            - date_from: Ayer 00:00:00 ajustado UTC
            - date_to: Ahora ajustado UTC menos minutos de seguridad
        """
        today = self.today_short

        # Fecha inicial: ayer 00:00:00
        yesterday = today - timedelta(days=1)
        date_from = datetime.combine(yesterday, datetime.min.time())

        # Ajustar por UTC y aplicar restricciones Amazon
        date_from_utc = date_from - timedelta(hours=self.utc_offset_hours)
        date_to_utc = self.get_amazon_safe_datetime()

        return date_from_utc, date_to_utc

    def get_incremental_range(self) -> Tuple[datetime, datetime]:
        """
        REEMPLAZA: 06.OrdersInterval.py logic
        Rango para extracción incremental (última hora)

        Returns:
            Tuple[datetime, datetime]: (date_from, date_to)
        """
        now = datetime.now()

        # Desde hace 1 hora hasta ahora (menos minutos de seguridad)
        date_from = now - timedelta(hours=1)
        date_to = now

        # Ajustar por UTC y aplicar restricciones
        date_from_utc = date_from - timedelta(hours=self.utc_offset_hours)
        date_to_utc = self.get_amazon_safe_datetime(date_to)

        return date_from_utc, date_to_utc

    def get_status_update_range(self) -> Tuple[datetime, datetime]:
        """
        REEMPLAZA: 06.OrdersUpdateStatusInterval.py logic
        Rango para actualización de estados (última hora expandida)

        Returns:
            Tuple[datetime, datetime]: (date_from, date_to)
        """
        now = datetime.now()

        # Desde hace 1 hora hasta ahora (menos minutos de seguridad)
        date_from = now - timedelta(hours=1)
        date_to = now

        # Ajustar por UTC y aplicar restricciones
        date_from_utc = date_from - timedelta(hours=self.utc_offset_hours)
        date_to_utc = self.get_amazon_safe_datetime(date_to)

        return date_from_utc, date_to_utc

    def get_weekly_catchup_range(self) -> Tuple[datetime, datetime]:
        """
        REEMPLAZA: 06.OrdersWeek.py logic
        Rango para catch-up semanal (últimos 7 días)

        Returns:
            Tuple[datetime, datetime]: (date_from, date_to)
        """
        today = self.today_short
        now = datetime.now()

        # Desde hace 7 días (00:00) hasta hoy
        week_ago = today - timedelta(days=7)
        date_from = datetime.combine(week_ago, datetime.min.time())
        date_to = now

        # Ajustar por UTC y aplicar restricciones
        date_from_utc = date_from - timedelta(hours=self.utc_offset_hours)
        date_to_utc = self.get_amazon_safe_datetime(date_to)

        return date_from_utc, date_to_utc

    def get_custom_range(self, date_from: datetime, date_to: datetime) -> Tuple[datetime, datetime]:
        """
        Ajustar rango personalizado según reglas de Amazon

        Args:
            date_from: Fecha inicial
            date_to: Fecha final

        Returns:
            Tuple[datetime, datetime]: Rango ajustado
        """
        # Ajustar por UTC
        date_from_utc = date_from - timedelta(hours=self.utc_offset_hours)
        date_to_utc = self.get_amazon_safe_datetime(date_to)

        # Validar que date_from no sea mayor que date_to
        if date_from_utc >= date_to_utc:
            raise ValueError(
                f"date_from ({date_from_utc}) must be before date_to ({date_to_utc})")

        return date_from_utc, date_to_utc

    def format_for_amazon_api(self, dt: datetime) -> str:
        """
        Formatear datetime para Amazon SP-API

        Args:
            dt: Datetime a formatear

        Returns:
            str: Fecha en formato ISO para Amazon API
        """
        return dt.isoformat()

    def get_debug_info(self) -> dict:
        """
        Información de debug sobre configuración de fechas

        Returns:
            dict: Info de configuración actual
        """
        now = datetime.now()
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)

        return {
            'local_time': now.isoformat(),
            'utc_time': now_utc.isoformat(),
            'utc_offset_hours': self.utc_offset_hours,
            'minutes_before_now': self.minutes_before_now,
            'today_short': self.today_short.isoformat(),
            'amazon_safe_time': self.get_amazon_safe_datetime().isoformat()
        }


# Instancia global para uso fácil
datetime_helper = AmazonDateTimeHelper()
