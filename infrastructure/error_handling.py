import asyncio
import logging
import traceback
import config.setting as st
from datetime import datetime
from typing import Dict, Any
import aiofiles
from infrastructure.async_email_client import AsyncEmailClient
from infrastructure.metrics_collector import MetricsCollector
from models.error_models import AlertLevel, ErrorCategory, ErrorContext


"""
FUNCIONALIDAD:
- Captura y categoriza errores automáticamente
- Envía notificaciones por email inteligentes
- Logging estructurado
- Previene spam de emails
"""



class EnhancedErrorHandler:
    def __init__(self):
        self.logger = self._setup_logger()
        self.email_client = AsyncEmailClient()
        self.metrics = MetricsCollector()
        
    def _setup_logger(self):
        """Configurar logging estructurado"""
        logger = logging.getLogger('amazon_orders')
        logger.setLevel(logging.INFO)
        
        # Handler para archivo
        file_handler = logging.FileHandler('logs/orders_processor.log')
        file_handler.setLevel(logging.INFO)
        
        # Handler para errores críticos
        error_handler = logging.FileHandler('logs/critical_errors.log')
        error_handler.setLevel(logging.ERROR)
        
        # Formato estructurado
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(name)s | %(message)s | %(extra)s'
        )
        
        file_handler.setFormatter(formatter)
        error_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(error_handler)
        
        return logger
    
    async def handle_error(self, error: Exception, context: Dict[str, Any] = None):
        """Manejo centralizado de errores"""
        error_context = self._create_error_context(error, context)
        
        # 1. Log estructurado
        await self._log_error(error_context)
        
        # 2. Métricas
        await self.metrics.record_error(error_context)
        
        # 3. Notificación por email (si aplica)
        if self._should_send_email(error_context):
            await self._send_error_notification(error_context)
        
        # 4. Alertas adicionales para errores críticos
        if error_context.level == AlertLevel.CRITICAL:
            await self._send_critical_alert(error_context)
    
    def _create_error_context(self, error: Exception, context: Dict[str, Any] = None) -> ErrorContext:
        """Crear contexto enriquecido del error"""
        tb = traceback.extract_tb(error.__traceback__)
        frame = tb[-1] if tb else None
        
        return ErrorContext(
            error_type=type(error).__name__,
            error_message=str(error),
            file_name=frame.filename if frame else "unknown",
            line_number=frame.lineno if frame else 0,
            function_name=frame.name if frame else "unknown",
            category=self._categorize_error(error),
            level=self._determine_alert_level(error),
            timestamp=datetime.now(),
            additional_data=context or {},
            stack_trace=traceback.format_exc(),
            process_mode=context.get('process_mode') if context else None,
            market_id=context.get('market_id') if context else None
        )
    
    def _categorize_error(self, error: Exception) -> ErrorCategory:
        """Categorizar el error automáticamente"""
        error_mappings = {
            'SellingApiException': ErrorCategory.API_ERROR,
            'DatabaseError': ErrorCategory.DATABASE_ERROR,
            'ConnectionError': ErrorCategory.DATABASE_ERROR,
            'ProcessingError': ErrorCategory.PROCESSING_ERROR,
            'SystemError': ErrorCategory.SYSTEM_ERROR,
        }
        
        error_type = type(error).__name__
        return error_mappings.get(error_type, ErrorCategory.SYSTEM_ERROR)
    
    def _determine_alert_level(self, error: Exception) -> AlertLevel:
        """Determinar nivel de alerta"""
        critical_errors = ['DatabaseError', 'ConnectionError', 'AuthenticationError']
        warning_errors = ['SellingApiException', 'RateLimitError']
        
        error_type = type(error).__name__
        
        if error_type in critical_errors:
            return AlertLevel.CRITICAL
        elif error_type in warning_errors:
            return AlertLevel.WARNING
        else:
            return AlertLevel.ERROR
    
    async def _log_error(self, error_context: ErrorContext):
        """Log estructurado del error"""
        extra_data = {
            'category': error_context.category.value,
            'level': error_context.level.value,
            'process_mode': error_context.process_mode,
            'market_id': error_context.market_id,
            'additional_data': error_context.additional_data
        }
        
        log_message = f"{error_context.error_type}: {error_context.error_message}"
        
        if error_context.level == AlertLevel.CRITICAL:
            self.logger.error(log_message, extra=extra_data)
        elif error_context.level == AlertLevel.WARNING:
            self.logger.warning(log_message, extra=extra_data)
        else:
            self.logger.info(log_message, extra=extra_data)
        
        # También mantener el CSV para compatibilidad
        await self._log_to_csv(error_context)
    
    async def _log_to_csv(self, error_context: ErrorContext):
        """Mantener logging a CSV (compatibilidad)"""
        log_entry = {
            'event_date': error_context.timestamp.isoformat(),
            'event_class': error_context.error_type,
            'event_desc': error_context.error_message,
            'event_file': error_context.file_name,
            'event_line': error_context.line_number,
            'file_or_reason': error_context.process_mode or 'unknown',
            'category': error_context.category.value,
            'level': error_context.level.value
        }
        
        # Usar aiofiles para escritura async
        async with aiofiles.open('logs/info_event_log.csv', 'a') as f:
            await f.write(f"{log_entry}\n")
    
    def _should_send_email(self, error_context: ErrorContext) -> bool:
        """Determinar si enviar email"""
        # Enviar email para errores y críticos, pero no para warnings repetitivos
        if error_context.level in [AlertLevel.ERROR, AlertLevel.CRITICAL]:
            return True
        
        # Para warnings, solo si no se ha enviado en las últimas 2 horas
        return not self._was_recently_notified(error_context)
    
    def _was_recently_notified(self, error_context: ErrorContext) -> bool:
        """Verificar si ya se notificó recientemente (evitar spam)"""
        # Implementar cache/DB check para evitar spam
        return False  # Simplificado por ahora
    
    async def _send_error_notification(self, error_context: ErrorContext):
        """Enviar notificación por email"""
        subject = f"🚨 [{error_context.level.value.upper()}] Amazon Orders - {error_context.error_type}"
        
        html_body = self._generate_error_html(error_context)
        
        await self.email_client.send_email(
            subject=subject,
            html_body=html_body,
            recipients=st.setting_email_recipients['errors']
        )
    
    async def _send_critical_alert(self, error_context: ErrorContext):
        """Enviar alerta crítica (múltiples canales)"""
        # Email prioritario
        await self.email_client.send_priority_email(
            subject=f"🔥 CRITICAL: Amazon Orders System Down",
            html_body=self._generate_critical_html(error_context),
            recipients=st.setting_email_recipients['critical']
        )
        
        # Aquí puedes añadir Slack, Teams, SMS, etc.
        # await self.slack_client.send_alert(error_context)
    
    def _generate_error_html(self, error_context: ErrorContext) -> str:
        """Generar HTML mejorado para el email"""
        return f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <div style="max-width: 800px; margin: 0 auto;">
                <h2 style="color: {'#d32f2f' if error_context.level == AlertLevel.CRITICAL else '#f57c00'};">
                    🚨 Error en Amazon Orders Processor
                </h2>
                
                <div style="background: #f5f5f5; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3>📋 Detalles del Error</h3>
                    <p><strong>Tipo:</strong> {error_context.error_type}</p>
                    <p><strong>Mensaje:</strong> {error_context.error_message}</p>
                    <p><strong>Archivo:</strong> {error_context.file_name}:{error_context.line_number}</p>
                    <p><strong>Función:</strong> {error_context.function_name}</p>
                    <p><strong>Categoría:</strong> {error_context.category.value}</p>
                    <p><strong>Nivel:</strong> {error_context.level.value}</p>
                    <p><strong>Timestamp:</strong> {error_context.timestamp}</p>
                    {f"<p><strong>Modo:</strong> {error_context.process_mode}</p>" if error_context.process_mode else ""}
                    {f"<p><strong>Market:</strong> {error_context.market_id}</p>" if error_context.market_id else ""}
                </div>
                
                <div style="background: #fff3cd; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3>🔧 Datos Adicionales</h3>
                    <pre style="background: #f8f9fa; padding: 10px; border-radius: 4px; overflow-x: auto;">
{error_context.additional_data}
                    </pre>
                </div>
                
                <div style="background: #f8d7da; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3>📊 Stack Trace</h3>
                    <pre style="background: #f8f9fa; padding: 10px; border-radius: 4px; overflow-x: auto; font-size: 12px;">
{error_context.stack_trace}
                    </pre>
                </div>
                
                <div style="margin-top: 30px; padding: 20px; background: #e3f2fd; border-radius: 8px;">
                    <h3>🔍 Acciones Sugeridas</h3>
                    {self._get_suggested_actions(error_context)}
                </div>
            </div>
        </body>
        </html>
        """
    
    def _get_suggested_actions(self, error_context: ErrorContext) -> str:
        """Generar acciones sugeridas según el tipo de error"""
        actions = {
            ErrorCategory.API_ERROR: """
                <ul>
                    <li>Verificar rate limits de Amazon SP-API</li>
                    <li>Revisar credenciales de API</li>
                    <li>Comprobar conectividad</li>
                </ul>
            """,
            ErrorCategory.DATABASE_ERROR: """
                <ul>
                    <li>Verificar conexión a base de datos</li>
                    <li>Revisar espacio en disco</li>
                    <li>Comprobar permisos de usuario</li>
                </ul>
            """,
            ErrorCategory.PROCESSING_ERROR: """
                <ul>
                    <li>Revisar formato de datos</li>
                    <li>Verificar lógica de negocio</li>
                    <li>Comprobar transformaciones</li>
                </ul>
            """
        }
        
        return actions.get(error_context.category, "<p>Revisar logs para más detalles</p>")