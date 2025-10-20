import logging
import sys
import traceback
import config.setting as st
from datetime import datetime
from typing import Dict, Any
import aiofiles
from infrastructure.metrics_collector import MetricsCollector
from pathlib import Path
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
        self.logs_dir = Path("logs")
        self.logs_dir.mkdir(exist_ok=True)
        self.logger = self._setup_logger()
        self.email_client = None
        self._email_initialized = False
        self.metrics = MetricsCollector()
        self._recent_notifications = {}
        
    def _setup_logger(self):
        """Configurar logging estructurado"""
        logger = logging.getLogger('AmazonManagement')
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        
        # Formato estructurado
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(filename)s:%(funcName)s:%(lineno)d | %(message)s'
        )
        
        # 1. Handler para CONSOLA
        # Handler para STDOUT (INFO y DEBUG)
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.DEBUG)
        stdout_handler.setFormatter(formatter)
        # Filtro para que solo INFO y DEBUG vayan a stdout
        stdout_handler.addFilter(lambda record: record.levelno <= logging.INFO)
        logger.addHandler(stdout_handler)
        
        # Handler para STDERR (WARNING, ERROR, CRITICAL)
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.WARNING)
        stderr_handler.setFormatter(formatter)
        logger.addHandler(stderr_handler)

        # 2. Handler para archivo general
        try:
            file_handler = logging.FileHandler(self.logs_dir / 'general.log', encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            print(f"❌ Error al crear el gestor de archivos: {e}")
        
        # 3. Handler para errores críticos
        try:
            error_handler = logging.FileHandler(self.logs_dir / 'critical_errors.log', encoding='utf-8')
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(formatter)
            logger.addHandler(error_handler)
        except Exception as e:
            print(f"❌ Error al crear el gestor de archivos: {e}")
        
        # Evitar propagación a root logger
        logger.propagate = False

        return logger
    
    async def _init_email_client(self):
        """Inicializar cliente de email lazy loading"""
        if not self._email_initialized:
            try:
                from infrastructure.async_email_client import AsyncEmailClient
                self.email_client = AsyncEmailClient()
                self._email_initialized = True
                self.logger.info("📧 Cliente de Email inicializado")
            except Exception as e:
                self.logger.error(f"❌ Error al iniciar el cliente de Email: {e}")
                self.email_client = None
    
    async def handle_error(self, error: Exception, context: Dict[str, Any] = None):
        """Manejo centralizado de errores"""

        try:
            error_context = self._create_error_context(error, context)
            
            # 1. Log estructurado
            await self._log_error(error_context)
            
            # 2. Métricas
            try:

                await self.metrics.record_process_error(error_context)
            except Exception as metrics_error:
                self.logger.warning(f"Fallo en el registro de métricas: {metrics_error}")
            
            # 3. Notificación por email (si aplica)
            if self._should_send_email(error_context):
                await self._send_error_notification(error_context)
            
            # 4. Alertas adicionales para errores críticos
            if error_context.level == AlertLevel.CRITICAL:
                await self._send_critical_alert(error_context)
        
        except Exception as handler_error:
            # Fallback: al menos imprimir en consola si todo falla
            print(f"❌ CRITICO: Error del gestor de errores: {handler_error}")
            print(f"❌ Error original: {error}")
            print(f"❌ Seguimiento: {traceback.format_exc()}")

    async def handle_info(self, message: str, context: Dict[str, Any] = None):
        """Manejar mensaje informativo"""
        log_message = f"INFO: {message}"
        extra_data = {
            'level': 'info',
            'context': context or {}
        }
        self.logger.info(log_message, extra=extra_data)

    async def handle_warning(self, message: str, context: Dict[str, Any] = None):
        """Manejar mensaje de advertencia"""
        log_message = f"WARNING: {message}"
        extra_data = {
            'level': 'warning',
            'context': context or {}
        }
        self.logger.warning(log_message, extra=extra_data)
                
        # Decidir si enviar email (solo para warnings importantes)
        if context and context.get('send_email', False):
            await self._send_warning_notification(message, context)

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
            'FileNotFoundError': ErrorCategory.SYSTEM_ERROR,
            'PermissionError': ErrorCategory.SYSTEM_ERROR,
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
        log_message = (
            f"[{error_context.category.value}] {error_context.error_type}: {error_context.error_message}"
        )
        
        if error_context.process_mode:
            log_message += f" | Mode: {error_context.process_mode}"
        
        if error_context.market_id:
            log_message += f" | Market: {error_context.market_id}"
        
        # Log según nivel
        if error_context.level == AlertLevel.CRITICAL:
            self.logger.critical(log_message)
            self.logger.critical(f"Stack trace:\n{error_context.stack_trace}")
            print(f"CRITICAL: {log_message}", file=sys.stderr)
        elif error_context.level == AlertLevel.ERROR:
            self.logger.error(log_message)
            self.logger.debug(f"Stack trace:\n{error_context.stack_trace}")
            print(f"ERROR: {log_message}", file=sys.stderr)
        elif error_context.level == AlertLevel.WARNING:
            self.logger.warning(log_message)
            print(f"WARNING: {log_message}", file=sys.stderr)
        else:
            self.logger.info(log_message)
    
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
        if error_context.level not in [AlertLevel.ERROR, AlertLevel.CRITICAL]:
            return False
    
        return self._was_recently_notified(error_context)
    
    def _was_recently_notified(self, error_context: ErrorContext) -> bool:
        """Verificar si ya se notificó recientemente (evitar spam)"""
        now = datetime.now()
        key = f"{error_context.error_type}_{error_context.category.value}"

        if key in self._recent_notifications:
            time_diff = now - self._recent_notifications[key]
            if time_diff.total_seconds() < 1800:  # 30 minutos
                return False
        
        self._recent_notifications[key] = now
        return True
    
    async def _send_error_notification(self, error_context: ErrorContext):
        """Enviar notificación por email"""
        try:

            await self._init_email_client()
            if not self.email_client:
                self.logger.warning("Cliente de Email no disponible, omitiendo nortificacion")
                return
            
            subject = f"🚨 [{error_context.level.value.upper()}] Amazon Management - {error_context.error_type}"
        
            html_body = self._generate_error_html(error_context)
            
            await self.email_client.send_email(
                subject=subject,
                html_body=html_body,
                recipients=st.setting_email_recipients['errors']
            )

            self.logger.info(f"📧 Notificacion de error enviada para {error_context.error_type}")
        
        except Exception as email_error:
            self.logger.info(f"📧 El envio de notificacion: {email_error}")

    async def _send_critical_alert(self, error_context: ErrorContext):
        """Enviar alerta crítica (múltiples canales)"""
        await self._init_email_client()
        if not self.email_client:
            self.logger.warning("Cliente de Email no disponible, omitiendo nortificacion")
            return
        
        subject = f"🔥 [CRITICAL]: Amazon Manager - {error_context.error_type}"

        # Email prioritario
        await self.email_client.send_priority_email(
            subject=subject,
            html_body=self._generate_critical_html(error_context),
            recipients=st.setting_email_recipients['critical']
        )

    async def _send_warning_notification(self, message: str, context: Dict[str, Any]):
        """Enviar alerta crítica (múltiples canales)"""
        await self._init_email_client()
        if not self.email_client:
            self.logger.warning("Cliente de Email no disponible, omitiendo nortificacion")
            return
        
        subject = f"⚠️ [WARNING]: Amazon Manager"

        # Email prioritario
        await self.email_client.send_priority_email(
            subject=subject,
            html_body=self._generate_warning_html(message, context),
            recipients=st.setting_email_recipients['warnings']
        )

    def _generate_critical_html(self, error_context: ErrorContext) -> str:
        """HTML para alertas críticas"""
        return self._generate_error_html(error_context).replace(
            "🚨 Error en Amazon Manager",
            "🔥 ERROR CRITICO en Amazon Manager"
        )

    def _generate_warning_html(self, message: str, context: Dict[str, Any]) -> str:
        """HTML para warnings"""
        return f"""
                <html>
                <body style="font-family: Arial, sans-serif;">
                    <div style="max-width: 800px; margin: 0 auto;">
                        <h2 style="color: {'#f57c00'};">
                            ⚠️ Warning en Amazon Management
                        </h2>
                        
                        <div style="background: #f5f5f5; padding: 20px; border-radius: 8px; margin: 20px 0;">
                            <h3>📋 Detalles de advertencia</h3>
                            <p><strong>Mensaje:</strong> {message}</p>
                            <p><strong>Proceso:</strong> {context.get('process_mode', 'unknown')}</p>
                            <p><strong>Timestamp:</strong> {datetime.now()}</p>
                            
                            {f"<p><strong>Archivo:</strong> {context.get('file_path')}</p>" if context.get('file_path') else ""}
                            {f"<p><strong>Contexto adicional:</strong> {context}</p>" if context else ""}

                        </div>

                        <p style="margin-top: 20px; color: #666;">
                            Powered by Amazon Orders Processor | {datetime.now().isoformat()}
                        </p>
                    </div>
                </body>
                </html>
        """

    def _generate_error_html(self, error_context: ErrorContext) -> str:
        """Generar HTML mejorado para el email"""
        return f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <div style="max-width: 800px; margin: 0 auto;">
                <h2 style="color: {'#d32f2f' if error_context.level == AlertLevel.CRITICAL else '#f57c00'};">
                    🚨 Error en Amazon Management
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
                    {f"<p><strong>Mercado:</strong> {error_context.market_id}</p>" if error_context.market_id else ""}
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

                <p style="margin-top: 20px; color: #666;">
                Powered by Amazon Orders Processor | {datetime.now().isoformat()}
            </p>
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