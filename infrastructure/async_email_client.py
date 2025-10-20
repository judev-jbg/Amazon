import aiosmtplib as aiosmtpd
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List
import config.setting as st


"""
FUNCIONALIDAD:
- Envío asíncrono de emails
- Templates HTML mejorados
- Emails prioritarios para errores críticos
"""

class AsyncEmailClient:
    def __init__(self):
        self.smtp_server = "smtp.office365.com"
        self.smtp_port = 587
        self.sender_email = st.setting_email['sender']
        self.sender_password = st.setting_email['password']

    async def send_email(self, subject: str, html_body: str, recipients: List[str]):
        """Enviar email asíncrono"""
        try:
            message = MIMEMultipart('alternative')
            message['Subject'] = subject
            message['From'] = self.sender_email
            message['To'] = ', '.join(recipients)
            message['X-Priority'] = '2'
            
            html_part = MIMEText(html_body, 'html', 'utf-8')
            message.attach(html_part)
            
            # Crear contexto SSL
            context = ssl.create_default_context()
            
            # Enviar
            await aiosmtpd.send(
                message,
                hostname=self.smtp_server,
                port=self.smtp_port,
                start_tls=True,
                username=self.sender_email,
                password=self.sender_password
            )
                
        except Exception as e:
            print(f"Error enviando email: {e}")
        
    async def send_priority_email(self, subject: str, html_body: str, recipients: List[str]):
        """Enviar email prioritario para errores críticos"""
        await self.send_email(f"🔥 URGENT: {subject}", html_body, recipients)
        