
"""
FUNCIONALIDAD:
- Punto de entrada único para todos los procesos
- Parsea argumentos de línea de comandos
- Inicializa el procesador según el modo
- Maneja errores globales y logging inicial
"""

import asyncio
import argparse
from utils.unified_order_processor import UnifiedOrderProcessor, ProcessMode


async def main():
    # Configura el modo de ejecución según argumentos
    parser = argparse.ArgumentParser(description='Amazon Order Processor')
    parser.add_argument('--mode', choices=['daily_full', 'incremental', 'status_update', 'weekly_catchup'], required=True)
    args = parser.parse_args()
    
    # Inicializa y ejecuta el procesador
    processor = UnifiedOrderProcessor()
    await processor.run(ProcessMode(args.mode))

if __name__ == "__main__":
    asyncio.run(main())