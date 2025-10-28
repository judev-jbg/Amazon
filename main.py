
"""
FUNCIONALIDAD:
- Punto de entrada único para todos los procesos
- Parsea argumentos de línea de comandos
- Inicializa el procesador según el modo
- Maneja errores globales y logging inicial
"""
import asyncio
import logging
import sys
import argparse
from infrastructure.dependency_injection.container import DependencyContainer
from utils.unified_order_processor import UnifiedOrderProcessor, ProcessMode
from infrastructure.error_handling import EnhancedErrorHandler


async def main():
    EnhancedErrorHandler()
    logger = logging.getLogger('AmazonManagement')
    try:

        # Configura el modo de ejecución según argumentos
        logger.info("=" * 46)
        logger.info("[INICIO] AMAZON MANAGEMENT")
        parser = argparse.ArgumentParser(
            description='Amazon Seller Management')
        parser.add_argument('--mode', choices=['daily_full', 'incremental', 'status_update',
                            'weekly_catchup', 'order_details', 'shipment_update', 'inventory_activate', 'inventory_deactivate', 'inventory_friday', 'inventory_monday', 'product_verification'], required=True)
        args = parser.parse_args()
        logger.info(f"📢 Procesamiento modo {args.mode} iniciado.")

        # Crear container de dependencias
        container = DependencyContainer.create_production_container()

        # Resolver procesador desde container
        processor = container.resolve(UnifiedOrderProcessor)

        # Ejecutar
        success = await processor.run(ProcessMode(args.mode))
        if success:
            logger.info(
                f"✅ Procesamiento modo {args.mode} completado con éxito.")
        else:
            logger.info(
                f"❌ El procesamiento modo {args.mode} se completó con errores.")
            sys.exit(1)
        logger.info("[FIN] AMAZON MANAGEMENT")
        logger.info("=" * 46)
    except Exception as e:
        logger.critical(f"Error crítico en main: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
