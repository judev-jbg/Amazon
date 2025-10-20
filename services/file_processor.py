# services/file_processor.py
import numpy as np
import pandas as pd
from pathlib import Path
import asyncio


class FileProcessor:
    """Utilidades para procesamiento de archivos"""

    async def read_excel_file(self, file_path: Path) -> pd.DataFrame:
        """Leer archivo Excel de forma asíncrona"""
        try:
            # Usar thread pool para operación bloqueante
            df = await asyncio.to_thread(pd.read_excel, file_path)
            df = df.replace({np.nan: None})
            return df
        except Exception as e:
            raise Exception(
                f"Error leyendo archivo Excel {file_path}: {str(e)}")

    async def read_shipment_file(self, file_path: Path) -> pd.DataFrame:
        """Leer archivo de envíos con columnas específicas"""
        try:
            # Columnas específicas para shipments
            df = await asyncio.to_thread(pd.read_excel, file_path, dtype=object)

            # Filtrar solo columnas relevantes
            shipment_columns = [
                "codbar", "Expedicion", "Referencia", "DptoDst", "id_order_ps", "reference_ps"]
            available_columns = [
                col for col in shipment_columns if col in df.columns]

            return df[available_columns] if available_columns else df

        except Exception as e:
            raise Exception(
                f"Error leyendo archivo de envíos {file_path}: {str(e)}")
