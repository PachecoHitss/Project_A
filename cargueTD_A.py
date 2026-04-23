# -*- coding: utf--8 -*-
"""
Script DEFINITIVO para la carga masiva y ultra-rápida de datos a Teradata.
Incluye dos modos de operación:
1. MODO ANÁLISIS: Identifica y guarda registros con caracteres no estándar.
   Uso: python inicio_A.py mi_archivo.csv --analizar
2. MODO CARGA (por defecto): Limpia automáticamente y carga los datos a Teradata.
   Uso: python inicio_A.py mi_archivo.csv
"""
import pandas as pd
import teradatasql
import os
import logging
import json
import argparse
import time
import sys


# --- 1. CONFIGURACIÓN PRINCIPAL ---

# TABLAS TEMPORALES

# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_migra'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_CERTIFICA_TYT'
# TABLE_NAME_TO_RECREATE = 'innovacion.TBL_OPTIN_ACTIVO_SALESFORCE'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_CONVERGENCIA_COSTA'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_CERTIFICA_MIGRA'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_CERTIFICA_UPSELLING'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_CLARO_DRIVE_EPOCA'
# TABLE_NAME_TO_RECREATE = 'innovacion.TBL_CARGUE_VAS'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_CERTIFICA_VAS'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_CUPO_CARRO_ABANDONADO'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_ACTUALIZA_INAPP_DEF'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_ACTUALIZA_EMAIL_DEF'
# TABLE_NAME_TO_RECREATE = 'innovacion.tbl_clientes_cinemark_Interesados'
# TABLE_NAME_TO_RECREATE = 'innovacion.tbl_clientes_cinemark_remarketing'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_EMPAQUETADOS_251031'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_CLARO_DRIVE_dejoUsar'

# TABLE_NAME_TO_RECREATE = 'innovacion.TBL_POTENCIAL_DIGITAL'

# TABLAS PERMANENTES
# TABLE_NAME_TO_RECREATE = 'innovacion.TBL_CLARO_MUSICA_EMPAQ'
TABLE_NAME_TO_RECREATE = 'innovacion.TBL_CLARO_MUSICA_10M'

# TABLAS CLARO PAY NUEVAS 202601
# TABLE_NAME_TO_RECREATE = 'innovacion.TBL_MigracionDepositoPay'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_CLARO_PAY_DEPOSITO'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_CLARO_PAY_PUNTORED10K'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_CLARO_PAY_DBM_SERVICIO'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_CLARO_PAY_DBM_COMERCIAL'
# TABLE_NAME_TO_RECREATE = 'innovacion.TBL_ClaroPay_ConDBM'
# TABLE_NAME_TO_RECREATE = 'innovacion.TBL_ClaroPay_SinDBM'

# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_ADOPCION_ELIMINADOS_APP'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_ADOPCION_SIN_LOGIN_6m'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_ADOPCION_DIGITAL'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_ADOPCION_EMPRESAS'

BATCH_SIZE = 250000
INPUT_DELIMITER = ';'

# ATENCIÓN: Es muy probable que necesites ajustar las claves de este diccionario
# basándote en la salida del diagnóstico.
SCHEMA_DEFINITION = {
    'TELE_NUMB'	:	'VARCHAR(50)'
    }
PRIMARY_INDEX_COLUMN = 'TELE_NUMB'

# --- 2. CONFIGURACIÓN DE RUTAS ---
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(PROJECT_DIR)
CONFIG_FILE = os.path.join(PARENT_DIR, 'Config.json')
LOG_DIR = os.path.join(PROJECT_DIR, 'logs')
# Archivo de salida para el modo análisis
BAD_CHARS_FILE = os.path.join(PROJECT_DIR, 'registros_con_caracteres_especiales.csv')

def sanitize_string(text):
    """Codifica a ASCII ignorando caracteres no válidos."""
    if isinstance(text, str):
        return text.encode('ascii', 'ignore').decode('ascii')
    return text

def analyze_data(input_file_path):
    """
    Modo análisis: Lee el archivo e identifica filas con caracteres no traducibles.
    """
    # ... (esta función no necesita cambios, la omito por brevedad)
    pass

def load_data(input_file_path, td_config):
    """
    Modo carga: Limpia y carga los datos a Teradata.
    """
    logging.info("--- INICIANDO MODO CARGA ---")
    TD_HOST, TD_USER, TD_PASSWORD = td_config['TD_HOST'], td_config['TD_USER'], td_config['TD_PASSWORD']

    # --- CÓDIGO DE DIAGNÓSTICO AÑADIDO ---
    try:
        # Intentar leer el encabezado con diferentes codificaciones
        for enc in ['utf-16', 'utf-8', 'latin-1']:
            try:
                df_head = pd.read_csv(input_file_path, delimiter=INPUT_DELIMITER, encoding=enc, nrows=1)
                logging.info(f"Diagnóstico de Columnas - Nombres detectados en el CSV ({enc}): {df_head.columns.tolist()}")
                break
            except Exception as e:
                logging.warning(f"No se pudo leer el encabezado con encoding {enc}: {e}")
        else:
            logging.error(f"Diagnóstico de Columnas - No se pudo leer el encabezado del archivo con ningún encoding probado.")
            df_head = None
    except Exception as e:
        logging.error(f"Diagnóstico de Columnas - Error inesperado: {e}")
    # --- FIN DEL CÓDIGO DE DIAGNÓSTICO ---


    # --- Preparar Base de Datos ---
    logging.info(f"Preparando la tabla: {TABLE_NAME_TO_RECREATE}")
    with teradatasql.connect(host=TD_HOST, user=TD_USER, password=TD_PASSWORD) as connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(f"DROP TABLE {TABLE_NAME_TO_RECREATE};")
                logging.info("Tabla anterior eliminada.")
            except teradatasql.Error as e:
                if "does not exist" in str(e): logging.info("La tabla no existía, se procederá a crearla.")
                else: raise e
            
            column_definitions = [f'"{name}" {datatype}' for name, datatype in SCHEMA_DEFINITION.items()]
            create_table_sql = f"""CREATE MULTISET TABLE {TABLE_NAME_TO_RECREATE}, NO FALLBACK ({', '.join(column_definitions)}) PRIMARY INDEX ( "{PRIMARY_INDEX_COLUMN}" );"""
            cursor.execute(create_table_sql)
            logging.info("Tabla recreada como MULTISET.")

    # --- Leer y Cargar Datos ---
    logging.info("Iniciando lectura y carga por lotes...")
    dtype_mapping = {col: str for col in SCHEMA_DEFINITION.keys()}
    # Intentar leer el archivo completo con diferentes codificaciones
    chunk_iterator = None
    for enc in ['utf-16', 'utf-8', 'latin-1']:
        try:
            chunk_iterator = pd.read_csv(
                input_file_path, delimiter=INPUT_DELIMITER,
                usecols=list(SCHEMA_DEFINITION.keys()), header=0,
                dtype=dtype_mapping,
                encoding=enc,
                chunksize=BATCH_SIZE, on_bad_lines='warn'
            )
            logging.info(f"Lectura de datos exitosa con encoding {enc}.")
            break
        except Exception as e:
            logging.warning(f"No se pudo leer el archivo con encoding {enc}: {e}")
    if chunk_iterator is None:
        logging.error("No se pudo leer el archivo con ningún encoding probado. Proceso abortado.")
        return

    with teradatasql.connect(host=TD_HOST, user=TD_USER, password=TD_PASSWORD) as connection:
        with connection.cursor() as cursor:
            column_list_sql = ', '.join([f'"{col}"' for col in SCHEMA_DEFINITION.keys()])
            placeholders = ', '.join(['?' for _ in SCHEMA_DEFINITION.keys()])
            insert_sql = f'INSERT INTO {TABLE_NAME_TO_RECREATE} ({column_list_sql}) VALUES ({placeholders});'
            
            total_rows_inserted = 0
            for i, chunk in enumerate(chunk_iterator):
                for col, dtype in SCHEMA_DEFINITION.items():
                    chunk[col] = chunk[col].fillna('').astype(str).str.strip()
                    if 'VARCHAR' in dtype.upper():
                        chunk[col] = chunk[col].apply(sanitize_string)
                
                if chunk.empty: continue
                    
                data_to_insert = list(chunk.itertuples(index=False, name=None))
                cursor.executemany(insert_sql, data_to_insert)
                total_rows_inserted += len(data_to_insert)
                logging.info(f"Lote {i+1} cargado. Total de filas insertadas: {total_rows_inserted}")
    
    logging.info("Carga de datos completada.")

def main():
    # ... (esta función no necesita cambios, la omito por brevedad)
    parser = argparse.ArgumentParser(description="Analiza o carga datos masivos a Teradata.")
    parser.add_argument("input_filename", help="Nombre del archivo de entrada (en 'data').")
    parser.add_argument("--analizar", action="store_true", help="Activa el modo de análisis para encontrar caracteres no estándar.")
    args = parser.parse_args()

    os.makedirs(LOG_DIR, exist_ok=True)
    log_filename = f"ejecucion_{os.path.splitext(args.input_filename)[0]}.log"
    log_file_path = os.path.join(LOG_DIR, log_filename)
    logging.basicConfig(handlers=[logging.FileHandler(log_file_path, 'w', 'utf-8'), logging.StreamHandler(sys.stdout)], level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info(f"--- Inicio del proceso para: {args.input_filename} ---")
    start_time = time.time()

    input_file_path = os.path.join(PROJECT_DIR, 'data', args.input_filename)
    if not os.path.exists(input_file_path):
        logging.error(f"El archivo de entrada no se encontró en: {input_file_path}")
        return

    try:
        if args.analizar:
            analyze_data(input_file_path)
        else:
            logging.info("Cargando configuración para el modo de carga...")
            if not os.path.exists(CONFIG_FILE): raise FileNotFoundError(f"'{CONFIG_FILE}' no se encontró.")
            with open(CONFIG_FILE, 'r') as f: config = json.load(f)
            td_config = config.get('teradata')
            if not td_config: raise EnvironmentError("Sección 'teradata' no encontrada en Config.json.")
            if not all(k in td_config for k in ['TD_HOST', 'TD_USER', 'TD_PASSWORD']):
                raise EnvironmentError("Claves faltantes en la sección 'teradata'.")
            
            load_data(input_file_path, td_config)

    except Exception as e:
        logging.error("--- ERROR CRÍTICO ---", exc_info=True)
    
    finally:
        duration = time.time() - start_time
        logging.info(f"--- Proceso finalizado en {duration:.2f} segundos ({duration/60:.2f} minutos). ---")

if __name__ == "__main__":
    main()