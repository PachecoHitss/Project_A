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
TABLE_NAME_TO_RECREATE = 'innovacion.TMP_EMAPAQ_MUSICA_202509'
#TABLE_NAME_TO_RECREATE = 'innovacion.TMP_ACTUALIZA_INAPP_DEF'
#TABLE_NAME_TO_RECREATE = 'innovacion.TMP_ACTUALIZA_PUSH_DEF'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_POTENCIAL_R_99K_CON_EXCLUSIONES'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_POTENCIAL_R_39K_CON_EXCLUSIONES'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_POTENCIAL_R_99K_SIN_EXCLUSIONES'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_POTENCIAL_R_39K_SIN_EXCLUSIONES'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_OTRAS_CIUDADES'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_BAJA_PENETRACION'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_CABLERAS'
# TABLE_NAME_TO_RECREATE = 'innovacion.TMP_POTENCIAL_R_99K_SIN_EX'

BATCH_SIZE = 250000
INPUT_DELIMITER = ';'

SCHEMA_DEFINITION = {
    'tele_numb': 'VARCHAR(50)'
#    ,'EMAIL':     'VARCHAR(500)'
}
PRIMARY_INDEX_COLUMN = 'tele_numb'

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
    logging.info("--- INICIANDO MODO ANÁLISIS ---")
    logging.info(f"Buscando registros con caracteres no estándar en: {input_file_path}")

    dtype_mapping = {col: str for col in SCHEMA_DEFINITION.keys()}
    chunk_iterator = pd.read_csv(
        input_file_path, delimiter=INPUT_DELIMITER, 
        usecols=list(SCHEMA_DEFINITION.keys()), header=0, 
        dtype=dtype_mapping, encoding='latin-1', 
        chunksize=BATCH_SIZE, on_bad_lines='warn'
    )
    
    found_records = []
    total_processed = 0
    
    # Eliminar archivo de análisis anterior si existe
    if os.path.exists(BAD_CHARS_FILE):
        os.remove(BAD_CHARS_FILE)

    for i, chunk in enumerate(chunk_iterator):
        logging.info(f"Analizando lote {i+1}...")
        
        # Guardamos una copia del chunk original para comparar
        original_chunk = chunk.copy()

        # Aplicamos la sanitización
        for col, dtype in SCHEMA_DEFINITION.items():
            if 'VARCHAR' in dtype.upper():
                chunk[col] = chunk[col].fillna('').astype(str).apply(sanitize_string)
        
        # Comparamos el chunk original con el sanitizado
        # `ne` significa "not equal". `any(axis=1)` devuelve True para cada fila donde al menos una celda cambió.
        diff_mask = chunk.ne(original_chunk).any(axis=1)
        
        problematic_records = original_chunk[diff_mask]
        
        if not problematic_records.empty:
            found_records.append(problematic_records)
        
        total_processed += len(original_chunk)
    
    if found_records:
        all_problematic_df = pd.concat(found_records)
        num_found = len(all_problematic_df)
        logging.info(f"Análisis completo. Se encontraron {num_found} registros con caracteres no estándar de un total de {total_processed} procesados.")
        logging.info(f"Guardando los registros problemáticos en: {BAD_CHARS_FILE}")
        all_problematic_df.to_csv(BAD_CHARS_FILE, index=False, sep=';', encoding='utf-8-sig')
    else:
        logging.info("Análisis completo. No se encontraron registros con caracteres no estándar.")

def load_data(input_file_path, td_config):
    """
    Modo carga: Limpia y carga los datos a Teradata.
    """
    logging.info("--- INICIANDO MODO CARGA ---")
    TD_HOST, TD_USER, TD_PASSWORD = td_config['TD_HOST'], td_config['TD_USER'], td_config['TD_PASSWORD']

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
    chunk_iterator = pd.read_csv(
        input_file_path, delimiter=INPUT_DELIMITER, 
        usecols=list(SCHEMA_DEFINITION.keys()), header=0, 
        dtype=dtype_mapping, encoding='latin-1', 
        chunksize=BATCH_SIZE, on_bad_lines='warn'
    )

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