# Project A — Carga Masiva a Teradata

Scripts Python para la carga masiva y ultra-rápida de archivos CSV a tablas de Teradata, con soporte para limpieza de caracteres especiales y modo diagnóstico.

---

## Estructura del proyecto

```
Project_A/
├── cargueTD_A.py                          # Script principal de carga (tablas permanentes / Claro Música)
├── cargueTD_B.py                          # Script secundario de carga (tablas temporales / campañas)
├── requirements.txt                        # Dependencias Python
├── registros_con_caracteres_especiales.csv # Salida del modo análisis
├── registros_fallidos.csv                  # Registros que no pudieron cargarse
├── data/                                   # Archivos de entrada (.csv / .txt)
├── logs/                                   # Logs de ejecución
└── output/                                 # Archivos de salida procesados
```

---

## Prerrequisitos

- Python 3.8+
- Acceso a la red corporativa / VPN para conectar con Teradata
- Archivo `Config.json` en el directorio **padre** del proyecto con las credenciales de Teradata:

```json
{
  "host": "<HOST_TERADATA>",
  "user": "<USUARIO>",
  "password": "<CONTRASEÑA>"
}
```

> El archivo `Config.json` **no se incluye en el repositorio** por seguridad.

---

## Instalación

```bash
# 1. Clonar el repositorio
git clone https://github.com/PachecoHitss/Project_A.git
cd Project_A

# 2. Crear y activar el entorno virtual
python -m venv venv

# Windows (PowerShell)
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\venv\Scripts\Activate.ps1

# 3. Instalar dependencias
pip install -r requirements.txt
```

---

## Configuración antes de ejecutar

Edita la sección `# 1. CONFIGURACIÓN PRINCIPAL` al inicio del script:

| Variable | Descripción |
|---|---|
| `TABLE_NAME_TO_RECREATE` | Nombre completo de la tabla Teradata destino (ej. `innovacion.TBL_CLARO_MUSICA_10M`) |
| `BATCH_SIZE` | Tamaño del lote de inserción (default: `250000`) |
| `INPUT_DELIMITER` | Delimitador del CSV de entrada (default: `;`) |
| `SCHEMA_DEFINITION` | Diccionario columna → tipo SQL de la tabla destino |
| `PRIMARY_INDEX_COLUMN` | Columna usada como Primary Index en Teradata |

---

## Uso

### Modo Carga (por defecto)
Limpia automáticamente los caracteres no ASCII y carga los datos a Teradata.

```bash
python cargueTD_A.py data/mi_archivo.csv
```

### Modo Análisis
Identifica y guarda en `registros_con_caracteres_especiales.csv` todas las filas que contienen caracteres no estándar, **sin cargar** nada a Teradata.

```bash
python cargueTD_A.py data/mi_archivo.csv --analizar
```

### Script B (campañas / tablas temporales)
Mismos modos de operación, apunta a tablas temporales de campaña:

```bash
python cargueTD_B.py data/mi_archivo.csv
python cargueTD_B.py data/mi_archivo.csv --analizar
```

---

## Flujo recomendado

```
1. Configurar TABLE_NAME_TO_RECREATE y SCHEMA_DEFINITION en el script
2. Ejecutar en modo --analizar para revisar caracteres problemáticos
3. Revisar registros_con_caracteres_especiales.csv si hay hallazgos
4. Ejecutar en modo carga normal
5. Revisar logs/ para confirmar resultado y ver registros_fallidos.csv si aplica
```

---

## Logs

Los logs se generan automáticamente en la carpeta `logs/` con el nombre del archivo de entrada y la fecha de ejecución. Contienen el detalle de lotes procesados, registros insertados y errores.

---

## Dependencias

| Paquete | Uso |
|---|---|
| `pandas` | Lectura y procesamiento de archivos CSV en chunks |
| `teradatasql` | Conexión y ejecución de queries en Teradata |
