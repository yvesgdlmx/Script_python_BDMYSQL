import csv
import re
from datetime import datetime
import mysql.connector

# Función para extraer la hora del nombre del campo
def extract_hour(field_name):
    hour_match = re.search(r"(\d{1,2}):(\d{2})", field_name)
    if hour_match:
        return f"{hour_match.group(1)}:{hour_match.group(2)}"
    return ""

# Función para extraer el número al principio del campo
def extract_num(field_name):
    num_match = re.search(r"^(\d+)", field_name)  # Buscar el patrón de números al principio del campo
    return num_match.group(1) if num_match else None

# Función para extraer la fecha desde el campo
def extract_date(field_name):
    parts = field_name.split('-')
    if len(parts) >= 2:
        day = parts[1]
        current_year = datetime.now().year
        current_month = datetime.now().month
        extracted_date = datetime(current_year, current_month, int(day)).strftime("%Y-%m-%d")
        return extracted_date
    return None

# Definir el archivo de entrada y salida
input_file = 'I:/VISION/scantotals_YVES.auto.tab'
output_file = 'C:/Users/Desarrollo/Desktop/TemporalesTab/scantotals2_modificado.tab'

# Definir una bandera para indicar si se deben empezar a procesar los datos
start_processing = False

# Lista para almacenar los datos procesados
data = []

with open(input_file, 'r') as original_file:
    reader = csv.reader(original_file, delimiter='\t')

    # Leer cada línea del archivo
    for row in reader:
        # Verificar si se ha encontrado la línea que indica el comienzo de los datos relevantes
        if start_processing or (row and row[0].startswith('Key')):
            start_processing = True
            # Verificar si la fila contiene datos válidos (no está vacía)
            if row and row[0].strip():
                name_field = row[0]
                
                # Extraer fecha, hora y número
                extracted_date = extract_date(name_field)
                extracted_hour = extract_hour(name_field)
                extracted_num = extract_num(name_field)

                # Insertar datos en la posición correcta
                row.insert(1, extracted_date)  # Insertar la fecha como segunda columna
                row.append(extracted_hour)  # Añadir la hora al final
                row.append(extracted_num)  # Añadir el número al final
                
                data.append(row)

# Escribir los datos procesados en el archivo de salida
with open(output_file, 'w', newline='') as new_file:
    writer = csv.writer(new_file, delimiter='\t')
    for row in data:
        writer.writerow(row)

# Cargar los datos en MySQL
try:
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='luis',
        database='optimex',
        allow_local_infile=True
    )

    cursor = connection.cursor()

    # Usar INSERT IGNORE para evitar duplicados
    sql_command = f"""
    LOAD DATA LOCAL INFILE '{output_file}'
    INTO TABLE generadores
    FIELDS TERMINATED BY '\\t'
    LINES TERMINATED BY '\\n'
    IGNORE 1 LINES
    (name, fecha, mean, median, hits, multi, shortest, longest, total, stddev, hour, num)
    SET fecha = STR_TO_DATE(fecha, '%Y-%m-%d')
    """

    cursor.execute(sql_command)
    connection.commit()

except mysql.connector.Error as err:
    print("Error al ejecutar el comando SQL:", err)

finally:
    if 'cursor' in locals() and cursor:
        cursor.close()
    if 'connection' in locals() and connection.is_connected():
        connection.close()

print("Carga de datos completada.")