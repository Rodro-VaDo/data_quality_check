# This script concatenates multiple Excel files from a specified folder,
# each containing several sheets, into a single Excel file with multiple sheets.
import os
import pandas as pd
import datetime

# Define the folder containing the Excel files
folder = r"C:\Users\rodrigo.vallejo\OneDrive - Aldeas Infantiles SOS Colombia\Bases de datos"
files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith('.xlsx')]

# Initialize empty lists to hold dataframes
participantes_list = []
servicios_list = []
familia_a_list = []
familia_o_list = []
defensor_list = []

# Load each file and sheet into dataframes
for file in files:
    try:
        participantes = pd.read_excel(file, sheet_name='participantes')
        servicios = pd.read_excel(file, sheet_name='servicios')
        familia_a = pd.read_excel(file, sheet_name='familia_acogida')
        familia_o = pd.read_excel(file, sheet_name='familia_origen')
        defensor = pd.read_excel(file, sheet_name='defensor')
        
# Add source file column
        participantes['source_file'] = os.path.basename(file)
        servicios['source_file'] = os.path.basename(file)
        familia_a['source_file'] = os.path.basename(file)
        familia_o['source_file'] = os.path.basename(file)
        defensor['source_file'] = os.path.basename(file)

# Append dataframes to lists
        participantes_list.append(participantes)
        servicios_list.append(servicios)
        familia_a_list.append(familia_a)
        familia_o_list.append(familia_o)
        defensor_list.append(defensor)
    except Exception as e:
        print(f"Error reading {file}: {e}")

# Concatenate all dataframes
all_participantes = pd.concat(participantes_list, ignore_index=True)
all_servicios = pd.concat(servicios_list, ignore_index=True)
all_familia_acogida = pd.concat(familia_a_list, ignore_index=True)
all_familia_origen = pd.concat(familia_o_list, ignore_index=True)
all_defensores = pd.concat(defensor_list, ignore_index=True)

# Filter out rows with empty IDs
all_participantes = all_participantes.dropna(subset=['ID DEL PARTICIPANTE (PRIMARIA)'])
all_servicios = all_servicios.dropna(subset=['ID DEL PARTICIPANTE'])
all_familia_acogida = all_familia_acogida.dropna(subset=['ID FAMILIA / CASA DE ACOGIDA'])
all_familia_origen = all_familia_origen.dropna(subset=['ID FAMILIA DE ORIGEN EN DFE'])
all_defensores = all_defensores.dropna(subset=['ID DEFENSORÍA'])

# Obtain current date for filename
fecha = datetime.datetime.now().strftime("%Y%m%d")

# Save all DataFrames to a single Excel file with multiple sheets
output_path = fr"C:\Users\rodrigo.vallejo\OneDrive - Aldeas Infantiles SOS Colombia\Bases de datos\Consolidados\all_participants_service_families_data_{fecha}.xlsx"
with pd.ExcelWriter(output_path) as writer:
    all_participantes.to_excel(writer, sheet_name='participantes', index=False)
    all_servicios.to_excel(writer, sheet_name='servicios', index=False)
    all_familia_acogida.to_excel(writer, sheet_name='familia_acogida', index=False)
    all_familia_origen.to_excel(writer, sheet_name='familia_origen', index=False)
    all_defensores.to_excel(writer, sheet_name='defensores', index=False)
