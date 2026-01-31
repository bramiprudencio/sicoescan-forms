import requests
from google.cloud import firestore
import concurrent.futures # <--- LA CLAVE PARA LA VELOCIDAD
from tqdm import tqdm # Barra de progreso
import sys

# Importamos TUS módulos procesadores
from processors import (
  form_100,
  form_110,
  form_170,
  form_400,
  form_500
)

# Configuración
ARCHIVO_LISTA = "guias/400_1.txt"
BASE_URL = "https://storage.googleapis.com/sicoescan/forms/"
NUM_HILOS = 20

# Inicializar Firestore (Firestore Client es thread-safe, podemos usar una instancia global)
try:
  db = firestore.Client()
except Exception:
  from google.oauth2 import service_account
  cred = service_account.Credentials.from_service_account_file('./firebase-credentials.json')
  db = firestore.Client(credentials=cred)

def procesar_un_archivo(linea_cruda):
  file_name = linea_cruda.strip()
  if not file_name: return "VACIO"

  url = f"{BASE_URL}{file_name}"

  try:
    response = requests.get(url, timeout=10)
    
    if response.status_code != 200:
      return f"ERROR_DOWNLOAD_{response.status_code}"
      
    response.encoding = "utf-8"
    html_content = response.text
    name_upper = file_name.upper()

    if "FORM100" in name_upper:
      form_100.process_100(html_content, file_name, db)
    elif "FORM110" in name_upper:
      form_110.process_110(html_content, file_name, db)
    elif "FORM170" in name_upper:
      form_170.process_170(html_content, file_name, db)
    elif "FORM400" in name_upper:
      form_400.process_400(html_content, file_name, db)
    elif "FORM500" in name_upper:
      form_500.process_500(html_content, file_name, db)
    else:
      return "SKIP_UNKNOWN"

    return "OK"

  except Exception as e:
    with open("guias/backfill_errors.txt", "a") as f:
      f.write(f"{file_name} - {str(e)}\n")
    return f"ERROR_EXCEPTION"

def run_backfill_rapido():
  print(f"🚀 Iniciando procesamiento PARALELO con {NUM_HILOS} hilos.")
  
  try:
    with open(ARCHIVO_LISTA, "r", encoding="utf-8") as f:
      lines = f.readlines()
  except FileNotFoundError:
    print(f"❌ No se encontró el archivo {ARCHIVO_LISTA}")
    return

  files_to_process = [line for line in lines if line.strip()]
  total_files = len(files_to_process)
  
  with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_HILOS) as executor:
    results = list(tqdm(executor.map(procesar_un_archivo, files_to_process), total=total_files, unit="form"))

  ok_count = results.count("OK")
  errores = total_files - ok_count
  
  print(f"\n✅ Proceso completado.")
  print(f"Total procesados con éxito: {ok_count}")
  print(f"Total fallos/skips: {errores}")

if __name__ == "__main__":
  run_backfill_rapido()