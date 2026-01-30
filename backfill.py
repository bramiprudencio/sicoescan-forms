import requests
from google.cloud import firestore
import sys

# Importamos TUS módulos procesadores
from processors import form_100, form_110, form_170, form_400, form_500

# Configuración
ARCHIVO_LISTA = "100_faltantes.txt" # El archivo con la lista de nombres
BASE_URL = "https://storage.googleapis.com/sicoescan/forms/" # URL base de tu bucket

# Inicializar Firestore (usará tus credenciales locales o el json)
try:
  db = firestore.Client()
except Exception:
  # Si usas un archivo json explícito:
  from google.oauth2 import service_account
  cred = service_account.Credentials.from_service_account_file('./firebase-credentials.json')
  db = firestore.Client(credentials=cred)

def run_backfill():
  print(f"🚀 Iniciando procesamiento desde lista: {ARCHIVO_LISTA}")
  
  try:
    with open(ARCHIVO_LISTA, "r", encoding="utf-8") as f:
      lines = f.readlines()
  except FileNotFoundError:
    print(f"❌ No se encontró el archivo {ARCHIVO_LISTA}")
    return

  contador = 0
  errores = 0

  for line in lines:
    file_name = line.strip()
    if not file_name: continue # Saltar líneas vacías
    
    url = f"{BASE_URL}{file_name}"


    print(f"📄 [{contador + 1}] Procesando: {file_name}")

    try:
      # --- 1. TU MÉTODO ORIGINAL (Requests) ---
      response = requests.get(url)
      
      if response.status_code != 200:
        print(f"⚠️ Error descargando {url}: Status {response.status_code}")
        errores += 1
        continue
        
      response.encoding = "utf-8"
      html_content = response.text
      
      # --- 2. ENRUTADOR MODULAR ---
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
        print(f"⏩ Salta: Formulario no reconocido")

    except Exception as e:
      print(f"❌ ERROR procesando {file_name}: {e}")
      errores += 1
      with open("backfill_errors.txt", "a") as err_file:
        err_file.write(f"{file_name}\n")

    contador += 1

  print(f"\n✅ Proceso completado.")
  print(f"Total procesados: {contador}")
  print(f"Total errores: {errores}")

if __name__ == "__main__":
  run_backfill()