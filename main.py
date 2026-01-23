import functions_framework
from google.cloud import storage
from google.cloud import firestore

from processors import (
  form_100,
  form_110,
  form_120,
  form_150,
  form_170,
  form_180,
  form_190,
  form_200,
  form_220,
  form_300,
  form_400,
  form_500,
  form_600
)

storage_client = storage.Client()
db = firestore.Client()

@functions_framework.cloud_event
def router_process(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    if file_name.endswith("/"): return

    if not file_name.startswith('forms/'):
        print(f"⏩ Archivo omitido (Fuera de carpeta forms/): {file_name}")
        return

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    try:
        content = blob.download_as_text(encoding="utf-8")
    except Exception as e:
        print(f"Error descargando: {e}")
        return

    # 2. Enrutamiento (Router)
    name_upper = file_name.split("_")[-2].upper()

    match name_upper:
        case "FORM100":
            form_100.process_100(content, file_name, db)
        case "FORM110":
            form_110.process_110(content, file_name, db)
        case "FORM120":
            form_120.process_120(content, file_name, db)
        case "FORM150":
            form_150.process_150(content, file_name, db)
        case "FORM170":
            form_170.process_170(content, file_name, db)
        case "FORM180":
            form_180.process_180(content, file_name, db)
        case "FORM190":
            form_190.process_190(content, file_name, db)
        case "FORM200":
            form_200.process_200(content, file_name, db)
        case "FORM220":
            form_220.process_220(content, file_name, db)
        case "FORM300":
            form_300.process_300(content, file_name, db)
        case "FORM400":
            form_400.process_400(content, file_name, db)
        case "FORM500":
            form_500.process_500(content, file_name, db)
        case "FORM600":
            form_600.process_600(content, file_name, db)
        case "FORM900":
            print("900 omititdo")
        case _:
            print(f"Formato no reconocido: {file_name}")
