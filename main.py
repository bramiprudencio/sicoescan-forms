import functions_framework
from google.cloud import storage
from google.cloud import firestore

# Importamos los módulos de nuestras carpetas
from processors import form_100, form_110, form_170, form_400, form_500

# Inicializamos clientes una sola vez (Global Scope)
storage_client = storage.Client()
db = firestore.Client()

@functions_framework.cloud_event
def router_process(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    if file_name.endswith("/"): return

    # 1. Descargar archivo
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    try:
        content = blob.download_as_text(encoding="utf-8")
    except Exception as e:
        print(f"Error descargando: {e}")
        return

    # 2. Enrutamiento (Router)
    name_upper = file_name.upper()

    if "FORM100" in name_upper:
        form_100.process_100(content, file_name, db)
       
    elif "FORM110" in name_upper:
        form_110.process_110(content, file_name, db)
    ''' 
    elif "FORM170" in name_upper:
        form_170.process_170(content, file_name, db)

    elif "FORM400" in name_upper:
        form_400.process_400(content, file_name, db)
    
    elif "FORM500" in name_upper:
        form_500.process_500(content, file_name, db)
    
    else:
        print(f"Formato no reconocido: {file_name}")
    '''
    