from bs4 import BeautifulSoup
import re
from shared.utils import clean_text, parse_float, generate_slug
from shared.firestore import insert_entidad, insert_convocatoria, insert_item

def process_120(html_content, file_name, db):
    print(f"--- Procesando Formulario 120: {file_name} ---")
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
    except Exception as e:
        print(f"Error parseando HTML en {file_name}: {e}")
        return

    try:
        convocatoria_cuce = soup.find('td', class_='FormularioCUCE')
        if convocatoria_cuce:
            cuce = clean_text(convocatoria_cuce.get_text())
            insert_convocatoria(
                db,
                cuce=cuce,
                estado="Publicado",
                forms="FORM120"
            )
        else:
            print(f"Advertencia: No se encontró CUCE en {file_name}")
            return

        print(f"✅ Formulario 120 procesado: {cuce}")

    except Exception as e:
        print(f"❌ Error fatal procesando {file_name}: {e}")