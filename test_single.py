import requests
import pprint
from bs4 import BeautifulSoup
from shared.utils import slugify, clean_text
from processors import form_100, form_110, form_170, form_400, form_500

# ==========================================
# 1. CLASES "MOCK" MEJORADAS
# ==========================================
class MockDocument:
    def __init__(self, doc_id="dummy_id", data=None):
        self.id = doc_id
        self.exists = True
        # Guardamos datos internos para simular lectura
        self._data = data if data else {"estado": "Publicado", "departamento": "TEST_DEPTO"}

    def set(self, data, merge=False):
        print(f"\n🔵 [MOCK DB] SET (ID: {self.id}):")
        pprint.pprint(data)

    def update(self, data):
        print(f"\n🟠 [MOCK DB] UPDATE (ID: {self.id}):")
        # Mostramos qué estamos actualizando
        pprint.pprint(data)

    def to_dict(self):
        return self._data

    def get(self, key):
        return self._data.get(key)

class MockCollection:
    def __init__(self, name):
        self.name = name
        self.preloaded_docs = [] # Lista para inyectar documentos falsos

    def document(self, doc_id):
        return MockDocument(doc_id)

    def where(self, filter=None, **kwargs):
        return self 

    def stream(self):
        print(f"   [MOCK DB] Consultando colección '{self.name}'...")
        
        # Si tenemos documentos precargados (para Form 500), devolvemos esos
        if self.preloaded_docs:
            print(f"   [MOCK DB] -> Devolviendo {len(self.preloaded_docs)} documentos simulados para matching.")
            return self.preloaded_docs
        
        # Si no, devolvemos uno genérico
        doc = MockDocument("item_simulado_generico")
        doc._data = {"slug": "item-generico", "descripcion": "Item Generico"}
        return [doc]
    
    def get(self):
        return self.stream()

class MockFirestoreClient:
    def __init__(self):
        self._collections = {}

    def collection(self, name):
        if name not in self._collections:
            self._collections[name] = MockCollection(name)
        return self._collections[name]

# ==========================================
# 2. AYUDANTE PARA "ENGAÑAR" AL FORM 500
# ==========================================
def pre_scan_items(html_content, mock_db):
    """
    Lee el HTML, busca descripciones usando decode_contents (igual que el procesador)
    y llena la BD falsa.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    slugs_debug = []

    # Buscamos la tabla del Form 500
    section = soup.find("font", string=lambda t: t and "RECEPCIÓN DE BIENES" in t)
    if section:
        rows = section.find_parent("table").find_all("tr")
        for row in rows[2:]: # Saltamos headers
            cols = row.find_all("td")
            if len(cols) > 3:
                # -----------------------------------------------------------
                # CORRECCIÓN: Usamos decode_contents() para incluir las negritas
                # y que el slug coincida exactamente con tu procesador.
                # -----------------------------------------------------------
                raw_html = cols[3].decode_contents().strip()
                desc_limpia = " ".join(raw_html.split()) # Misma limpieza ligera
                
                # Importante: Usa TU función clean_text si ella quita HTML antes de slugify
                # Si tu procesador hace slugify(clean_text(val)), haz lo mismo aquí.
                # Asumiremos que el slug se hace sobre el texto limpio de tags para el ID:
                
                slug = slugify(clean_text(desc_limpia))
                
                # Guardamos en la lista para depurar
                slugs_debug.append(slug)

                # Creamos el documento falso
                # Usamos un ID numérico secuencial simulando CUCE_#
                doc_id = f"MOCK_ID_{len(slugs_debug)}" 
                doc = MockDocument(doc_id=doc_id)
                doc._data = {
                    "slug": slug, 
                    "descripcion": desc_limpia, # Guardamos con HTML para que se vea real
                    "estado": "Publicado" 
                }
                mock_db.collection("items").preloaded_docs.append(doc)
    
    print(f"🧪 [TEST PRE-SCAN] Se inyectaron {len(slugs_debug)} items.")
    if slugs_debug:
        print(f"   Ejemplo de Slug generado en Mock: '{slugs_debug[0]}'")
        
# ==========================================
# 3. CONFIGURACIÓN Y EJECUCIÓN
# ==========================================24-1704-00-1513873-1-1

TEST_URL = "https://storage.googleapis.com/sicoescan/forms/24-1704-00-1513873-1-1_FORM500_1.html"

def run_test():
    print(f"📄 URL: {TEST_URL}\n")

    try:
        response = requests.get(TEST_URL)
        response.encoding = "utf-8"
        if response.status_code != 200:
            print(f"❌ Error descargando: {response.status_code}")
            return
        html_content = response.text
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        return

    file_name = TEST_URL.split('/')[-1]
    name_upper = file_name.upper()
    
    # Inicializar BD Falsa
    mock_db = MockFirestoreClient()

    print("-" * 60)

    try:
        # LOGICA ESPECIAL: Si es 500 o 170, pre-llenamos la BD
        if "FORM500" in name_upper:
            print("⚙️ Preparando entorno para FORM 500...")
            pre_scan_items(html_content, mock_db) # <--- AQUÍ ESTÁ EL TRUCO
            form_500.process_500(html_content, file_name, mock_db)
        
        elif "FORM170" in name_upper:
            print("⚙️ Preparando entorno para FORM 170...")
            # Podrías hacer una función similar pre_scan para el 170 si lo necesitas
            form_170.process_170(html_content, file_name, mock_db)

        elif "FORM100" in name_upper:
            form_100.process_100(html_content, file_name, mock_db)
        
        elif "FORM110" in name_upper:
            form_110.process_110(html_content, file_name, mock_db)
        
        elif "FORM400" in name_upper:
            form_400.process_400(html_content, file_name, mock_db)
        
        else:
            print("⚠️ Formulario no reconocido.")

    except Exception as e:
        print(f"\n❌ Error durante el procesamiento: {e}")
        import traceback
        traceback.print_exc()

    print("-" * 60)
    print("✅ Prueba finalizada.")

if __name__ == "__main__":
    run_test()