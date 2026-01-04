from bs4 import BeautifulSoup
import re
from shared.utils import clean_text, parse_float, slugify
from shared.database import (
    check_and_update_convocatoria_170,
    get_items_by_cuce,
    update_item_adjudicacion,
    update_item_desierto,
    insert_proponente
)

def process_170(html_content, file_name, db):
    print(f"--- Procesando Formulario 170: {file_name} ---")
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
    except Exception as e:
        print(f"Error parseando HTML en {file_name}: {e}")
        return

    convocatoria_cuce = None
    items_adjudicados_map = {} # Clave: slug(descripcion), Valor: data
    items_desiertos_map = {}   # Clave: slug(descripcion), Valor: data

    # ==========================================
    # 1. EXTRACCIÓN
    # ==========================================
    try:
        # CUCE (Tu lógica: segundo strong con esa clase)
        cuce_tags = soup.find_all('strong', class_='FormularioEtiquetaCUCE')
        if len(cuce_tags) > 1:
            convocatoria_cuce = clean_text(cuce_tags[1].get_text())
        
        if not convocatoria_cuce:
            print(f"❌ No se encontró CUCE en {file_name}")
            return

        # Limpieza previa (Tu lógica de eliminar 'Margenes' en proponentes)
        try:
            nro_doc_td = soup.find('td', string=re.compile(r"\bNro. Documento\b", re.IGNORECASE))
            if nro_doc_td:
                prop_rows = nro_doc_td.find_parent("table").find_all("tr")
                if len(prop_rows) > 1:
                    margenes = prop_rows[1].find("td", string=re.compile(r"\bMargenes\b", re.IGNORECASE))
                    if margenes: margenes.decompose()
        except: pass

        # ------------------------------------------------
        # A. ITEMS ADJUDICADOS
        # ------------------------------------------------
        adj_title = soup.find("td", string=re.compile(r"\bDETALLE\b.*\bADJUDICADOS\b", re.IGNORECASE))
        if adj_title:
            rows = adj_title.find_parent("table").find_all("tr")
            
            # Detectar cabecera compleja (con "Preferencia")
            start_idx = 2
            pref_td = rows[1].find("td", string=re.compile(r"\bPreferencia\b", re.IGNORECASE))
            if pref_td:
                pref_td.decompose()
                # Fusionar headers de dos filas
                h1 = [h.get_text(strip=True) for h in rows[1].find_all("td")]
                h2 = [h.get_text(strip=True) for h in rows[2].find_all("td")]
                headers_raw = h1 + h2
                start_idx = 3
            else:
                headers_raw = [h.get_text(strip=True) for h in rows[1].find_all("td")]

            # Mapeo
            map_headers = {
                "Código Catalogo": "cod_catalogo",
                "Descripción": "descripcion",
                "Unidad de Medida": "medida",
                "Cantidad adjudicada": "cantidad_adjudicada",
                "Precio referencial unitario": "precio_referencial",
                "Precio referencial total": "precio_referencial_total",
                "Precio unitario adjudicado": "precio_adjudicado",
                "Total adjudicado": "precio_adjudicado_total",
                "Proponente Adjudicado": "proponente_nombre",
                "Tipo de Proponente (MyPE, OECA, APP)": "tipo_proponente",
                "Causal de declaratoria desierta": "causal_desierto"
            }
            headers = [map_headers.get(h, h.lower().replace(" ", "_")) for h in headers]

            for row in rows[start_idx:]:
                b = row.find("b")
                if b: b.decompose()
                cols = row.find_all("td")
                if not cols or len(cols) < 2: continue

                item = {}
                for i in range(len(cols)):
                    if i < len(headers):
                        item[headers[i]] = clean_text(cols[i].get_text(strip=True))
                
                # Guardamos en mapa usando slug de descripcion como clave
                if 'descripcion' in item:
                    key_slug = slugify(item['descripcion'])
                    items_adjudicados_map[key_slug] = item

        # ------------------------------------------------
        # B. ITEMS DESIERTOS
        # ------------------------------------------------
        des_title = soup.find("td", string=re.compile(r"\bDETALLE\b.*\bDESIERTOS\b", re.IGNORECASE))
        if des_title:
            rows = des_title.find_parent("table").find_all("tr")
            # Asumimos estructura simple para desiertos
            headers_raw = [h.get_text(strip=True) for h in rows[1].find_all("td")]
            headers = [map_headers.get(h, h.lower().replace(" ", "_")) for h in headers_raw]

            for row in rows[2:]:
                b = row.find("b")
                if b: b.decompose()
                cols = row.find_all("td")
                if not cols or len(cols) < 2: continue

                item = {}
                for i in range(len(cols)):
                    if i < len(headers):
                        item[headers[i]] = clean_text(cols[i].get_text(strip=True))
                
                if 'descripcion' in item:
                    key_slug = slugify(item['descripcion'])
                    items_desiertos_map[key_slug] = item

    except Exception as e:
        print(f"Error extrayendo datos en {file_name}: {e}")
        return

    # ==========================================
    # 2. ACTUALIZACIÓN EN BASE DE DATOS
    # ==========================================
    
    # 1. Actualizar Convocatoria (Lógica de estado Recibido/Publicado -> Adjudicado)
    check_and_update_convocatoria_170(db, convocatoria_cuce)

    # 2. Obtener items existentes
    existing_docs = list(get_items_by_cuce(db, convocatoria_cuce))
    
    # 3. Emparejar y Actualizar
    for doc in existing_docs:
        doc_id = doc.id
        
        # Intentamos encontrar coincidencia en los mapas usando el ID del documento
        # (El ID del documento es "CUCE_slug-descripcion", así que verificamos si contiene el slug)
        
        match_adjudicado = None
        match_desierto = None

        # Buscamos en adjudicados
        for slug_key, data in items_adjudicados_map.items():
            if slug_key in doc_id:
                match_adjudicado = data
                break
        
        # Buscamos en desiertos (si no fue adjudicado)
        if not match_adjudicado:
            for slug_key, data in items_desiertos_map.items():
                if slug_key in doc_id:
                    match_desierto = data
                    break
        
        # --- APLICAR ACTUALIZACIONES ---
        if match_adjudicado:
            update_data = {
                'proponente_nombre': match_adjudicado.get('proponente_nombre'),
                'precio_adjudicado': parse_float(match_adjudicado.get('precio_adjudicado')),
                'precio_adjudicado_total': parse_float(match_adjudicado.get('precio_adjudicado_total')),
                'cantidad_adjudicada': parse_float(match_adjudicado.get('cantidad_adjudicada')), # Usamos float por seguridad
                'estado': 'Adjudicado'
            }
            update_item_adjudicacion(db, doc_id, update_data)
            
            # Guardar proponente
            if match_adjudicado.get('proponente_nombre'):
                insert_proponente(db, match_adjudicado.get('proponente_nombre'))

        elif match_desierto:
            update_item_desierto(db, doc_id, match_desierto.get('causal_desierto'))
        
        else:
            # Caso raro: El item existe en la convocatoria original pero no aparece ni en adjudicados ni en desiertos del 170.
            # Podríamos ignorarlo o marcarlo como pendiente.
            pass

    print(f"✅ Formulario 170 procesado: {convocatoria_cuce}")