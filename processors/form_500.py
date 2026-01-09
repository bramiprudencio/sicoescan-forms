from bs4 import BeautifulSoup
import re
from datetime import datetime
from shared.utils import clean_text, parse_float, slugify
from shared.firestore import (
    update_convocatoria_status, 
    get_items_by_cuce, 
    update_item_adjudicacion, 
    insert_proponente
)

# Helper simple para fechas dd/mm/yyyy
def local_parse_date(date_str):
    if not date_str: return None
    try:
        # Limpiamos espacios y tomamos lo que parezca una fecha
        clean = date_str.strip().split(' ')[0] 
        return datetime.strptime(clean, "%d/%m/%Y")
    except:
        return None

def process_500(html_content, file_name, db):
    print(f"--- Procesando Formulario 500: {file_name} ---")
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
    except Exception as e:
        print(f"Error parseando HTML en {file_name}: {e}")
        return

    convocatoria_cuce = None
    items_extracted = []

    # ==========================================
    # 1. EXTRACCIÓN (Tu lógica original)
    # ==========================================
    try:
        # Extraer CUCE
        cuce_td = soup.find("td", string=lambda text: text and "CUCE" in text)
        if cuce_td:
            convocatoria_cuce = clean_text(cuce_td.find_next_sibling("td").get_text())
        
        if not convocatoria_cuce:
            print(f"❌ No se encontró CUCE en {file_name}")
            return

        # Extraer Tabla de Items
        title_font = soup.find("font", string=re.compile(r"RECEPCIÓN DE BIENES", re.IGNORECASE))
        if title_font:
            items_table = title_font.find_parent("table")
            rows = items_table.find_all("tr")

            # Mapeo de columnas
            if len(rows) > 1:
                headers = [h.get_text(strip=True) for h in rows[1].find_all("td")]
                
                map_headers = {
                    "Nro. de contrato": "nr_contrato",
                    "Fecha de firma de contrato": "fecha_contrato",
                    "Nombre o razón social de la empresa contratada": "proponente_nombre",
                    "Descripción del bien, obra o servicio objeto del contrato": "descripcion",
                    "Estado de la recepción": "estado",
                    "Cantidad solicitada": "cantidad_solicitada",
                    "Cantidad Recepcionada/No Recepcionada": "cantidad_recepcionada",
                    "Fecha  de recepción según contrato (día/mes/año)": "fecha_recepcion",
                    "Fecha de  recepción provisional/ sujeta a verificación (día/mes/año)": "fecha_recepcion_provisional",
                    "Fecha de recepción definitiva /  de emisión del informe de conformidad  (día/mes/año)": "fecha_recepcion_definitiva",
                    "Monto real  ejecutado": "precio_adjudicado_total"
                }
                
                # Normalizamos headers para match
                headers = [map_headers.get(h, h.lower().replace(" ", "_")) for h in headers]

                for row in rows[2:]:
                    cols = row.find_all("td")
                    if not cols or len(cols) < 2: continue

                    item = {}
                    # Limpiar <b> tags
                    b = row.find("b")
                    if b: b.decompose()

                    for i in range(len(cols)):
                        if i < len(headers):
                            item[headers[i]] = clean_text(cols[i].get_text(strip=True))
                    
                    items_extracted.append(item)

    except Exception as e:
        print(f"Error extrayendo datos en {file_name}: {e}")
        return

    # ==========================================
    # 2. PROCESAMIENTO Y ACTUALIZACIÓN
    # ==========================================
    
    # A) Actualizar Convocatoria
    update_convocatoria_status(db, convocatoria_cuce, 'Contratado', 'FORM500')

    # B) Obtener Items existentes de Firestore
    # Esto devuelve un generador (stream)
    existing_docs = list(get_items_by_cuce(db, convocatoria_cuce)) 

    print(f"Items extraídos: {len(items_extracted)} | Items en BD: {len(existing_docs)}")

    # C) Lógica de Matching (Tu algoritmo)
    for doc in existing_docs:
        doc_id = doc.id
        
        # Buscamos si alguno de los items extraídos coincide con este documento
        match_found = False
        
        for extracted in items_extracted:
            # Tu condición clave: si el slug de la descripción está en el ID del documento
            if slugify(extracted.get('descripcion', '')) in doc_id:
                
                # --- Preparar datos para actualizar ---
                
                # Parseo de números
                cant_solicitada = parse_float(extracted.get('cantidad_solicitada'))
                precio_total = parse_float(extracted.get('precio_adjudicado_total'))
                
                # Cálculo de precio unitario adjudicado
                precio_unitario = 0
                if cant_solicitada and precio_total:
                    try:
                        precio_unitario = precio_total / cant_solicitada
                    except ZeroDivisionError:
                        precio_unitario = 0

                update_data = {
                    'proponente_nombre': extracted.get('proponente_nombre'),
                    'estado': extracted.get('estado'), # "Recepción Definitiva", "Desierto", etc.
                    'cantidad_recepcionada': parse_float(extracted.get('cantidad_recepcionada')),
                    'fecha_recepcion_definitiva': local_parse_date(extracted.get('fecha_recepcion_definitiva')),
                    'precio_adjudicado_unitario': precio_unitario,
                    'precio_adjudicado_total': precio_total,
                    'nr_contrato': extracted.get('nr_contrato')
                }

                # 1. Actualizamos el item
                update_item_adjudicacion(db, doc_id, update_data)
                
                # 2. Guardamos el proponente por separado
                if extracted.get('proponente_nombre'):
                    insert_proponente(db, extracted.get('proponente_nombre'))
                
                match_found = True
                # break # Rompemos el loop interno (extracted) porque ya encontramos el match para este doc
                # NOTA: Quité el break por si acaso hay duplicados raros, pero con tu lógica original estaba bien ponerlo.
                # Si un doc solo puede tener un match, descomenta el break.
                break 
        
        if not match_found:
            # Opcional: Loggear que un item de la base de datos no tuvo actualización en el Form 500
            # print(f"⚠️ Item {doc_id} no encontrado en el Form 500")
            pass

    print(f"✅ Formulario 500 procesado: {convocatoria_cuce}")