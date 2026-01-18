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

def local_parse_date(date_str):
    if not date_str: return None
    try:
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
    
    # Conjunto para rastrear qué IDs de Firestore se actualizaron
    matched_ids = set() 
    found_deserted_section = False # Bandera para saber si hubo tabla explícita

    # ==========================================
    # 0. EXTRACCIÓN DE CUCE
    # ==========================================
    try:
        cuce_td = soup.find("td", string=lambda text: text and "CUCE" in text)
        if cuce_td:
            convocatoria_cuce = clean_text(cuce_td.find_next_sibling("td").get_text())
        
        if not convocatoria_cuce:
            print(f"❌ No se encontró CUCE en {file_name}")
            return
    except Exception as e:
        print(f"Error extrayendo CUCE: {e}")
        return

    # Actualizamos estado de la convocatoria
    update_convocatoria_status(db, convocatoria_cuce, 'Recibido', 'FORM500')

    # Obtenemos items existentes en Firestore
    existing_docs = list(get_items_by_cuce(db, convocatoria_cuce))
    print(f"Items en BD para {convocatoria_cuce}: {len(existing_docs)}")

    # ==========================================
    # 1. PROCESAR TABLA DE "RECEPCIÓN DE BIENES"
    # ==========================================
    try:
        title_font = soup.find("font", string=re.compile(r"RECEPCIÓN DE BIENES", re.IGNORECASE))
        if title_font:
            items_table = title_font.find_parent("table")
            # Limpiamos tablas anidadas
            [t.decompose() for t in items_table.find_all('table')]
            
            rows = items_table.find_all("tr")

            if len(rows) > 1:
                # Buscamos encabezados
                headers_raw = [h.get_text(strip=True) for h in rows[1].find_all("td")]
                
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
                headers = [map_headers.get(h, h.lower().replace(" ", "_")) for h in headers_raw]

                for row in rows[2:]:
                    cols = row.find_all("td")
                    if not cols or len(cols) < 2: continue

                    item = {}
                    for i in range(len(cols)):
                        if i < len(headers):
                            key = headers[i]
                            # Preservamos HTML para descripción, texto plano para el resto
                            if key == 'descripcion':
                                item[key] = cols[i].decode_contents().strip()
                            else:
                                item[key] = clean_text(cols[i].get_text(strip=True))

                    print(f"   ➡️ Item extraído: {item.get('descripcion', '')[:30]}...")
                    
                    # --- LÓGICA DE MATCHING Y UPDATE (RECEPCIÓN) ---
                    incoming_desc = clean_text(item.get('descripcion', ''))
                    
                    match_doc = None
                    for doc in existing_docs:
                        if clean_text(doc.to_dict().get('descripcion', '')) == incoming_desc:
                            match_doc = doc
                            break
                    
                    if match_doc:
                        # Cálculo de unitario
                        cant = parse_float(item.get('cantidad_solicitada'))
                        total = parse_float(item.get('precio_adjudicado_total'))
                        unitario = (total / cant) if (cant and total) else 0

                        update_data = {
                            'proponente_nombre': item.get('proponente_nombre'),
                            'estado': item.get('estado', 'Recibido'), 
                            'cantidad_recepcionada': parse_float(item.get('cantidad_recepcionada')),
                            'fecha_recepcion_definitiva': local_parse_date(item.get('fecha_recepcion_definitiva')),
                            'precio_adjudicado_unitario': unitario,
                            'precio_adjudicado_total': total,
                            'nr_contrato': item.get('nr_contrato')
                        }
                        
                        update_item_adjudicacion(db, match_doc.id, update_data)
                        matched_ids.add(match_doc.id) # <--- REGISTRAMOS EL MATCH
                        
                        if item.get('proponente_nombre'):
                            insert_proponente(db, item.get('proponente_nombre'))

    except Exception as e:
        print(f"Error procesando tabla de recepción: {e}")

    # ==========================================
    # 2. PROCESAR TABLA DE "ITEMS DESIERTOS / CANCELADOS"
    # ==========================================
    try:
        # Buscamos títulos comunes para esta sección
        deserted_title = soup.find("font", string=re.compile(r"(ITEMS?|LOTES?).*(DESIERTOS?|CANCELADOS?|ANULADOS?)", re.IGNORECASE))
        
        if deserted_title:
            found_deserted_section = True
            print("   ⚠️ Se encontró sección de Items Desiertos/Cancelados.")
            
            d_table = deserted_title.find_parent("table")
            [t.decompose() for t in d_table.find_all('table')] # Limpiar anidadas
            
            d_rows = d_table.find_all("tr")
            
            # Intentamos detectar la columna descripción dinámicamente
            idx_desc = -1
            if len(d_rows) > 1:
                headers = d_rows[1].find_all("td")
                for idx, h in enumerate(headers):
                    if "DESCRIPCI" in clean_text(h.get_text()).upper():
                        idx_desc = idx
                        break
            
            # Si no encontramos header, asumimos índice 1 o 2 según estructura común
            if idx_desc == -1: idx_desc = 2 

            for row in d_rows[2:]:
                cols = row.find_all("td")
                if len(cols) > idx_desc:
                    # Obtenemos descripción del item desierto
                    # Usamos decode_contents para mantener consistencia si hay HTML
                    desc_deserted = clean_text(cols[idx_desc].decode_contents().strip())
                    
                    # Buscamos match en la BD
                    for doc in existing_docs:
                        # Solo procesamos si NO fue matcheado ya en la tabla de recepción
                        if doc.id not in matched_ids:
                            if clean_text(doc.to_dict().get('descripcion', '')) == desc_deserted:
                                
                                # ACTUALIZAMOS A DESIERTO
                                update_item_adjudicacion(db, doc.id, {
                                    'estado': 'Desierto',
                                    'monto_adjudicado': 0,
                                    'adjudicado_a': None
                                })
                                matched_ids.add(doc.id)
                                print(f"   🚫 Item marcado como Desierto (Explícito): {doc.id}")
                                break

    except Exception as e:
        print(f"Error procesando tabla de desiertos: {e}")

    # ==========================================
    # 3. LOGICA FINAL: IMPLICIT DESERTED
    # ==========================================
    # "Si no hubiese la lista [de desiertos] hay que declarar desiertos a todos los que no se modificaron"
    
    if not found_deserted_section:
        print("   ℹ️ No se encontró lista explícita de desiertos. Aplicando lógica de desierto implícito.")
        count_implicit = 0
        
        for doc in existing_docs:
            if doc.id not in matched_ids:
                # Si el item estaba en la BD pero NO apareció en la tabla de recepción
                # y NO hubo tabla de desiertos explícita -> Asumimos Desierto
                update_item_adjudicacion(db, doc.id, {
                    'estado': 'Desierto', # O 'No Recepcionado'
                    'observacion': 'Marcado automáticamente por ausencia en Form 500'
                })
                count_implicit += 1
                matched_ids.add(doc.id) # Lo marcamos para no repetir
        
        if count_implicit > 0:
            print(f"   📉 {count_implicit} items marcados como Desiertos (Implícitos).")

    print(f"✅ Formulario 500 procesado: {convocatoria_cuce}")