from bs4 import BeautifulSoup
import re
from shared.utils import (
  clean_text,
  parse_float,
  parse_date,
  generate_slug,
  normalize_for_match
)
from shared.firestore import (
    update_convocatoria_status,
    get_items_by_cuce,
    update_item_adjudicacion,
    insert_proponente,
    insert_item
)

def process_500(html_content, file_name, db):
    print(f"--- Procesando Formulario 500: {file_name} ---")
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
    except Exception as e:
        print(f"Error parseando HTML en {file_name}: {e}")
        return

    convocatoria_cuce = None
    
    # Conjunto para rastrear qué IDs de Firestore se tocaron
    matched_ids = set() 
    # Slugs usados en esta sesión (para evitar duplicados al crear nuevos)
    used_slugs_in_session = set()
    
    found_deserted_section = False 

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

    # Obtenemos items existentes en Firestore para hacer match
    existing_docs = list(get_items_by_cuce(db, convocatoria_cuce))
    print(f"Items en BD para {convocatoria_cuce}: {len(existing_docs)}")

    # Pre-procesamos descripciones de la BD para búsqueda rápida
    # Mapa: { "descripcion_normalizada": doc_object }
    existing_map = {normalize_for_match(doc.to_dict().get('descripcion', '')): doc for doc in existing_docs}

    # ==========================================
    # 1. PROCESAR TABLA DE "RECEPCIÓN DE BIENES"
    # ==========================================
    try:
        title_font = soup.find("font", string=re.compile(r"RECEPCIÓN DE BIENES", re.IGNORECASE))
        if title_font:
            items_table = title_font.find_parent("table")
            [t.decompose() for t in items_table.find_all('table')]
            
            rows = items_table.find_all("tr")

            if len(rows) > 1:
                # Headers
                headers_raw = [h.get_text(strip=True) for h in rows[1].find_all("td")]
                
                map_headers = {
                    "Nro. de contrato": "nr_contrato",
                    "Fecha de firma de contrato": "fecha_contrato",
                    "Nombre o razón social de la empresa contratada": "proponente_nombre",
                    "Descripción del bien, obra o servicio objeto del contrato": "descripcion",
                    "Estado de la recepción": "estado",
                    "Cantidad solicitada": "cantidad_solicitada",
                    "Cantidad Solicitada": "cantidad_solicitada",
                    "Cantidad Recepcionada/No Recepcionada": "cantidad_recepcionada",
                    "Fecha  de recepción según contrato (día/mes/año)": "fecha_recepcion",
                    "Fecha de  recepción provisional/ sujeta a verificación (día/mes/año)": "fecha_recepcion_provisional",
                    "Fecha de recepción definitiva /  de emisión del informe de conformidad  (día/mes/año)": "fecha_recepcion_definitiva",
                    "Monto real ejecutado": "precio_adjudicado_total"
                }
                headers = [map_headers.get(h, h.lower().replace(" ", "_")) for h in headers_raw]

                for row in rows[2:]:
                    cols = row.find_all("td")
                    if not cols or len(cols) < 2: continue

                    item_data = {}
                    for i in range(len(cols)):
                        if i < len(headers):
                            key = headers[i]
                            # Preservamos HTML
                            if key == 'descripcion':
                                item_data[key] = cols[i].decode_contents().strip()
                            else:
                                item_data[key] = clean_text(cols[i].get_text(strip=True))
                    
                    # --- LÓGICA DE MATCHING ---
                    desc_clean = normalize_for_match(item_data.get('descripcion', ''))
                    match_doc = existing_map.get(desc_clean)
                    
                    # Datos comunes a actualizar/insertar
                    cant = parse_float(item_data.get('cantidad_solicitada'))
                    total = parse_float(item_data.get('precio_adjudicado_total'))
                    unitario = (total / cant) if (cant and total and cant > 0) else 0

                    update_payload = {
                        'proponente_nombre': item_data.get('proponente_nombre'),
                        'estado': item_data.get('estado', 'Recibido'), 
                        'cantidad_recepcionada': parse_float(item_data.get('cantidad_recepcionada')),
                        'fecha_recepcion_definitiva': parse_date(item_data.get('fecha_recepcion_definitiva')),
                        'precio_adjudicado_unitario': unitario,
                        'precio_adjudicado_total': total,
                        'nr_contrato': item_data.get('nr_contrato')
                    }

                    if match_doc:
                        # ACTUALIZAR EXISTENTE
                        update_item_adjudicacion(db, match_doc.id, update_payload)
                        matched_ids.add(match_doc.id)
                    else:
                        # CREAR NUEVO (Si no existía en Form 100/110/400)
                        # Generamos Slug
                        raw_desc = item_data.get('descripcion', 'item')
                        slug_base = generate_slug(raw_desc)
                        
                        slug_final = slug_base
                        counter = 1
                        # Verificar contra lo usado en esta sesión
                        while slug_final in used_slugs_in_session:
                            slug_final = f"{slug_base}_{counter}"
                            counter += 1
                        used_slugs_in_session.add(slug_final)

                        # Completamos payload para inserción completa
                        update_payload['descripcion'] = item_data.get('descripcion')
                        update_payload['cantidad_solicitada'] = cant
                        update_payload['tipo_form'] = "FORM500_CREATED" # Marca de origen
                        
                        insert_item(db, update_payload, convocatoria_cuce, slug_final)
                        print(f"   ✨ Item creado en F500 (No existía): {slug_final}")

                    # Crear Proponente si aplica
                    if item_data.get('proponente_nombre'):
                        insert_proponente(db, item_data.get('proponente_nombre'))

    except Exception as e:
        print(f"Error procesando tabla de recepción: {e}")

    # ==========================================
    # 2. PROCESAR TABLA DE "ITEMS DESIERTOS / CANCELADOS"
    # ==========================================
    try:
        deserted_title = soup.find("font", string=re.compile(r"(ITEMS?|LOTES?).*(DESIERTOS?|CANCELADOS?|ANULADOS?)", re.IGNORECASE))
        
        if deserted_title:
            found_deserted_section = True
            
            d_table = deserted_title.find_parent("table")
            [t.decompose() for t in d_table.find_all('table')]
            
            d_rows = d_table.find_all("tr")
            
            # Detectar columna descripción
            idx_desc = -1
            if len(d_rows) > 1:
                headers = d_rows[1].find_all("td")
                for idx, h in enumerate(headers):
                    if "DESCRIPCI" in clean_text(h.get_text()).upper():
                        idx_desc = idx
                        break
            if idx_desc == -1: idx_desc = 2 

            for row in d_rows[2:]:
                cols = row.find_all("td")
                if len(cols) > idx_desc:
                    
                    # MATCHING DESIERTO
                    # Usamos decode_contents para normalizar igual que arriba
                    desc_html = cols[idx_desc].decode_contents().strip()
                    desc_clean = normalize_for_match(desc_html)
                    
                    match_doc = existing_map.get(desc_clean)

                    if match_doc:
                        if match_doc.id not in matched_ids:
                            update_item_adjudicacion(db, match_doc.id, {
                                'estado': 'Desierto',
                                'monto_adjudicado': 0,
                                'adjudicado_a': None
                            })
                            matched_ids.add(match_doc.id)
                    else:
                        # Si es desierto y no existe, generalmente no vale la pena crearlo,
                        # pero si quisieras, aquí iría la lógica de insert_item con estado Desierto.
                        pass

    except Exception as e:
        print(f"Error procesando tabla de desiertos: {e}")

    # ==========================================
    # 3. LÓGICA FINAL: IMPLICIT DESERTED
    # ==========================================
    # Si NO hay tabla de desiertos, todo lo que no se tocó es desierto
    
    if not found_deserted_section:
        count_implicit = 0
        for doc in existing_docs:
            if doc.id not in matched_ids:
                # IMPORTANTE: Verificamos que no esté ya 'Adjudicado' o 'Recibido' por otro proceso
                current_status = doc.to_dict().get('estado', '')
                if current_status not in ['Recibido', 'Entregado']:
                    update_item_adjudicacion(db, doc.id, {
                        'estado': 'Desierto', 
                        'observacion': 'Marcado automáticamente por ausencia en Form 500'
                    })
                    count_implicit += 1
                    matched_ids.add(doc.id)
        
        if count_implicit > 0:
            print(f"   📉 {count_implicit} items marcados como Desiertos (Implícitos).")

    print(f"✅ Formulario 500 procesado: {convocatoria_cuce}")