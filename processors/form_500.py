import pprint
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

    # ==========================================
    # 1. EXTRACCIÓN MEJORADA
    # ==========================================
    try:
        cuce_td = soup.find("td", string=lambda text: text and "CUCE" in text)
        if cuce_td:
            convocatoria_cuce = clean_text(cuce_td.find_next_sibling("td").get_text())
        
        if not convocatoria_cuce:
            print(f"❌ No se encontró CUCE en {file_name}")
            return

        title_font = soup.find("font", string=re.compile(r"RECEPCIÓN DE BIENES", re.IGNORECASE))
        if title_font:
            items_table = title_font.find_parent("table")
            rows = items_table.find_all("tr")

            if len(rows) > 1:
                headers_raw = [h.get_text(strip=True) for h in rows[1].find_all("td")]
                
                # Mapa de encabezados
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
                    
                    # ❌ YA NO HACEMOS b.decompose() AQUÍ 
                    # Queremos leer el HTML de la descripción si existe

                    for i in range(len(cols)):
                        if i < len(headers):
                            key = headers[i]
                            cell = cols[i]
                            
                            # Lógica para mantener negritas en descripción
                            if key == 'descripcion':
                                # Usamos decode_contents para obtener "<b>Texto</b>"
                                val = cell.decode_contents().strip()
                                # Limpieza suave de espacios múltiples
                                val = " ".join(val.split())
                                item[key] = val
                            else:
                                # Para el resto, texto plano limpio
                                item[key] = clean_text(cell.get_text(strip=True))
                    
                    # === AGREGA ESTAS LÍNEAS AQUÍ ===
                    print(f"\n🔎 ITEM EXTRAÍDO {len(items_extracted) + 1}:")
                    pprint.pprint(item)
                    # ================================
                    
                    items_extracted.append(item)

    except Exception as e:
        print(f"Error extrayendo datos en {file_name}: {e}")
        return

    # ==========================================
    # 2. PROCESAMIENTO Y MATCHING (NUEVO)
    # ==========================================
    
    update_convocatoria_status(db, convocatoria_cuce, 'Recibido ', 'FORM500')

    # Obtenemos items existentes (que ahora tendrán ID tipo CUCE_1, CUCE_2...)
    existing_docs = list(get_items_by_cuce(db, convocatoria_cuce)) 

    print(f"Items extraídos: {len(items_extracted)} | Items en BD: {len(existing_docs)}")

    for extracted in items_extracted:
        
        # 1. Calculamos el slug del item que acaba de llegar (Form 500)
        # Importante: clean_text para ignorar negritas al generar el slug de comparación
        slug_incoming = slugify(clean_text(extracted.get('descripcion', '')))
        
        match_doc = None

        # 2. Buscamos el match en la lista de BD comparando SLUGS, no IDs
        for doc in existing_docs:
            doc_data = doc.to_dict()
            
            # Buscamos el slug guardado en BD
            slug_db = doc_data.get('slug')
            
            # Fallback: Si es un item viejo sin campo 'slug', lo calculamos al vuelo
            if not slug_db:
                slug_db = slugify(clean_text(doc_data.get('descripcion', '')))

            if slug_db == slug_incoming:
                match_doc = doc
                break 
        
        # 3. Si hay match, actualizamos usando el ID del documento encontrado
        if match_doc:
            cant_solicitada = parse_float(extracted.get('cantidad_solicitada'))
            precio_total = parse_float(extracted.get('precio_adjudicado_total'))
            
            precio_unitario = 0
            if cant_solicitada and precio_total:
                try:
                    precio_unitario = precio_total / cant_solicitada
                except ZeroDivisionError:
                    precio_unitario = 0

            update_data = {
                'proponente_nombre': extracted.get('proponente_nombre'),
                'estado': extracted.get('estado'),
                'cantidad_recepcionada': parse_float(extracted.get('cantidad_recepcionada')),
                'fecha_recepcion_definitiva': local_parse_date(extracted.get('fecha_recepcion_definitiva')),
                'precio_adjudicado_unitario': precio_unitario,
                'precio_adjudicado_total': precio_total,
                'nr_contrato': extracted.get('nr_contrato')
            }

            # Usamos match_doc.id (que será el ID correcto de la BD)
            update_item_adjudicacion(db, match_doc.id, update_data)
            
            if extracted.get('proponente_nombre'):
                insert_proponente(db, extracted.get('proponente_nombre'))
        else:
            # Debug para saber por qué falla si ocurre
            # print(f"⚠️ No match for: {slug_incoming}")
            pass

    print(f"✅ Formulario 500 procesado: {convocatoria_cuce}")