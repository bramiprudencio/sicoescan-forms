from bs4 import BeautifulSoup
import re
import unicodedata
from shared.utils import clean_text, parse_float, generate_slug
# Asegúrate de tener insert_proponente o crea la lógica simple abajo
from shared.firestore import insert_entidad, insert_convocatoria, insert_item, insert_proponente 

def process_170(html_content, file_name, db):
    print(f"--- Procesando Formulario 170: {file_name} ---")
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
    except Exception as e:
        print(f"Error parseando HTML en {file_name}: {e}")
        return

    convocatoria_data = {}

    # ==========================================
    # 2. CONVOCATORIA
    # ==========================================
    try:
        # CUCE
        # Nota: En tu snippet usabas find_all(...)[1], ajustamos para ser seguros
        etiquetas_cuce = soup.find_all('strong', class_='FormularioEtiquetaCUCE')
        if len(etiquetas_cuce) > 1:
            convocatoria_data['cuce'] = clean_text(etiquetas_cuce[1].get_text())
        else:
            # Fallback por si la estructura cambia levemente
            cuce_td = soup.find('td', string='CUCE:')
            if cuce_td:
                convocatoria_data['cuce'] = clean_text(cuce_td.find_next_sibling('td').get_text())

        if not convocatoria_data.get('cuce'):
            print(f"Advertencia: No se encontró CUCE en {file_name}")
            return

        # Actualizamos la Convocatoria General para indicar que ya hay adjudicación
        # No sobrescribimos todo, solo lo necesario.
        insert_convocatoria(
            db,
            cuce=convocatoria_data.get('cuce'),
            estado="Adjudicado", # Actualizamos estado
            forms="FORM170"      # Se añadirá al array de forms
        )

    except Exception as e:
        print(f"Error procesando convocatoria en {file_name}: {e}")

    # ==========================================
    # 3. ITEMS ADJUDICADOS
    # ==========================================
    used_slugs = set()
    
    try:
        # Buscamos la tabla específica de adjudicados
        title_adjudicados = soup.find("td", string=re.compile(r"\bDETALLE\b.*\bADJUDICADOS\b", re.IGNORECASE))
        
        if title_adjudicados:
            rows = title_adjudicados.find_parent("table").find_all("tr")

            # --- Lógica de Cabeceras (Tu lógica original preservada) ---
            # Verificamos si existe la fila extra de "Preferencia" / "Margenes"
            margenes_cell = rows[1].find("td", string=re.compile(r"\bPreferencia\b", re.IGNORECASE))
            
            if margenes_cell:
                # Cabecera compuesta (Fila 1 + Fila 2)
                headers = [h.get_text(strip=True) for h in rows[1].find_all("td")] + \
                          [h.get_text(strip=True) for h in rows[2].find_all("td")]
                start_row = 3
            else:
                # Cabecera simple (Fila 1)
                headers = [h.get_text(strip=True) for h in rows[1].find_all("td")]
                start_row = 2

            # Mapeo de columnas
            map_headers = {
                "Código Catalogo": "cod_catalogo",
                "Descripción": "descripcion",
                "Unidad de Medida": "medida",
                "Cantidad adjudicada": "cantidad_adjudicada",
                "Precio referencial unitario": "precio_referencial",
                "Precio unitario referencial": "precio_referencial",
                "Precio referencial total": "precio_referencial_total",
                "Precio unitario adjudicado": "precio_adjudicado",
                "Total adjudicado": "precio_adjudicado_total",
                "Proponente Adjudicado": "proponente_nombre",
                "Buenas Prácticas de Manufactura (BPM)": "bpm",
                "Buenas Prácticas de Almacenamiento (BPA)": "bpa",
                "Bienes Producidos en el pais": "bpp",
                "Porcentaje Componentes Origen Nac. del CBP entre el 30% y 50%": "pcon_30",
                "Porcentaje Componentes Origen Nac. del CBP mayor al 50%": "pcon_50",
                "Tipo de Proponente (MyPE, OECA, APP)": "tipo_proponente",
                "Causal de declaratoria desierta": "causal_desierto"
            }
            headers = [map_headers.get(h, h.lower().replace(" ", "_")) for h in headers]

            # --- Procesamiento de Filas ---
            for row in rows[start_row:]:
                # Limpiar tags <b> residuales
                b_tag = row.find("b")
                if b_tag: b_tag.decompose()
                
                cols = row.find_all("td")
                if not cols or len(cols) < 2: continue

                item = {}
                for i in range(len(cols)):
                    if i < len(headers):
                        key = headers[i]
                        val = cols[i] # Elemento Tag
                        
                        # Mantenemos HTML en descripción para coherencia con F100
                        if key == 'descripcion':
                            item[key] = val.decode_contents().strip()
                        elif key in ['cantidad_adjudicada', 'precio_referencial', 'precio_referencial_total', 
                                     'precio_adjudicado', 'precio_adjudicado_total']:
                            item[key] = parse_float(val.get_text(strip=True))
                        else:
                            item[key] = clean_text(val.get_text(strip=True))

                # --- Inyección de Datos Contextuales ---
                item['estado'] = "Adjudicado"

                # --- Generación de Slug (Misma lógica F100) ---
                raw_desc = item.get('descripcion', 'item')
                slug_base = generate_slug(raw_desc)
                
                slug_final = slug_base
                counter = 1
                while slug_final in used_slugs:
                    slug_final = f"{slug_base}_{counter}"
                    counter += 1
                used_slugs.add(slug_final)

                # --- Guardado de Item ---
                insert_item(db, item, convocatoria_data.get('cuce'), slug_final)

                # --- Guardado de Proponente ---
                if item.get("proponente_nombre"):
                    insert_proponente(db, item.get("proponente_nombre"))

    except Exception as e:
        print(f"Error procesando items adjudicados en {file_name}: {e}")


    # ==========================================
    # 4. ITEMS DESIERTOS
    # ==========================================
    try:
        title_desiertos = soup.find("td", string=re.compile(r"\bDETALLE\b.*\bDESIERTOS\b", re.IGNORECASE))
        
        if title_desiertos:
            rows = title_desiertos.find_parent("table").find_all("tr")
            
            # Cabecera suele ser simple en desiertos
            if len(rows) > 1:
                headers = [h.get_text(strip=True) for h in rows[1].find_all("td")]
                
                # Mismo mapeo, aunque aquí solo aplicarán 'descripcion', 'precio', 'causal'
                headers = [map_headers.get(h, h.lower().replace(" ", "_")) for h in headers]

                for row in rows[2:]:
                    b_tag = row.find("b")
                    if b_tag: b_tag.decompose()
                    
                    cols = row.find_all("td")
                    if not cols or len(cols) < 2: continue

                    item = {}
                    for i in range(len(cols)):
                        if i < len(headers):
                            key = headers[i]
                            val = cols[i]

                            if key == 'descripcion':
                                item[key] = val.decode_contents().strip()
                            elif key in ['precio_referencial', 'precio_referencial_total']:
                                item[key] = parse_float(val.get_text(strip=True))
                            else:
                                item[key] = clean_text(val.get_text(strip=True))

                    # Contexto
                    item['estado'] = "Desierto"

                    # Slug
                    raw_desc = item.get('descripcion', 'item')
                    slug_base = generate_slug(raw_desc)
                    
                    slug_final = slug_base
                    counter = 1
                    while slug_final in used_slugs:
                        slug_final = f"{slug_base}_{counter}"
                        counter += 1
                    used_slugs.add(slug_final)

                    # Guardado
                    insert_item(db, item, convocatoria_data.get('cuce'), slug_final)

    except Exception as e:
        print(f"Error procesando items desiertos en {file_name}: {e}")

    print(f"✅ Formulario 170 procesado: {convocatoria_data.get('cuce')}")