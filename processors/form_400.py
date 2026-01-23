from bs4 import BeautifulSoup
import re
import unicodedata
from shared.utils import clean_text, parse_float, generate_slug
from shared.firestore import insert_entidad, insert_convocatoria, insert_item

# --- Función Helper para crear IDs limpios ---
def generate_slug(text):
    if not text: return "item"
    # 1. Quitar HTML tags (si quedaron)
    text = BeautifulSoup(text, "html.parser").get_text(separator=" ")
    # 2. Normalizar unicode (quitar acentos)
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
    # 3. Quitar caracteres especiales
    text = re.sub(r'[^\w\s-]', '', text).lower()
    # 4. Reemplazar espacios
    text = re.sub(r'[-\s]+', '_', text).strip('-_')
    return text[:60]

def process_400(html_content, file_name, db):
    print(f"--- Procesando Formulario 400: {file_name} ---")
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
    except Exception as e:
        print(f"Error parseando HTML en {file_name}: {e}")
        return

    # Estructuras temporales
    entidad_data = {}
    convocatoria_data = {}
    items_data = []

    # ==========================================
    # 1. ENTIDAD (Con lógica de recuperación de Departamento)
    # ==========================================
    try:
        section_entidad = soup.find("font", string=re.compile('ENTIDAD', re.IGNORECASE))
        
        if section_entidad:
            entidad_fila = section_entidad.find_parent("table").find_all("tr")[-1].find_all("td")
            
            # Construcción del ID (específico del Form 400)
            cod_raw = clean_text(entidad_fila[0].get_text(strip=True)) + ' - ' + clean_text(entidad_fila[2].get_text(strip=True))
            
            entidad_data = {
                "cod": cod_raw,
                "nombre": clean_text(entidad_fila[3].get_text(strip=True)),
                "fax": clean_text(entidad_fila[4].get_text(strip=True)),
                "telefono": None, 
                "departamento": None
            }

            # --- CONSULTAR DEPARTAMENTO ---
            if entidad_data.get("cod"):
                entidad_ref = db.collection("entidades").document(entidad_data["cod"])
                entidad_snap = entidad_ref.get()
                
                if entidad_snap.exists:
                    existing_data = entidad_snap.to_dict()
                    entidad_data["departamento"] = existing_data.get("departamento")

                insert_entidad(
                    db, 
                    entidad_data["cod"], 
                    entidad_data["nombre"], 
                    entidad_data["fax"], 
                    entidad_data["telefono"]
                )
    except Exception as e:
        print(f"Error extrayendo entidad en {file_name}: {e}")

    # ==========================================
    # 2. CONVOCATORIA
    # ==========================================
    try:
        # CUCE
        convocatoria_cuce = soup.find('td', string='Código Proceso')
        if convocatoria_cuce:
            convocatoria_data['cuce'] = clean_text(convocatoria_cuce.find_next_sibling('td').get_text())
        else:
            print(f"Advertencia: No se encontró CUCE en {file_name}")
            return

        # Recuperar filas base para Modalidad y Objeto
        try:
            convocatoria_rows = convocatoria_cuce.find_parent('table').find_all('tr')
            convocatoria_data['modalidad'] = clean_text(convocatoria_rows[5].find_all('td')[0].get_text(strip=True))
            convocatoria_data['objeto'] = clean_text(convocatoria_rows[4].find_all('td')[0].get_text(strip=True))
        except Exception as e:
            print(f"Error extrayendo filas fijas (modalidad/objeto): {e}")

        # Normativa
        try:
            normativa_td = soup.find('td', string=re.compile('Normativa', re.IGNORECASE))
            if normativa_td:
                convocatoria_data['normativa'] = clean_text(
                    normativa_td.find_parent().find_next_sibling('tr').find_all('td')[3].get_text(strip=True)
                )
        except: pass

        # Fechas y Total (Búsqueda por 'Fecha de firma' estructura típica del 400)
        try:
            firma_td = soup.find('td', string=re.compile('Fecha de firma', re.IGNORECASE))
            if firma_td:
                convocatoria_row = firma_td.find_parent('table').find_all('tr')[1]
                if len(convocatoria_row.find_all('td')) >= 6:
                    cols = convocatoria_row.find_all('td')
                    convocatoria_data['fecha_formalizacion'] = clean_text(cols[3].get_text(strip=True))
                    convocatoria_data['total'] = parse_float(cols[4].get_text(strip=True))
                    convocatoria_data['fecha_entrega'] = clean_text(cols[6].get_text(strip=True))
        except Exception as e:
            print(f"Error extrayendo fechas/total: {e}")

        # Otros campos
        try:
            fecha_pub_td = soup.find('td', string=re.compile('Fecha de envío del formulario', re.IGNORECASE))
            if fecha_pub_td:
                raw_fecha = fecha_pub_td.find_next_sibling('td').get_text(strip=True)
                convocatoria_data['fecha_publicacion'] = raw_fecha.split(' ')[0]
            
            moneda_b = soup.find('b', string=re.compile('Moneda del contrato', re.IGNORECASE))
            if moneda_b:
                convocatoria_data['moneda'] = clean_text(moneda_b.find_parent('td').find_next_sibling('td').get_text(strip=True))
            
            tipo_contr_td = soup.find('td', string=re.compile('Tipo de contratación', re.IGNORECASE))
            if tipo_contr_td:
                convocatoria_data['tipo_contratacion'] = clean_text(
                    tipo_contr_td.find_parent('tr').find_next_sibling('tr').find_all('td')[-1].get_text(strip=True)
                )
                
            convocatoria_data['recurrente_sgte_gestion'] = False
            
        except Exception as e:
             print(f"Error extrayendo campos varios: {e}")

        # ==========================================
        # 3. ITEMS (Con decode_contents para HTML)
        # ==========================================
        items_section = soup.find("td", string=re.compile(r'Código del? (Catálogo|Catalogo)', re.IGNORECASE))
        
        if items_section:
            items_table = items_section.find_parent("tr").find_parent("table")
            
            # Limpieza específica del Form 400 (tablas anidadas)
            items_cols_size = len(items_section.find_parent("tr").find_all('td'))
            [table.decompose() for table in items_table.find_all('table')]
            [row.decompose() for row in items_table.find_all("tr") if len(row.find_all('td')) != items_cols_size]
            
            rows = items_table.find_all("tr")
            
            if len(rows) > 0:
                headers = [h.get_text(strip=True) for h in rows[0].find_all("td")]
                
                # Mapeo directo a nombres estándar de la BD
                map_headers = {
                    "Código del Catálogo": "cod_catalogo",
                    "Código del Catálogo (UNSPSC)": "cod_catalogo",
                    "Descripción del bien o servicio": "descripcion",
                    "Descripción del bien, obra, servicio general o de consultoría": "descripcion",
                    "Unidad de Medida": "unidad",          
                    "Unidad de medida": "unidad",          
                    "Cantidad": "cantidad",                
                    "Cantidad / Cantidad estimada si es variable": "cantidad", 
                    "Precio unitario": "precio_unitario",       
                    "Precio referencial unitario": "precio_unitario", 
                    "Precio referencial total": "precio_total",      
                    "Monto total (p.unit. x cantidad) / Total estimado cuando la cantidad es variable": "precio_total",
                    "Origen del item": "origen"
                }
                headers = [map_headers.get(h, h.lower().replace(" ", "_")) for h in headers]

                for row in rows[1:]:
                    cols = row.find_all("td")
                    if not cols or len(cols) < 2: continue
                    
                    item = {}
                    
                    for i in range(len(cols)):
                        if i < len(headers):
                            key = headers[i]
                            
                            # LOGICA CLAVE: Mantener HTML en descripción
                            if key == 'descripcion':
                                item[key] = cols[i].decode_contents().strip()
                            elif key in ['cantidad', 'precio_unitario', 'precio_total']:
                                item[key] = parse_float(cols[i].get_text(strip=True))
                            else:
                                item[key] = clean_text(cols[i].get_text(strip=True))
                    
                    items_data.append(item)

        # ==========================================
        # 4. GUARDADO
        # ==========================================
        insert_convocatoria(
            db,
            cuce=convocatoria_data.get('cuce'),
            cod_entidad=entidad_data.get('cod'),
            entidad_nombre=entidad_data.get('nombre'),
            entidad_departamento=entidad_data.get('departamento'), 
            fecha_publicacion=convocatoria_data.get('fecha_publicacion'),
            objeto=convocatoria_data.get('objeto'),
            modalidad=convocatoria_data.get('modalidad'),
            normativa=convocatoria_data.get('normativa'),
            tipo_contratacion=convocatoria_data.get('tipo_contratacion'),
            moneda=convocatoria_data.get('moneda'),
            recurrente_sgte_gestion=convocatoria_data.get('recurrente_sgte_gestion'),
            total=convocatoria_data.get('total'),
            fecha_formalizacion=convocatoria_data.get('fecha_formalizacion'),
            fecha_entrega=convocatoria_data.get('fecha_entrega'),
            estado="Publicado",
            forms="FORM400"
        )

        # Set para controlar duplicados de slugs
        used_slugs = set()

        for i, item in enumerate(items_data):
            # A. Inyección de datos
            item['entidad_cod'] = entidad_data.get('cod')
            item['entidad_nombre'] = entidad_data.get('nombre')
            item['entidad_departamento'] = entidad_data.get('departamento')
            item['modalidad'] = convocatoria_data.get('modalidad')
            item['tipo_convocatoria'] = convocatoria_data.get('tipo_convocatoria')
            item['estado'] = "Publicado"

            # B. Generación de Slug
            raw_desc = it.get('descripcion', f'item_{i}')
            slug_base = generate_slug(raw_desc)
            
            # C. Manejo de duplicados
            slug_final = slug_base
            counter = 1
            while slug_final in used_slugs:
                slug_final = f"{slug_base}_{counter}"
                counter += 1
            
            used_slugs.add(slug_final)

            # D. Insertar con slug
            insert_item(db, it, convocatoria_data.get('cuce'), slug_final)

        print(f"✅ Formulario 400 procesado: {convocatoria_data.get('cuce')}")

    except Exception as e:
        print(f"❌ Error fatal procesando {file_name}: {e}")