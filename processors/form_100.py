from bs4 import BeautifulSoup
import re
from shared.utils import clean_text, parse_float
from shared.firestore import insert_entidad, insert_convocatoria, insert_item

def process_100(html_content, file_name, db):
    print(f"--- Procesando Formulario 100: {file_name} ---")
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
    except Exception as e:
        print(f"Error parseando HTML en {file_name}: {e}")
        return

    # Variables
    entidad_data = {}
    convocatoria_data = {}
    items_data = []

    # ==========================================
    # 1. ENTIDAD (Extracción y Consulta)
    # ==========================================
    try:
        section_title = soup.find("td", string="1. IDENTIFICACIÓN DE LA ENTIDAD")
        if section_title:
            entidad_fila = section_title.find_parent("table").find_all("tr")[-1].find_all("td")
            
            entidad_data = {
                "cod": clean_text(entidad_fila[0].get_text(strip=True)),
                "nombre": clean_text(entidad_fila[1].get_text(strip=True)),
                "fax": clean_text(entidad_fila[2].get_text(strip=True)),
                "telefono": clean_text(entidad_fila[3].get_text(strip=True)),
                "departamento": None # Inicializamos en None
            }

            # --- NUEVA LÓGICA: CONSULTAR DEPARTAMENTO EN FIRESTORE ---
            if entidad_data.get("cod"):
                # 1. Referencia al documento de la entidad
                entidad_ref = db.collection("entidades").document(entidad_data["cod"])
                
                # 2. Obtenemos el documento (Snapshot)
                entidad_snapshot = entidad_ref.get()

                if entidad_snapshot.exists:
                    existing_data = entidad_snapshot.to_dict()
                    # 3. Extraemos el departamento si existe
                    entidad_data["departamento"] = existing_data.get("departamento")
                    # print(f"Departamento recuperado: {entidad_data['departamento']}")
                
                # 4. Actualizamos/Creamos la entidad con los datos nuevos del form (fax, tel)
                # Nota: Esto NO borrará el departamento gracias a la lógica en database.py
                insert_entidad(
                    db, 
                    entidad_data["cod"], 
                    entidad_data["nombre"], 
                    entidad_data["fax"], 
                    entidad_data["telefono"]
                )

    except Exception as e:
        print(f"Error procesando entidad en {file_name}: {e}")

    # ==========================================
    # 2. CONVOCATORIA
    # ==========================================
    try:
        convocatoria_cuce = soup.find('td', class_='FormularioCUCE')
        if convocatoria_cuce:
            convocatoria_data['cuce'] = clean_text(convocatoria_cuce.get_text())
        else:
            print(f"Advertencia: No se encontró CUCE en {file_name}")
            return 

        mapping = {
            'Fecha de publicación (en el SICOES)': 'fecha_publicacion',
            'Objeto de la Contratación': 'objeto',
            'Subasta': 'subasta',
            'Concesión Administrativa': 'concesion',
            'Tipo de convocatoria': 'tipo_convocatoria',
            'Forma de adjudicación': 'forma_adjudicacion',
            'Normativa utilizada': 'normativa',
            'Tipo de contratación': 'tipo_contratacion',
            'Método de selección y adjudicación': 'metodo_seleccion',
            'Garantías solicitadas': 'garantias',
            'Moneda considerada para el proceso': 'moneda',
            'Elaboración del DBC': 'elaboracion_dbc',
            'Bienes o servicios recurrentes con cargo a la siguiente gestión:': 'recurrente_sgte_gestion',
        }

        for label_text, key in mapping.items():
            label_td = soup.find('td', class_=re.compile(r'FormularioEtiqueta'), string=re.compile(re.escape(label_text), re.IGNORECASE))
            if label_td:
                value_td = label_td.find_next_sibling('td', class_=re.compile(r'FormularioDato'))
                if value_td:
                    convocatoria_data[key] = clean_text(value_td.get_text())

        # Modalidad
        try:
            modalidad_td = soup.find("td", string="Modalidad")
            if modalidad_td:
                convocatoria_data['modalidad'] = clean_text(modalidad_td.find_parent("tr").find_next_sibling("tr").find_all("td")[0].get_text(strip=True))
        except:
            pass

        # ==========================================
        # 3. CRONOGRAMA
        # ==========================================
        cronograma_title_td = soup.find('td', class_='FormularioSubtitulo', string=re.compile(r'CRONOGRAMA DE (PROCESO|ACTIVIDADES)', re.IGNORECASE))
        if cronograma_title_td:
            cronograma_table = cronograma_title_td.parent.find_next_sibling('tr').find('table')
            
            def get_date_val(pattern):
                cell = cronograma_table.find('td', string=re.compile(pattern))
                return clean_text(cell.find_next_sibling('td').get_text()) if cell else None

            first_row = cronograma_table.find('tr').find_all('td')
            cronograma_cols = []
            for i in range(len(first_row)):
                txt = first_row[i].get_text()
                if 'Actividad' in txt or 'Fecha' in txt:
                    cronograma_cols.append(i)
            
            for row in cronograma_table.find_all('tr'):
                for i, cell in enumerate(row.find_all('td')):
                    if i not in cronograma_cols:
                        cell.decompose()
            
            convocatoria_data['fecha_presentacion'] = get_date_val(r'Presentación')
            convocatoria_data['fecha_adjudicacion'] = get_date_val(r'Adjudicación')
            convocatoria_data['fecha_formalizacion'] = get_date_val(r'Formalización')
            convocatoria_data['fecha_entrega'] = get_date_val(r'Entrega')

        # ==========================================
        # 4. ITEMS Y TOTALES
        # ==========================================
        items_table = soup.find("td", string=re.compile(r'Código del? Catálogo'))
        
        if items_table:
            items_table = items_table.find_parent("tr").find_parent("table")
            total_raw = items_table.find_all("td")[-1].get_text()
            convocatoria_data['total'] = parse_float(total_raw)

            items_cols_size = len(soup.find("td", string=re.compile(r'Código del? Catálogo')).find_parent("tr").find_all('td'))
            [table.decompose() for table in items_table.find_all('table')]
            [row.decompose() for row in items_table.find_all("tr") if len(row.find_all('td')) != items_cols_size]
            
            rows = items_table.find_all("tr")
            if len(rows) > 0:
                headers = [h.get_text(strip=True) for h in rows[0].find_all("td")]
                map_headers = {
                    "Código del Catálogo": "cod_catalogo",
                    "Código de Catálogo": "cod_catalogo",
                    "Descripción del bien o servicio": "descripcion",
                    "Unidad de Medida": "medida",
                    "Cantidad": "cantidad_solicitada",
                    "Precio referencial unitario": "precio_referencial",
                    "Precio referencial total": "precio_referencial_total"
                }
                headers = [map_headers.get(h, h.lower().replace(" ", "_")) for h in headers]

                for row in rows[1:]:
                    cols = row.find_all("td")
                    if not cols or len(cols) < 2: continue
                    
                    item = {}
                    b_tag = row.find("b")
                    if b_tag: b_tag.decompose()

                    for i in range(len(cols)):
                        if i < len(headers):
                            key = headers[i]
                            val = cols[i].get_text().strip()
                            if key in ['cantidad_solicitada', 'precio_referencial', 'precio_referencial_total']:
                                item[key] = parse_float(val)
                            else:
                                item[key] = clean_text(val)
                    items_data.append(item)

        # ==========================================
        # 5. GUARDADO (PASANDO EL DEPARTAMENTO)
        # ==========================================
        
        insert_convocatoria(
            db,
            cuce=convocatoria_data.get('cuce'),
            cod_entidad=entidad_data.get('cod'),
            entidad_nombre=entidad_data.get('nombre'),
            # AQUI pasamos el departamento obtenido de Firestore
            entidad_departamento=entidad_data.get('departamento'), 
            fecha_publicacion=convocatoria_data.get('fecha_publicacion'),
            objeto=convocatoria_data.get('objeto'),
            modalidad=convocatoria_data.get('modalidad'),
            subasta=convocatoria_data.get('subasta'),
            concesion=convocatoria_data.get('concesion'),
            tipo_convocatoria=convocatoria_data.get('tipo_convocatoria'),
            forma_adjudicacion=convocatoria_data.get('forma_adjudicacion'),
            normativa=convocatoria_data.get('normativa'),
            tipo_contratacion=convocatoria_data.get('tipo_contratacion'),
            metodo_seleccion=convocatoria_data.get('metodo_seleccion'),
            garantias=convocatoria_data.get('garantias'),
            moneda=convocatoria_data.get('moneda'),
            elaboracion_dbc=convocatoria_data.get('elaboracion_dbc'),
            recurrente_sgte_gestion=convocatoria_data.get('recurrente_sgte_gestion'),
            total=convocatoria_data.get('total'),
            fecha_presentacion=convocatoria_data.get('fecha_presentacion'),
            fecha_adjudicacion=convocatoria_data.get('fecha_adjudicacion'),
            fecha_formalizacion=convocatoria_data.get('fecha_formalizacion'),
            fecha_entrega=convocatoria_data.get('fecha_entrega'),
            estado="Publicado", 
            forms="FORM100" 
        )

        for it in items_data:
            insert_item(
                db,
                cuce=convocatoria_data.get('cuce'),
                cod_catalogo=it.get('cod_catalogo'),
                descripcion=it.get('descripcion'),
                medida=it.get('medida'),
                cantidad_solicitada=it.get('cantidad_solicitada'),
                precio_referencial=it.get('precio_referencial'),
                precio_referencial_total=it.get('precio_referencial_total'),
                entidad_cod=entidad_data.get('cod'),
                entidad_nombre=entidad_data.get('nombre'),
                # AQUI TAMBIEN
                entidad_departamento=entidad_data.get('departamento')
            )

        print(f"✅ Formulario 100 procesado: {convocatoria_data.get('cuce')}")

    except Exception as e:
        print(f"❌ Error fatal procesando {file_name}: {e}")