from bs4 import BeautifulSoup
import re
from shared.utils import clean_text, parse_float
from shared.database import insert_entidad, insert_convocatoria, insert_item

def process_110(html_content, file_name, db):
    print(f"--- Procesando Formulario 110: {file_name} ---")
    
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
    # 1. ENTIDAD
    # ==========================================
    try:
        section_title = soup.find("td", string="1. IDENTIFICACIÓN DE LA ENTIDAD")
        if section_title:
            # Tu lógica: tabla padre -> última fila -> celdas
            entidad_fila = section_title.find_parent("table").find_all("tr")[-1].find_all("td")
            
            entidad_data = {
                "cod": clean_text(entidad_fila[0].get_text(strip=True)),
                "nombre": clean_text(entidad_fila[1].get_text(strip=True)),
                "fax": clean_text(entidad_fila[2].get_text(strip=True)),
                "telefono": clean_text(entidad_fila[3].get_text(strip=True)),
            }

            # Insertar Entidad inmediatamente (es seguro hacerlo aquí)
            if entidad_data.get("cod"):
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
    # 2. CONVOCATORIA (Datos Generales)
    # ==========================================
    try:
        convocatoria_cuce = soup.find('td', class_='FormularioCUCE')
        if convocatoria_cuce:
            convocatoria_data['cuce'] = clean_text(convocatoria_cuce.get_text())
        else:
            print(f"Advertencia: No se encontró CUCE en {file_name}")
            return

        # Mapeo general
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

        # Modalidad (Lógica específica)
        try:
            modalidad_td = soup.find("td", string="Modalidad")
            if modalidad_td:
                convocatoria_data['modalidad'] = clean_text(
                    modalidad_td.find_parent("tr").find_next_sibling("tr").find_all("td")[0].get_text(strip=True)
                )
        except:
            pass
        
        # Cronograma (Lógica específica del 110)
        cronograma_title_td = soup.find('td', class_='FormularioSubtitulo', string=re.compile(r'CRONOGRAMA DE (PROCESO|ACTIVIDADES)', re.IGNORECASE))
        if cronograma_title_td:
            cronograma_table = cronograma_title_td.parent.find_next_sibling('tr').find('table')
            
            def get_date(label_pattern):
                cell = cronograma_table.find('td', string=re.compile(label_pattern))
                return clean_text(cell.find_next_sibling('td').get_text()) if cell else None

            convocatoria_data['fecha_presentacion'] = get_date(r'Presentación')
            convocatoria_data['fecha_formalizacion'] = get_date(r'Formalización')
            convocatoria_data['fecha_entrega'] = get_date(r'Entrega')

        # ==========================================
        # 3. ITEMS Y TOTAL
        # ==========================================
        # Buscamos la tabla que contiene "Código del Catálogo"
        items_section = soup.find("td", string="Código del Catálogo")
        
        if items_section:
            items_table = items_section.find_parent("tr").find_parent("table")
            
            # Limpiar tablas anidadas (decompose)
            [item.decompose() for item in items_table.find_all('table')]
            
            # Obtener TOTAL GENERAL (última celda de la tabla)
            try:
                total_raw = items_table.find_all("td")[-1].get_text()
                convocatoria_data['total'] = parse_float(total_raw)
            except:
                convocatoria_data['total'] = None

            rows = items_table.find_all("tr")
            
            # Tu lógica: Cabecera en índice 1 (rows[1])
            if len(rows) > 1:
                headers = [h.get_text(strip=True) for h in rows[1].find_all("td")]
                
                map_headers = {
                    "Código del Catálogo": "cod_catalogo",
                    "Descripción del bien o servicio": "descripcion",
                    "Unidad de Medida": "medida",
                    "Cantidad": "cantidad_solicitada",
                    "Precio referencial unitario": "precio_referencial",
                    "Precio referencial total": "precio_referencial_total",
                    # Mapeo específico del 110
                    "Precio Unitario del Proveedor Preseleccionado": "precio_referencial",
                    "Precio Total del Proveedor Preseleccionado": "precio_referencial_total"
                }
                headers = [map_headers.get(h, h.lower().replace(" ", "_")) for h in headers]

                # Tu lógica: Iterar desde fila 2 hasta la penúltima (rows[2:-1])
                # Nota: rows[2:-1] ignora la última fila (que suele ser el total)
                for row in rows[2:-1]:
                    # Usamos recursive=False para no agarrar celdas de tablas anidadas si quedó alguna
                    cols = row.find_all("td", recursive=False)
                    
                    # Tu validación: Si no hay cols, o < 2, o la primera columna no es un número
                    if not cols or len(cols) < 2 or not re.fullmatch(r"[0-9]+", cols[0].get_text(strip=True)):
                        continue
                    
                    item = {}
                    
                    # Manejo de la descripción en negrita <b> (si existe)
                    b = row.find("b")
                    if b:
                        # Extraemos texto del b si es necesario, y lo borramos del DOM para no duplicar
                        # catalogo_descripcion = b.get_text() # (Si quisieras usarlo)
                        b.decompose()

                    for i in range(len(cols)):
                        if i < len(headers):
                            key = headers[i]
                            val = cols[i].get_text().strip()
                            
                            # Convertimos a float los campos numéricos
                            if key in ['cantidad_solicitada', 'precio_referencial', 'precio_referencial_total']:
                                item[key] = parse_float(val)
                            else:
                                item[key] = clean_text(val)
                    
                    items_data.append(item)

        # ==========================================
        # 4. GUARDADO FINAL
        # ==========================================

        # Guardar Convocatoria
        insert_convocatoria(
            db,
            cuce=convocatoria_data.get('cuce'),
            cod_entidad=entidad_data.get('cod'),
            entidad_nombre=entidad_data.get('nombre'),
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
            estado="Publicado", # O el estado que prefieras
            forms="FORM100" # Para agregar al array de forms
        )

        # Guardar Items
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
                entidad_departamento=entidad_data.get('departamento')
            )

        print(f"✅ Formulario 110 procesado: {convocatoria_data.get('cuce')}")

    except Exception as e:
        print(f"❌ Error fatal procesando {file_name}: {e}")