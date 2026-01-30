from bs4 import BeautifulSoup
import re
from shared.utils import (
    clean_text,
    extract_cronograma,
    extract_modalidad,
    generate_slug,
    parse_float
)
from shared.firestore import (
    insert_convocatoria,
    insert_entidad,
    insert_item
)

def process_110(html_content, file_name, db):
  print(f"--- Procesando Formulario 110: {file_name} ---")
  
  try:
    soup = BeautifulSoup(html_content, 'html.parser')
  except Exception as e:
    print(f"Error parseando HTML en {file_name}: {e}")
    return

  entidad_data = {}
  convocatoria_data = {}
  items_data = []

  # ==========================================
  # 1. ENTIDAD
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
        "departamento": None 
      }

    if entidad_data.get("cod"):
      entidad_ref = db.collection("entidades").document(entidad_data["cod"])
      entidad_snapshot = entidad_ref.get()

    if entidad_snapshot.exists:
      entidad_data["departamento"] = entidad_snapshot.to_dict().get("departamento")
    else:
      insert_entidad(
      db, 
      entidad_data["cod"], 
      entidad_data["nombre"], 
      fax=entidad_data["fax"], 
      telefono=entidad_data["telefono"]
      )

  except Exception as e:
    print(f"Error extrayendo entidad en {file_name}: {e}")

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
      'Fecha de publicación': 'fecha_publicacion',
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
    convocatoria_data['modalidad'] = extract_modalidad(soup)
    
    # Cronograma
    cronograma_extracted = extract_cronograma(soup)
    convocatoria_data.update(cronograma_extracted)

    # ==========================================
    # 3. ITEMS Y TOTAL
    # ==========================================
    items_table = soup.find("td", string=re.compile(r'Código del? Catálogo'))
    
    if items_table:
      items_table = items_table.find_parent("tr").find_parent("table")
      
      items_cols_size = len(soup.find("td", string=re.compile(r'Código del? Catálogo')).find_parent("tr").find_all('td'))
      [table.decompose() for table in items_table.find_all('table')]
      [row.decompose() for row in items_table.find_all("tr") if len(row.find_all('td')) != items_cols_size]
      rows = items_table.find_all("tr")
      
      if len(rows) > 1:
        headers = [h.get_text(strip=True) for h in rows[0].find_all("td")]
        
        map_headers = {
          "Código del Catálogo": "catalogo_cod",
          "Descripción del bien o servicio": "descripcion",
          "Unidad de Medida": "medida",
          "Cantidad": "cantidad_solicitada",
          "Precio referencial unitario": "precio_referencial",
          "Precio referencial total": "precio_referencial_total",
          "Precio Unitario del Proveedor Preseleccionado": "precio_referencial",
          "Precio Total del Proveedor Preseleccionado": "precio_referencial_total"
        }
        headers = [map_headers.get(h, h.lower().replace(" ", "_")) for h in headers]

        # Iteramos desde la 2da hasta la penúltima (donde suele estar el total)
        for row in rows[1:]:
          cols = row.find_all("td", recursive=False)
          
          if not cols or len(cols) < 2 or cols[0].get_text().startswith("#"): continue

          item = {}

          for i in range(len(cols)):
            if i < len(headers):
              key = headers[i]
              # Mantenemos HTML en descripción
              if key == 'descripcion':
                item[key] = cols[i].decode_contents().strip()
              else:
                item[key] = clean_text(cols[i].get_text().strip())
          items_data.append(item)

    total = 0.0
    for item in items_data:
      total += parse_float(item.get('precio_referencial_total')) or 0.0
    convocatoria_data['total_referencial'] = total

    # ==========================================
    # 4. GUARDADO FINAL (CON CAMPOS EXTRA Y SLUGS)
    # ==========================================

    print(convocatoria_data)
    print('-------------------')
    print(entidad_data)
    print('-------------------')
    print(items_data)
    
    insert_convocatoria(
      db,
      cuce=convocatoria_data.get('cuce'),

      entidad_cod=entidad_data.get('cod'),
      entidad_nombre=entidad_data.get('nombre'),
      entidad_departamento=entidad_data.get('departamento'),

      objeto=convocatoria_data.get('objeto'),
      modalidad=convocatoria_data.get('modalidad'),
      subasta=convocatoria_data.get('subasta') or "No",
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
      total_referencial=convocatoria_data.get('total_referencial'),

      fecha_publicacion=convocatoria_data.get('fecha_publicacion'),
      fecha_presentacion=convocatoria_data.get('fecha_presentacion'),
      fecha_formalizacion=convocatoria_data.get('fecha_formalizacion'),
      fecha_entrega=convocatoria_data.get('fecha_entrega'),

      estado="Publicado",
      forms=file_name.split("FORM")[-1].replace(".html", "")
    )

    # Set para controlar duplicados dentro de la misma convocatoria
    used_slugs = set()

    for i, item in enumerate(items_data):
      raw_desc = item.get('descripcion', f'item_{i}')
      slug_base = generate_slug(raw_desc)
      slug_final = slug_base
      counter = 1
      while slug_final in used_slugs:
        slug_final = f"{slug_base}_{counter}"
        counter += 1
      
      used_slugs.add(slug_final)

      insert_item(
        db,
        cuce=convocatoria_data.get('cuce'),
        item_identifier=slug_final,

        descripcion=item.get('descripcion'),
        catalogo_cod=item.get('catalogo_cod'),
        medida=item.get('medida'),
        cantidad_solicitada=item.get('cantidad_solicitada') or 1.0,
        precio_referencial=item.get('precio_referencial') or 0.0,
        precio_referencial_total=item.get('precio_referencial_total') or 0.0,

        estado="Publicado",
        modalidad=convocatoria_data.get('modalidad'),
        tipo_convocatoria=convocatoria_data.get('tipo_convocatoria'),
        fecha_publicacion=convocatoria_data.get('fecha_publicacion'),
        fecha_presentacion=convocatoria_data.get('fecha_presentacion'),

        entidad_cod=entidad_data.get('cod'),
        entidad_nombre=entidad_data.get('nombre'),
        entidad_departamento=entidad_data.get('departamento')
      )

    print(f"✅ Formulario 110 procesado: {convocatoria_data.get('cuce')}")

  except Exception as e:
    print(f"❌ Error fatal procesando {file_name}: {e}")