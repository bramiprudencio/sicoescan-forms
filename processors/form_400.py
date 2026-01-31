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

def process_400(html_content, file_name, db):
  print(f"--- Procesando Formulario 400: {file_name} ---")
  
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
    section_entidad = soup.find("font", string=re.compile('ENTIDAD', re.IGNORECASE))
    if section_entidad:
      entidad_fila = section_entidad.find_parent("table").find_all("tr")[-1].find_all("td")

      entidad_data = {
        "cod": clean_text(entidad_fila[0].get_text(strip=True) +
                  ' - ' + entidad_fila[2].get_text(strip=True)),
        "nombre": clean_text(entidad_fila[3].get_text(strip=True)),
        "fax": clean_text(entidad_fila[4].get_text(strip=True)),
        "departamento": None
      }

      if entidad_data.get("cod"):
        entidad_ref = db.collection("entidades").document(entidad_data["cod"])
        entidad_snap = entidad_ref.get()
        
        if entidad_snap.exists:
          existing_data = entidad_snap.to_dict()
          entidad_data["departamento"] = existing_data.get("departamento")
        #else:
          #insert_entidad(
          #  db, 
          #  entidad_data["cod"], 
          #  entidad_data["nombre"], 
          #  entidad_data["fax"]
          #)

  except Exception as e:
    print(f"Error extrayendo entidad en {file_name}: {e}")

  # ==========================================
  # 2. CONVOCATORIA
  # ==========================================
  try:
    convocatoria_cuce = soup.find('td', string='Código Proceso')
    if convocatoria_cuce:
      convocatoria_data['cuce'] = clean_text(convocatoria_cuce.find_next_sibling('td').get_text())
    else:
      print(f"Advertencia: No se encontró CUCE en {file_name}")
      return

    # Modalidad y Objeto
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
    except Exception as e:
      print(f"Error extrayendo normativa: {e}")

    # Fecha formalizacion (presentacion), fecha entrega, total referencial
    try:
      firma_td = soup.find('td', string=re.compile('Fecha de firma', re.IGNORECASE))
      if firma_td:
        convocatoria_row = firma_td.find_parent('table').find_all('tr')[1]
        if len(convocatoria_row.find_all('td')) >= 6:
          cols = convocatoria_row.find_all('td')
          convocatoria_data['fecha_formalizacion'] = clean_text(cols[3].get_text(strip=True)) # fecha_presentacion
          convocatoria_data['total_referencial'] = parse_float(cols[4].get_text(strip=True))
          convocatoria_data['fecha_entrega'] = clean_text(cols[6].get_text(strip=True))
    except Exception as e:
      print(f"Error extrayendo cronograma fijo: {e}")

    # Fecha publicacion
    try:
      fecha_pub_td = soup.find('td', string=re.compile('Fecha de envío del formulario', re.IGNORECASE))
      if fecha_pub_td:
        raw_fecha = fecha_pub_td.find_next_sibling('td').get_text(strip=True)
        convocatoria_data['fecha_publicacion'] = raw_fecha.split(' ')[0]
    except Exception as e:
      print(f"Error extrayendo fecha de publicación: {e}")

    # Moneda
    try:
      moneda_b = soup.find('b', string=re.compile('Moneda del contrato', re.IGNORECASE))
      if moneda_b:
        convocatoria_data['moneda'] = clean_text(moneda_b.find_parent('td').find_next_sibling('td').get_text(strip=True))
    except Exception as e:
      print(f"Error extrayendo moneda: {e}")
    
    # TIpo contratacion
    try:
      tipo_contr_td = soup.find('td', string=re.compile('Tipo de contratación', re.IGNORECASE))
      if tipo_contr_td:
        convocatoria_data['tipo_contratacion'] = clean_text(
          tipo_contr_td.find_parent('tr').find_next_sibling('tr').find_all('td')[-1].get_text(strip=True)
        )
    except Exception as e:
      print(f"Error extrayendo tipo de contratación: {e}")

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
          "Código del Catálogo": "catalogo_cod",
          "Código del Catálogo (UNSPSC)": "catalogo_cod",
          "Descripción del bien o servicio": "descripcion",
          "Descripción del bien, obra, servicio general o de consultoría": "descripcion",
          "Unidad de Medida": "unidad",      
          "Unidad de medida": "unidad",      
          "Cantidad": "cantidad",        
          "Cantidad / Cantidad estimada si es variable": "cantidad",
          "La cantidad es:": "cantidad",
          "Precio unitario": "precio_referencial",     
          "Precio referencial unitario": "precio_referencial", 
          "Precio referencial total": "precio_referencial_total",
          "Monto total (p.unit. x cantidad) / Total estimado cuando la cantidad es variable": "precio_referencial_total",
          "Origen del item": "origen"
        }
        headers = [map_headers.get(h, h.lower().replace(" ", "_")) for h in headers]

        for row in rows[1:]:
          cols = row.find_all("td")
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

    # ==========================================
    # 4. GUARDADO
    # ==========================================
    print(entidad_data)
    print(convocatoria_data)
    print(f"Items encontrados: {items_data}")
    insert_convocatoria(
      db,
      cuce=convocatoria_data.get('cuce'),

      entidad_cod=entidad_data.get('cod'),
      entidad_nombre=entidad_data.get('nombre'),
      entidad_departamento=entidad_data.get('departamento'),

      objeto=convocatoria_data.get('objeto'),
      modalidad=convocatoria_data.get('modalidad'),
      subasta=convocatoria_data.get('subasta') or "No",
      #concesion=convocatoria_data.get('concesion'),
      tipo_convocatoria=convocatoria_data.get('tipo_convocatoria') or "Convocatoria Publica Nacional",
      forma_adjudicacion=convocatoria_data.get('forma_adjudicacion'),
      normativa=convocatoria_data.get('normativa'),
      tipo_contratacion=convocatoria_data.get('tipo_contratacion'),
      metodo_seleccion=convocatoria_data.get('metodo_seleccion'),
      garantias=convocatoria_data.get('garantias'),
      moneda=convocatoria_data.get('moneda'),
      elaboracion_dbc=convocatoria_data.get('elaboracion_dbc'),
      recurrente_sgte_gestion=convocatoria_data.get('recurrente_sgte_gestion') or "No",
      total_referencial=convocatoria_data.get('total_referencial'),

      fecha_publicacion=convocatoria_data.get('fecha_publicacion'),
      fecha_presentacion=convocatoria_data.get('fecha_formalizacion'),
      fecha_adjudicacion=convocatoria_data.get('fecha_formalizacion'),
      fecha_formalizacion=convocatoria_data.get('fecha_formalizacion'),
      fecha_entrega=convocatoria_data.get('fecha_entrega'),

      estado="Publicado", 
      forms=file_name.split("FORM")[-1].replace(".html", "")
    )

    # Set para controlar duplicados de slugs
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
        tipo_convocatoria=convocatoria_data.get('tipo_convocatoria') or "Convocatoria Publica Nacional",
        tipo_contratacion=convocatoria_data.get('tipo_contratacion'),
        fecha_publicacion=convocatoria_data.get('fecha_publicacion'),
        fecha_presentacion=convocatoria_data.get('fecha_formalizacion'),

        entidad_cod=entidad_data.get('cod'),
        entidad_nombre=entidad_data.get('nombre'),
        entidad_departamento=entidad_data.get('departamento')
      )

    print(f"✅ Formulario 400 procesado: {convocatoria_data.get('cuce')}")

  except Exception as e:
    print(f"❌ Error fatal procesando {file_name}: {e}")