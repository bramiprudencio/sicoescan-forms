from google.cloud import firestore
from shared.utils import parse_bool, parse_float, slugify, clean_text, parse_date

# ✅ Insertar o Actualizar una entidad
def insert_entidad(db, cod, nombre, fax=None, telefono=None,
      departamento=None, direccion=None, max_autoridad=None,
      max_autoridad_cargo=None, tipo=None):
  """
  Crea la entidad o actualiza sus datos (fax, teléfono) si ya existe.
  """
  entidad_ref = db.collection("entidades").document(cod)
  
  data = {
    "nombre": nombre,
    "fax": fax,
    "telefono": telefono,
    "departamento:": departamento,
    "direccion": direccion,
    "max_autoridad": max_autoridad,
    "max_autoridad_cargo": max_autoridad_cargo,
    "tipo": tipo
  }
  
  # Filtramos None para no borrar datos existentes con nulos accidentalmente
  data = {k: v for k, v in data.items() if v is not None}

  # merge=True asegura que si la entidad ya tenía otros campos, no se borren
  entidad_ref.set(data, merge=True)

# ✅ Insertar o Actualizar una convocatoria
def insert_convocatoria(db, cuce, entidad_cod=None,
    entidad_nombre=None, entidad_departamento=None,
    fecha_publicacion=None, objeto=None,
    modalidad=None, subasta=None, concesion=None,
    tipo_convocatoria=None, forma_adjudicacion=None,
    normativa=None, tipo_contratacion=None,
    metodo_seleccion=None, garantias=None,
    moneda=None, elaboracion_dbc=None,
    recurrente_sgte_gestion=None, total_referencial=None,
    fecha_presentacion=None, fecha_adjudicacion=None,
    fecha_formalizacion=None, fecha_entrega=None,
    estado=None, forms=None):
  
  convocatoria_ref = db.collection("convocatorias").document(cuce)
  
  data = {
    "entidad_cod": entidad_cod,
    "entidad_nombre": entidad_nombre,
    "entidad_departamento": entidad_departamento,
    
    "objeto": objeto,
    "modalidad": modalidad,
    "tipo_convocatoria": tipo_convocatoria,
    "forma_adjudicacion": forma_adjudicacion,
    "normativa": normativa,
    "tipo_contratacion": tipo_contratacion,
    "metodo_seleccion": metodo_seleccion,
    "garantias": garantias,
    "moneda": moneda,
    "elaboracion_dbc": elaboracion_dbc,
    "estado": estado,

    "subasta": parse_bool(subasta),
    "concesion": parse_bool(concesion),
    "recurrente_sgte_gestion": parse_bool(recurrente_sgte_gestion),
    "total_referencial": parse_float(total_referencial),

    "fecha_publicacion": parse_date(fecha_publicacion),
    "fecha_presentacion": parse_date(fecha_presentacion),
    "fecha_adjudicacion": parse_date(fecha_adjudicacion),
    "fecha_formalizacion": parse_date(fecha_formalizacion),
    "fecha_entrega": parse_date(fecha_entrega)
  }

  data = {k: v for k, v in data.items() if v is not None}

  if forms:
    if isinstance(forms, list):
      data["forms"] = firestore.ArrayUnion(forms)
    else:
      data["forms"] = firestore.ArrayUnion([forms])

  convocatoria_ref.set(data, merge=True)

# shared/database.py

# Ejemplo de cómo debería verse insert_item ahora:
def insert_item(db, data, cuce, item_identifier):
  doc_id = f"{cuce}_{item_identifier}"
  for key in data:
    if key.startswith("fecha"):
      data[key] = parse_date(data[key])
    elif key.startswith(("precio", "cantidad")):
      data[key] = parse_float(data[key])
    elif key == "recurrente_sgte_gestion":
      data[key] = parse_bool(data[key])
  db.collection("items").document(doc_id).set(data, merge=True)

def insert_item(db, cuce, item_identifier,
    descripcion=None, catalogo_cod=None, medida=None,
    cantidad_solicitada=None, cantidad_adjudicada=None, cantidad_recepcionada=None,
    precio_referencial=None, precio_referencial_total=None,
    precio_adjudicado=None, precio_adjudicado_total=None,
    fecha_publicacion=None, fecha_presentacion=None,
    estado=None, modalidad=None, tipo_convocatoria=None, tipo_contratacion=None,
    entidad_cod=None, entidad_nombre=None, entidad_departamento=None,
    proponente_nit=None, proponente_nombre=None):
  
  doc_id = f"{cuce}_{item_identifier}"
  data = {
    "cuce": cuce,
    "catalogo_cod": catalogo_cod,
    "descripcion": descripcion,
    "medida": medida,

    "cantidad_solicitada": parse_float(cantidad_solicitada),
    "cantidad_adjudicada": parse_float(cantidad_adjudicada),
    "cantidad_recepcionada": parse_float(cantidad_recepcionada),

    "precio_referencial": parse_float(precio_referencial),
    "precio_referencial_total": parse_float(precio_referencial_total),
    "precio_adjudicado": parse_float(precio_adjudicado),
    "precio_adjudicado_total": parse_float(precio_adjudicado_total),

    "fecha_publicacion": parse_date(fecha_publicacion),
    "fecha_presentacion": parse_date(fecha_presentacion),
    
    "estado": estado,
    "modalidad": modalidad,
    "tipo_convocatoria": tipo_convocatoria,
    "tipo_contratacion": tipo_contratacion,

    "entidad_cod": entidad_cod,
    "entidad_nombre": entidad_nombre,
    "entidad_departamento": entidad_departamento,

    "proponente_nit": proponente_nit,
    "proponente_nombre": proponente_nombre
  }
  data = {k: v for k, v in data.items() if v is not None}
  db.collection("items").document(doc_id).set(data, merge=True)

# ✅ NUEVO: Actualizar estado de convocatoria (Form 500)
def update_convocatoria_status(db, cuce, nuevo_estado, form_tag):
  ref = db.collection("convocatorias").document(cuce)
  try:
    ref.update({
      "estado": nuevo_estado,
      "forms": firestore.ArrayUnion([form_tag])
    })
  except Exception as e:
    print(f"⚠️ No se pudo actualizar convocatoria {cuce} (quizás no existe): {e}")

# ✅ NUEVO: Traer items existentes para compararlos
def get_items_by_cuce(db, cuce):
  items_ref = db.collection("items")
  # Traemos todos los items de ese CUCE
  query = items_ref.where(filter=firestore.FieldFilter("cuce", "==", cuce))
  return query.stream()

# ✅ NUEVO: Actualizar un item específico con datos de adjudicación
def update_item_adjudicacion(db, doc_id, data):
  ref = db.collection("items").document(doc_id)
  # Filtramos None para limpieza
  data = {k: v for k, v in data.items() if v is not None}
  
  try:
    ref.update(data)
  except Exception as e:
    print(f"⚠️ Error actualizando item {doc_id}: {e}")

# ✅ Reutilizamos tu insert_proponente (asegúrate de que esté en este archfparseivo)
def insert_proponente(db, nombre):
  if not nombre: return
  doc_id = slugify(nombre)
  ref = db.collection("proponentes").document(doc_id)
  if not ref.get().exists:
    ref.set({ 
      "nombre": nombre
    })

# ... (imports y funciones anteriores) ...

# ✅ NUEVO: Lógica especial de actualización de estado para Form 170
def check_and_update_convocatoria_170(db, cuce):
  ref = db.collection("convocatorias").document(cuce)
  doc = ref.get()
  
  if doc.exists:
    data = doc.to_dict()
    current_status = data.get('estado')
    
    # Solo actualizamos si está en estados previos válidos
    if current_status in ['Publicado']:
      ref.update({
        'estado': 'Adjudicado',
        'forms': firestore.ArrayUnion(['FORM170'])
      })
      return True
    else:
      # Si ya estaba en otro estado (ej. Contratado), solo agregamos el form tag
      ref.update({
        'forms': firestore.ArrayUnion(['FORM170'])
      })
  return False

# ✅ NUEVO: Actualizar item desierto
def update_item_desierto(db, doc_id, causal):
  ref = db.collection("items").document(doc_id)
  ref.update({
    'estado': 'Desierto',
    'causal_desierto': causal
  })