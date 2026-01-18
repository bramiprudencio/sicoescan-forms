from google.cloud import firestore
from shared.utils import parse_float, slugify, clean_text

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
def insert_convocatoria(
    db, cuce, cod_entidad, fecha_publicacion, objeto,
    modalidad=None, subasta=None, concesion=None,
    tipo_convocatoria=None, forma_adjudicacion=None,
    normativa=None, tipo_contratacion=None,
    metodo_seleccion=None, garantias=None,
    moneda=None, elaboracion_dbc=None,
    recurrente_sgte_gestion=None, total=None,
    fecha_presentacion=None, fecha_adjudicacion=None,
    fecha_formalizacion=None, fecha_entrega=None,
    estado=None, forms=None, 
    # NUEVOS ARGUMENTOS
    entidad_nombre=None, entidad_departamento=None 
):
    convocatoria_ref = db.collection("convocatorias").document(cuce)
    
    data = {
        "cod_entidad": cod_entidad,
        "entidad_nombre": entidad_nombre,           # Guardamos nombre
        "entidad_departamento": entidad_departamento, # Guardamos departamento
        "fecha_publicacion": fecha_publicacion,
        "objeto": objeto,
        # ... (resto de campos igual) ...
        "modalidad": modalidad,
        "subasta": subasta,
        "concesion": concesion,
        "tipo_convocatoria": tipo_convocatoria,
        "forma_adjudicacion": forma_adjudicacion,
        "normativa": normativa,
        "tipo_contratacion": tipo_contratacion,
        "metodo_seleccion": metodo_seleccion,
        "garantias": garantias,
        "moneda": moneda,
        "elaboracion_dbc": elaboracion_dbc,
        "recurrente_sgte_gestion": recurrente_sgte_gestion,
        "total": total,
        "fecha_presentacion": fecha_presentacion,
        "fecha_adjudicacion": fecha_adjudicacion,
        "fecha_formalizacion": fecha_formalizacion,
        "fecha_entrega": fecha_entrega,
        "estado": estado
    }

    data = {k: v for k, v in data.items() if v is not None}

    if forms:
        if isinstance(forms, list):
            data["forms"] = firestore.ArrayUnion(forms)
        else:
            data["forms"] = firestore.ArrayUnion([forms])

    convocatoria_ref.set(data, merge=True)

# shared/database.py

def insert_item(db, data, cuce, index):
    # Generamos el ID secuencial: CUCE_1, CUCE_2, etc.
    doc_id = f"{cuce}_{index}"
            
    data['cuce'] = cuce

    ref = db.collection("items").document(doc_id)
    ref.set(data, merge=True)

# ✅ NUEVO: Actualizar estado de convocatoria (Form 500)
def update_convocatoria_status(db, cuce, nuevo_estado, form_tag):
    ref = db.collection("convocatorias").document(cuce)
    
    # Usamos update porque el documento DEBE existir
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

# ✅ Reutilizamos tu insert_proponente (asegúrate de que esté en este archivo)
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