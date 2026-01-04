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
    db, cuce, cod_entidad,
    entidad_nombre=None, entidad_departamento=None,
    fecha_publicacion=None, objeto=None,
    modalidad=None, subasta=None, concesion=None,
    tipo_convocatoria=None, forma_adjudicacion=None,
    normativa=None, tipo_contratacion=None,
    metodo_seleccion=None, garantias=None,
    moneda=None, elaboracion_dbc=None,
    recurrente_sgte_gestion=None, total=None,
    fecha_presentacion=None, fecha_adjudicacion=None,
    fecha_formalizacion=None, fecha_entrega=None,
    estado=None, forms=None
):
    convocatoria_ref = db.collection("convocatorias").document(cuce)
    
    data = {
        "cod_entidad": cod_entidad,
        "entidad_nombre": entidad_nombre,
        "entidad_departamento": entidad_departamento,
        "fecha_publicacion": fecha_publicacion,
        "objeto": objeto,
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

    # 1. Filtramos keys con valores None
    data = {k: v for k, v in data.items() if v is not None}

    # 2. Manejo especial para 'forms' (ArrayUnion)
    # Si quieres AGREGAR un formulario a la lista existente sin borrar los anteriores:
    if forms:
        if isinstance(forms, list):
            # Agrega solo valores únicos al array existente en Firestore
            data["forms"] = firestore.ArrayUnion(forms)
        else:
            data["forms"] = firestore.ArrayUnion([forms])

    # 3. Guardar con merge=True
    convocatoria_ref.set(data, merge=True)

# ✅ Insertar o Actualizar un item
def insert_item(db, cuce, cod_catalogo, descripcion, medida=None,
                cantidad_solicitada=None, precio_referencial=None,
                precio_referencial_total=None, fecha_publicacion=None,
                estado=None, entidad_cod=None, entidad_nombre=None,
                entidad_departamento=None):
    
    # ID Compuesto único
    doc_id = f"{cuce}_{slugify(descripcion)}"
    items_ref = db.collection("items").document(doc_id)
    
    data = {
        "cuce": cuce,
        "cod_catalogo": cod_catalogo,
        "descripcion": descripcion,
        "medida": medida,
        "cantidad_solicitada": cantidad_solicitada,
        "precio_referencial": precio_referencial,
        "precio_referencial_total": precio_referencial_total,
        "fecha_publicacion": fecha_publicacion,
        "estado": estado,
        "entidad_cod": entidad_cod,
        "entidad_nombre": entidad_nombre,
        "entidad_departamento": entidad_departamento
    }

    # Filtramos None
    data = {k: v for k, v in data.items() if v is not None}

    # Upsert
    items_ref.set(data, merge=True)