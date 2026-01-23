from datetime import datetime, date
import unicodedata, re

from bs4 import BeautifulSoup

def clean_text(text):
    if text:
        return re.sub(r'\s+', ' ', text).strip()
    return None

def slugify(value: str) -> str:
    if value:
        value = value.strip()  # remove whitespace/newlines
        normalized = unicodedata.normalize('NFD', value)
        without_accents = ''.join(
            c for c in normalized if unicodedata.category(c) != 'Mn'
        )
        underscored = without_accents.replace(" ", "_")
        clean = re.sub(r'[^a-zA-Z0-9_]', '', underscored)
        return clean.lower()
    return None

# --- Función Helper para crear IDs limpios ---
def generate_slug(text):
    if not text: return "item"
    # 1. Quitar HTML tags (si quedaron)
    text = BeautifulSoup(text, "html.parser").get_text(separator=" ")
    # 2. Normalizar unicode (quitar acentos: canción -> cancion)
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
    # 3. Quitar caracteres que no sean alfanuméricos o espacios
    text = re.sub(r'[^\w\s-]', '', text).lower()
    # 4. Reemplazar espacios por guiones bajos
    text = re.sub(r'[-\s]+', '_', text).strip('-_')
    # 5. Cortar si es extremadamente largo (Firestore soporta ids largos, pero mejor prevenir)
    return text[:60]

def parse_date(value):
    """Accepts datetime, date, ISO strings, or DD/MM/YYYY strings."""
    if not value:
        return None
    if isinstance(value, (datetime, date)):
        return value
    try:
        # Try ISO format first
        return datetime.fromisoformat(value)
    except Exception:
        pass
    try:
        # Try Bolivian style dd/mm/yyyy
        return datetime.strptime(value, "%d/%m/%Y")
    except Exception:
        return None
    
def parse_float(value):
    """
    Parses a float from a string, handling both US (1,000.00) 
    and European (1.000,00) formats automatically.
    """
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    
    # 1. Convert to string and strip spaces
    s = str(value).strip()
    
    # 2. Optional: Remove currency symbols and other non-numeric chars
    # Keep only digits, dots, commas, and negative signs
    s = re.sub(r'[^\d.,-]', '', s)
    
    # 3. Analyze positions of separators
    last_comma = s.rfind(',')
    last_dot = s.rfind('.')
    
    # Case A: Both separators exist (e.g., "1,234.56" or "1.234,56")
    if last_comma != -1 and last_dot != -1:
        if last_comma > last_dot:
            # European style (dot is thousands, comma is decimal) -> 1.234,56
            s = s.replace('.', '').replace(',', '.')
        else:
            # US style (comma is thousands, dot is decimal) -> 1,234.56
            s = s.replace(',', '')
            
    # Case B: Only commas exist (e.g., "123,45" or "1,000,000")
    elif last_comma != -1:
        # If there is more than one comma, it's definitely a thousands separator
        if s.count(',') > 1:
            s = s.replace(',', '')
        else:
            # If there is only one comma:
            # Usually implies decimal (e.g. "10,50"), UNLESS it is exactly "1,000"
            # Logic: We treat single comma as decimal separator
            s = s.replace(',', '.')

    # Case C: Only dots exist (e.g., "123.45" or "1.000.000")
    elif last_dot != -1:
        # If more than one dot, it's thousands (e.g. 1.000.000)
        if s.count('.') > 1:
            s = s.replace('.', '')
        # If only one dot, Python float() handles it natively
    
    try:
        return float(s)
    except (ValueError, TypeError):
        return None
        
def parse_int(value):
    """Convert string to int, return None if invalid."""
    if not value:
        return None
    try:
        return int(value.replace(",", "").replace(" ", ""))
    except Exception:
        return None

def parse_bool(value):
    """Convert 'Si'/'No' strings to booleans."""
    if value is None:
        return None
    return str(value).strip().lower() == "si"

def normalize_for_match(text):
    if not text: return ""
    # Quitar HTML, acentos, mayúsculas y espacios extra
    text = BeautifulSoup(text, "html.parser").get_text(separator=" ")
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
    return re.sub(r'\s+', ' ', text).strip().lower()