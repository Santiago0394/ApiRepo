# buk_export_simple.py
import os
import sys
import re
import unicodedata
import getpass
import requests, pandas as pd
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime
from decimal import Decimal

# -------- Dónde guardar el CSV (junto al .exe si está congelado) --------
if getattr(sys, "frozen", False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# -------- Config --------
BASE = "https://deloitte-innomotics.buk.cl/api/v1/chile"
PAGE_SIZE = 1000
TIMEOUT = 20

# Generar nombres de archivo con fecha actual en formato ddmmaaaa
fecha_actual = datetime.now().strftime("%d%m%Y")
OUT_CSV_SEMI = os.path.join(BASE_DIR, f"Database_CL_{fecha_actual}.csv")
OUT_CSV_FILTERED = os.path.join(BASE_DIR, f"Database_CL_BAJAS_{fecha_actual}.csv")

# -------- Helpers --------
PREFIXES = ["de la","de los","de las","del","de","van","von","da","di","do"]
SUFFIXES = {"jr","sr","iii","iv","v"}

COUNTRY_OF_BIRTH_MAP = {
    "DE": "DEU",
    "AR": "ARG",
    "AU": "AUS",
    "AT": "AUT",
    "BS": "BHS",
    "BRB": "BRB",
    "BZ": "BLZ",
    "BO": "BOL",
    "BR": "BRA",
    "CL": "CHL",
    "CN": "CHN",
    "CO": "COL",
    "CR": "CRI",
    "DOM": "DOM",
    "EC": "ECU",
    "ES": "ESP",
    "US": "USA",
    "FR": "FRA",
    "GRC": "GRC",
    "GT": "GTM",
    "GY": "GUY",
    "HT": "HTI",
    "NL": "NLD",
    "HN": "HND",
    "EN": "IND",
    "IDN": "IDN",
    "ISR": "ISR",
    "IT": "ITA",
    "JM": "JAM",
    "JP": "JPN",
    "LV": "LVA",
    "MLT": "MLT",
    "MX": "MEX",
    "NI": "NIC",
    "NO": "NOR",
    "NZL": "NZL",
    "PAM": "PAN",
    "PY": "PRY",
    "PE": "PER",
    "PL": "POL",
    "PT": "PRT",
    "PR": "PRI",
    "RO": "ROU",
    "RU": "RUS",
    "SV": "SLV",
    "SE": "SWE",
    "CH": "CHE",
    "SR": "SUR",
    "TR": "TUR",
    "UA": "UKR",
    "VE": "VEN",
    "CU": "CUB",
    "HR": "HRV",
    "GB": "GBR",
}

# -------- Mapeo de bancos --------
BANK_CODE_MAP = {
    "BCI": "16",
    "BICE": "28",
    "Banco de Chile": "1",
    "COOPEUCH": "672",
    "Banco Estado": "2",
    "Falabella": "51",
    "Ripley": "53",
    "Santander": "37",
    "Scotiabank": "14",
    "Security": "49",
    "Itau": "39",
    "BBVA": "504",
    "Consorcio": "55"
}

def fetch_latest_open_period(session) -> tuple[str, str]:
    """
    Devuelve (start_yyyymmdd, end_yyyymmdd) del período 'abierto' más reciente.
    Si no hay 'abierto', retorna ("","").
    """
    url = f"{BASE}/process_periods"
    try:
        r = session.get(url, timeout=TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        print(f"⚠️ No se pudo obtener process_periods (abierto): {e}")
        return "", ""

    payload = r.json()
    items = payload.get("data", payload) or []
    abiertos = []
    for it in items:
        if str(it.get("status", "")).strip().lower() == "abierto":
            start = to_yyyymmdd(it.get("month"))
            end   = to_yyyymmdd(it.get("end_date"))
            if start and end:
                abiertos.append((start, end))

    if not abiertos:
        return "", ""

    abiertos.sort(key=lambda t: t[1])  # más reciente por end_date
    return abiertos[-1]


def fetch_latest_closed_period(session) -> tuple[str, str]:
    """
    Llama a /process_periods y devuelve (start_yyyymmdd, end_yyyymmdd)
    del período 'cerrado' más reciente. Si no hay cerrados, devuelve ("","").
    """
    url = f"{BASE}/process_periods"
    try:
        r = session.get(url, timeout=TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        print(f"⚠️ No se pudo obtener process_periods: {e}")
        return "", ""

    payload = r.json()
    items = payload.get("data", payload) or []
    cerrados = []
    for it in items:
        if str(it.get("status", "")).strip().lower() == "cerrado":
            start = to_yyyymmdd(it.get("month"))
            end   = to_yyyymmdd(it.get("end_date"))
            if start and end:
                cerrados.append((start, end))

    if not cerrados:
        return "", ""

    # más reciente = el de mayor end_date
    cerrados.sort(key=lambda t: t[1])  # ordena por end
    return cerrados[-1]


def split_prefix_suffix(surname_full):
    s = (surname_full or "").strip()
    if not s: return "",""
    low = s.lower()
    pref = ""
    for p in sorted(PREFIXES, key=len, reverse=True):
        if low.startswith(p + " "):
            pref = s[:len(p)]
            break
    suf = ""
    last = s.split()[-1].lower().strip(".")
    if last in SUFFIXES:
        suf = s.split()[-1]
    return pref, suf

def map_bank_code(bank_name):
    """
    Mapea el nombre del banco chileno al código numérico correspondiente.
   
    Args:
        bank_name: Nombre del banco (ej: "Banco Estado", "Santander", etc.)
   
    Returns:
        str: Código numérico del banco (ej: "2", "37") o el valor original si no se encuentra
    """
    if not bank_name:
        return ""
   
    # Normalizar el nombre del banco (eliminar espacios extra, capitalizar)
    bank_clean = str(bank_name).strip()
   
    # Buscar coincidencia exacta primero
    if bank_clean in BANK_CODE_MAP:
        return BANK_CODE_MAP[bank_clean]
   
    # Buscar coincidencia insensible a mayúsculas/minúsculas
    for bank_key, bank_code in BANK_CODE_MAP.items():
        if bank_clean.lower() == bank_key.lower():
            return bank_code
   
    # Si no se encuentra, devolver el valor original
    return bank_clean

def map_gender(val):
    if not val: return ""
    v = str(val).strip().lower()
    if v in ("m","male","masculino","hombre"): return 1
    if v in ("f","female","femenino","mujer"): return 2
    return ""

def analyze_employee_status(emp):
    """
    LÓGICA SIMPLIFICADA:
    - Si tiene end_date → va al archivo FILTRADO
    - Si NO tiene end_date → va al archivo ACTIVOS
   
    Args:
        emp: Diccionario con datos del empleado
       
    Returns:
        dict: {
            "is_active": bool,
            "end_date": str or None,
            "destination": "active" or "filtered"
        }
    """
    job = emp.get("current_job") or {}
    end_date = job.get("end_date")
   
    # Lógica simple: tiene end_date = filtrado, no tiene = activo
    if end_date:
        return {
            "is_active": False,
            "end_date": end_date,
            "destination": "filtered"
        }
    else:
        return {
            "is_active": True,
            "end_date": None,
            "destination": "active"
        }

def to_yyyymmdd(val):
    if not val: return ""
    if isinstance(val, datetime): return val.strftime("%Y%m%d")
    s = str(val).strip()
    if len(s) == 8 and s.isdigit(): return s
    for fmt in ("%Y-%m-%d","%d/%m/%Y","%Y/%m/%d","%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y%m%d")
        except:
            pass
    return ""

def _norm_country(value: str) -> str:
    if not value: return ""
    v = str(value).strip().upper()
    if v in {"CL","CH","CHILE","CHILENO","CHILENA"}:
        return "CL"
    if len(v) == 2 and v.isalpha(): return v
    return v[:2]

def map_country_of_birth(value) -> str:
    if not value:
        return ""
    code = str(value).strip().upper()
    return COUNTRY_OF_BIRTH_MAP.get(code, code)

def nationality_codes(emp, ca):
    nats = emp.get("nationalities")
    if isinstance(nats, list) and nats:
        codes = [_norm_country(x) for x in nats if str(x).strip()]
        codes = [c for c in codes if c]
        codes += ["","",""]
        return codes[0], codes[1], codes[2]
    nat = emp.get("nationality")
    if isinstance(nat, str) and nat.strip():
        return _norm_country(nat), "", ""
    ca1 = ca.get("Nationality 1") or ca.get("nationality_1")
    ca2 = ca.get("Nationality 2") or ca.get("nationality_2")
    ca3 = ca.get("Nationality 3") or ca.get("nationality_3")
    return _norm_country(ca1), _norm_country(ca2), _norm_country(ca3)

def _norm_key(s: str) -> str:
    if s is None: return ""
    s = s.lower()
    s = (s.replace("á","a").replace("é","e").replace("í","i")
           .replace("ó","o").replace("ú","u").replace("ñ","n"))
    for ch in ("-", "_"):
        s = s.replace(ch, " ")
    return " ".join(s.split())

# -------- FIX: conservar ceros (evitar perder 0 con `or`) --------
def get_from_attrs(emp, keys, prefer_job=False, date=False):
    def _search(dct):
        if not isinstance(dct, dict):
            return None
        wanted = {_norm_key(x) for x in keys}
        for k, v in dct.items():
            if _norm_key(k) in wanted:
                return v
        return None

    job = emp.get("current_job") or {}
    job_ca = job.get("custom_attributes") or {}
    emp_ca = emp.get("custom_attributes") or {}

    if prefer_job:
        val = _search(job_ca)
        if val is None:
            val = _search(emp_ca)
    else:
        val = _search(emp_ca)
        if val is None:
            val = _search(job_ca)

    if date:
        return to_yyyymmdd(val)
    return "" if val is None else str(val).strip()

def find_any(emp, aliases, date=False):
    alias_norm = {_norm_key(a) for a in aliases}
    def _from(d):
        if not isinstance(d, dict):
            return None
        for k, v in d.items():
            if _norm_key(k) in alias_norm:
                return v
        return None
    v = _from(emp)
    if v is None: v = _from(emp.get("custom_attributes") or {})
    if v is None: v = _from(emp.get("current_job") or {})
    if v is None: v = _from((emp.get("current_job") or {}).get("custom_attributes") or {})
    v = "" if v is None else str(v).strip()
    return to_yyyymmdd(v) if date else v

def normalize_workforce_type(emp: dict) -> str:
    raw_num = get_from_attrs(emp, ["Workforce Type"], prefer_job=True)
    if str(raw_num).strip().isdigit():
        return str(raw_num).strip()
    raw_txt = get_from_attrs(emp, ["Tipo de Trabajador", "Worker Type"], prefer_job=True)
    s = (raw_txt or "").strip().upper()
    if s in {"GASTO", "GASTOS"}: return "1"
    if s in {"COSTO", "COSTOS"}: return "2"
    return ""

def determine_exit_reason(emp: dict, job: dict, company_exit_date: str) -> str:
    """
    Mapea termination_reason del API a códigos según tu tabla.
    """
    # Obtener termination_reason del current_job
    termination_reason = job.get("termination_reason", "")
    
    # Normalizar (lower case, sin espacios)
    termination_reason_clean = str(termination_reason).lower().strip()
    
    # Mapear usando TERMINATION_REASON_MAP
    exit_code = TERMINATION_REASON_MAP.get(termination_reason_clean)
    
    return exit_code

def find_local_pay_level(emp: dict) -> str:
    """
    Obtiene 'Local Pay Level' priorizando:
    1) current_job.custom_attributes
    2) emp.custom_attributes
    3) jobs[].custom_attributes (toma el del job más reciente por start_date)
    Evita valores vacíos o 'NOT_APPLICABLE'.
    """
    aliases = ["Local Pay Level", "Local pay level", "Local PayLevel", "Pay Level", "Pay level"]

    # 1) current_job.custom_attributes (prefer_job=True ya mira job_ca primero)
    v = get_from_attrs(emp, aliases, prefer_job=True)
    if v and v.upper() != "NOT_APPLICABLE":
        return v.strip()

    # 2) emp.custom_attributes
    v = get_from_attrs(emp, aliases, prefer_job=False)
    if v and v.upper() != "NOT_APPLICABLE":
        return v.strip()

    # 3) jobs[]: elegir el valor del job más reciente por start_date
    best = ""
    best_date = ""
    alias_norm = {_norm_key(a) for a in aliases}

    for job in (emp.get("jobs") or []):
        ca = job.get("custom_attributes") or {}
        candidate = None
        for k, val in ca.items():
            if _norm_key(k) in alias_norm:
                candidate = str(val).strip()
                break

        if candidate and candidate.upper() != "NOT_APPLICABLE":
            d = to_yyyymmdd(job.get("start_date"))
            # Prefiere el job con start_date más reciente; si no hay fecha, usa el primero válido
            if d and (not best_date or d > best_date):
                best, best_date = candidate, d
            elif not best:
                best = candidate

    if best:
        return best.strip()

    # 4) Último recurso: buscar en estructuras planas
    v = find_any(emp, aliases)
    if v and v.upper() != "NOT_APPLICABLE":
        return v.strip()

    return ""



TERMINATION_REASON_MAP = {
    # Valores que se muestran en la api
    "renuncia": "1",
    "necesidades_empresa": "2", 
    "mutuo_acuerdo": "3",
    "vencimiento_plazo": "7",
    "fin_servicio": "7",
    "muerte": "6",
    "no_concurrencia": "99",
    "incumplimiento": "99",
    "falta_probidad": "99",
    
    # valores que podrian aparecer mas adelante
    "transferencia": "4",        
    "retiro": "5",                
    "divesture": "2", #Puede relacionarse a necesidades_empresa           
    "reset_entry": "95",       
}
# ---------- Normalizador ASCII ----------
def normalize_ascii(text: str) -> str:
    if text is None:
        return ""
    if not isinstance(text, str):
        return text
    t = (text.replace("–","-").replace("—","-")
              .replace("“",'"').replace("”",'"').replace("’","'")
              .replace("º","o").replace("ª","a"))
    t = t.replace("ñ","n").replace("Ñ","N")
    t = unicodedata.normalize("NFKD", t).encode("ascii","ignore").decode("ascii")
    t = re.sub(r"\s+"," ", t).strip()
    return t

def normalize_row_text(row: dict) -> dict:
    return {k: (normalize_ascii(v) if isinstance(v, str) else v) for k, v in row.items()}

def map_contract_type_code(value) -> str:
    if value is None:
        return ""
    s = normalize_ascii(str(value)).lower().strip()
    s = re.sub(r"\s+", " ", s)
    if "indefinid" in s or s in {"indef", "permanente", "permanent", "p"}:
        return "P"
    if "fijo" in s or "plazo" in s or "temporal" in s or "fixed" in s or "term" in s or s == "t":
        return "T"
    return ""

def convert_to_chl_code(contract_type_code):
    """Convierte P/T a códigos CHL"""
    if contract_type_code == "P":
        return "CHL-03"
    elif contract_type_code == "T":
        return "CHL-04"
    else:
        return ""

def map_contract_status_code(emp: dict) -> str:
    """Mapea el estado del contrato a códigos numéricos:
    0 = Terminated (tiene end_date)
    1 = Dormant (inactivo pero sin end_date)  
    3 = Active (activo y sin end_date)"""
    job = emp.get("current_job") or {}
    end_date = job.get("end_date")
    status = emp.get("status", "").lower()
   
    # Si tiene end_date → Terminated
    if end_date:
        return "0"
   
    # Sin end_date pero inactivo → Dormant
    if status in ["inactivo", "inactive", "suspenso", "suspended"]:
        return "1"
   
    # Sin end_date y activo → Active (default)
    return "3"
   

def format_decimal_two_places(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        value = 1 if value else 0
    if isinstance(value, (int, float, Decimal)):
        try:
            return f"{Decimal(str(value)):.2f}"
        except Exception:
            return str(value)
    s = str(value).strip()
    if not s:
        return ""
    s_norm = s.replace(" ", "")
    if "," in s_norm and "." in s_norm:
        if s_norm.rfind(",") > s_norm.rfind("."):
            s_norm = s_norm.replace(".", "")
            s_norm = s_norm.replace(",", ".")
        else:
            s_norm = s_norm.replace(",", "")
    elif "," in s_norm:
        if s_norm.count(",") == 1 and len(s_norm.split(",")[-1]) <= 2:
            s_norm = s_norm.replace(",", ".")
        else:
            s_norm = s_norm.replace(",", "")
    s_clean = re.sub(r"[^\d.\-]", "", s_norm)
    if s_clean in ("", ".", "-", "-.", ".-"):
        return s
    try:
        return f"{Decimal(s_clean):.2f}"
    except Exception:
        return s
# -------- Fecha Null a 99991231 --------
def handle_null_date(value, default="99991231"):
    """Convierte null/None a fecha por defecto"""
    if value is None or value == "":
        return default
    return to_yyyymmdd(value) or default

# -------- Mapeo contract_type a código numérico --------

def map_contract_type_status(contract_type_raw):
    """Mapea el tipo de contrato a código numérico"""
    if not contract_type_raw:
        return ""
   
    contract_type_clean = str(contract_type_raw).strip().lower()
   
    if contract_type_clean == "indefinido":
        return "1"
    elif contract_type_clean == "fijo":
        return "2"
    else:
        return ""

# -------- Mapeo Management Group a Employee Category --------    
def map_employee_category(mgmt_group_value):
    """Mapea Management Group a Employee Category"""
    if not mgmt_group_value:
        return ""
   
    mgmt_group_clean = str(mgmt_group_value).strip().upper()
   
    if mgmt_group_clean == "O":
        return "Individual Contributor"
    else:
        return "Management"


# -------- Mapea de 1,2 a Sr./Sra. --------
def map_salutation(val):
    """Convierte el resultado de map_gender a (SR. o SRA.)"""
    gender_value = map_gender(val)
   
    if gender_value == 1:
        return "SR."
    elif gender_value == 2:
        return "SRA."
    else:
        return ""

def is_valid_date(date_string, min_date="20220801"):
    if not date_string:
        return False
   
    return date_string >= min_date

# -------- Analiza x contratos para saber fecha de ingreso antigua vs contrato actual --------
def analyze_employee_contracts(emp):
    """
    Analiza contratos para obtener fecha de ingreso real vs contrato actual
   
    SOLUCIÓN ROBUSTA: Busca la fecha más antigua entre todos los contratos
    sin depender del orden del array jobs (que puede variar)
    """
    current_job = emp.get("current_job", {})
    jobs = emp.get("jobs", [])
   
    # Fecha del contrato actual (viene del current_job)
    current_contract_date = to_yyyymmdd(current_job.get("start_date"))
   
    # Buscar la fecha más antigua entre TODOS los contratos
    oldest_start_date = ""
    if jobs:
        # Obtener todas las fechas de start_date válidas
        all_dates = []
        for job in jobs:
            start_date = job.get("start_date")
            if start_date and start_date.strip():
                # Convertir a formato comparable (YYYYMMDD)
                formatted_date = to_yyyymmdd(start_date)
                if formatted_date:  # Solo agregar si la conversión fue exitosa
                    all_dates.append(formatted_date)
       
        # Encontrar la fecha más antigua (menor valor en formato YYYYMMDD)
        if all_dates:
            oldest_start_date = min(all_dates)
   
    return {
        "oldest_start_date": oldest_start_date or current_contract_date,  # Entrada real
        "current_contract_date": current_contract_date,                   # Contrato actual
        "debug": {
            "total_contracts": len(jobs),
            "has_multiple_contracts": len(jobs) > 1
        }
    }

# -------- Columnas de salida --------
COLS = [
    "Personnel Number","GID","Surname","Name",
    "Middle Initial","Aristocratic Title","Surname Prefix","Surname Suffix",
    "Preferred Name / Nickname","Surname 2","Title","Gender","Date of Birth",
    "Nationality 1","Nationality 2","Nationality 3","Highest Level of Education",
    "Contract Type","Contract Status","Contractual Weekly Working Time",
    "Standard Work Week","Company Entry Date","Service Date","Entry Reason","Company Exit Date",
    "Exit Reason","Workforce Type","Management Group","Date Management Group","ARE",
    "Location / Office (short name)","In-company Manager","OrgCode","Technical PMP Flag","GPM Status",
    "Country/Region - Place of Action","Tax Country/Region","Tax Country/Region State","Date Location Change",
    "Address 1","Address 2","Address 3","City","State","Country/Region - Home Address","Postal Code",
    "Incentive Payment Type","Cost Center","Functional Area","Country/Region","HR Service Area","Local Pay Level",
    "Date Workforce Type","Contract Date","Base Pay","Target Incentive Amount","Currency","Local Job Title",
    "Date Local Job Title","Depth Structure","Date GPM Status","GPM Exit Status","Date Contract Status","Date Base Pay",
    "Date Target Incentive Amount","Global Cost Center","Name (International)","Surname (International)",
    "Preferred Surname","Eligibility for Compensation Planning","GRIP Position","SPS_Eligibility","Date SPS_Eligibility",
    "Total Target Cash","Date Total Target Cash","Private E-mail Address",
    "Private Mobile Phone Number","Base Salary","Date Base Salary","Fixed Allowances","Date Fixed Allowance","JobRegion",
    "Finance Company Code","Currency Payroll","LTI_Eligibility","Date LTI_Eligibility","Bank Country/Region Code","Bank Code",
    "Bank Control Key","Account Number","International Bank Account Number","Payroll Area","Termination Date",
    "Last Date Worked","Position","Legal Entity",
    "Employee Group","Employee Category","Time Management Status","Employee Subgroup","Pay Scale Type","Pay Scale Area",
    "Pay Scale Group","Contract Type ","Standard Weekly Hours","Country of Birth","Salutation","Preferred Name","Line Manager",
    "SuccessFactors ID",
]

# Columnas para empleados filtrados (incluye motivo de filtro)
FILTERED_COLS = COLS + ["Filter Reason"]

def find_local_pay_level_strict(emp: dict) -> str:
    '''
    SOLO devuelve 'Local Pay Level' si su longitud > 9 (p. ej. 'CL_CSSOSEROPTL09').
    Busca la clave normalizándola (tolerante a NBSP, guiones/underscores, mayúsculas, etc.).
    Prioridad:
      1) current_job.custom_attributes['Local Pay Level']
      2) emp.custom_attributes['Local Pay Level']
      3) jobs[].custom_attributes['Local Pay Level'] (job más reciente por start_date)
    '''
    def _is_long_ok(v):
        if v is None:
            return False
        s = str(v).strip()
        return len(s) > 9 and s.upper() != "NOT_APPLICABLE"

    def _get_norm_key(dct, target="Local Pay Level"):
        # NBSP (\u00A0) → espacio normal, luego _norm_key
        if not isinstance(dct, dict):
            return None
        wanted = _norm_key(str(target).replace('\u00A0',' '))
        for k, v in dct.items():
            k_norm = _norm_key(str(k).replace('\u00A0',' '))
            if k_norm == wanted:
                return v
        return None

    # 1) current_job.custom_attributes
    job = (emp.get("current_job") or {})
    job_ca = job.get("custom_attributes") or {}
    v = _get_norm_key(job_ca, "Local Pay Level")
    if _is_long_ok(v):
        return str(v).strip()

    # 2) emp.custom_attributes
    emp_ca = emp.get("custom_attributes") or {}
    v = _get_norm_key(emp_ca, "Local Pay Level")
    if _is_long_ok(v):
        return str(v).strip()

    # 3) recorrer jobs (toma el más reciente por start_date)
    best = ""
    best_date = ""
    for jb in (emp.get("jobs") or []):
        ca = jb.get("custom_attributes") or {}
        v = _get_norm_key(ca, "Local Pay Level")
        if _is_long_ok(v):
            d = to_yyyymmdd(jb.get("start_date"))
            if d and (not best_date or d > best_date):
                best, best_date = str(v).strip(), d
            elif not best:
                best = str(v).strip()

    return best


def find_local_pay_level_exhaustive(emp: dict, *, return_debug: bool = False):
    """
    Busca 'Local Pay Level' en TODO el objeto del empleado:
      1) current_job.custom_attributes['Local Pay Level']   (si es válido → devuelve)
      2) emp.custom_attributes['Local Pay Level']           (si es válido → devuelve)
      3) jobs[].custom_attributes['Local Pay Level']        (elige el del job más reciente por start_date)
      4) BÚSQUEDA PROFUNDA: recorre todo el dict/list y junta cualquier 'Local Pay Level' que aparezca en cualquier parte.
         Aplica heurísticas para escoger el mejor: prefiere longitud > 9, que empiece con 'CL_', y la cadena más larga.

    Si return_debug=True, además devuelve info de cómo/desde dónde se obtuvo.
    """
    TARGET_KEY = "Local Pay Level"

    def _is_valid(v: object) -> bool:
        if v is None:
            return False
        s = str(v).strip()
        if not s or s.upper() == "NOT_APPLICABLE":
            return False
        # Lo que pides como "correcto" siempre es largo. Mantengo el umbral >9.
        return len(s) > 9

    def _norm(s: str) -> str:
        # misma normalización que usas en _norm_key, ampliada con NBSP → espacio
        s = "" if s is None else str(s)
        s = s.replace("\u00A0", " ")
        s = s.lower()
        s = (s.replace("á","a").replace("é","e").replace("í","i")
               .replace("ó","o").replace("ú","u").replace("ñ","n"))
        for ch in ("-", "_"):
            s = s.replace(ch, " ")
        return " ".join(s.split())

    wanted = _norm(TARGET_KEY)

    def _get(dct: dict):
        """Obtiene por clave normalizada (tolera NBSP, guion/underscore, mayúsculas, dobles espacios)."""
        if not isinstance(dct, dict):
            return None
        for k, v in dct.items():
            if _norm(str(k)) == wanted:
                return v
        return None

    def to_date(s):
        return to_yyyymmdd(s) if s else ""

    # 1) current_job.custom_attributes
    job = emp.get("current_job") or {}
    job_ca = job.get("custom_attributes") or {}
    v = _get(job_ca)
    if _is_valid(v):
        return (str(v).strip(), {"source": "current_job.custom_attributes"}) if return_debug else str(v).strip()

    # 2) emp.custom_attributes
    emp_ca = emp.get("custom_attributes") or {}
    v = _get(emp_ca)
    if _is_valid(v):
        return (str(v).strip(), {"source": "emp.custom_attributes"}) if return_debug else str(v).strip()

    # 3) jobs[].custom_attributes (elige el más reciente por start_date)
    best = ""
    best_date = ""
    best_src = None
    for idx, jb in enumerate(emp.get("jobs") or []):
        ca = (jb or {}).get("custom_attributes") or {}
        vv = _get(ca)
        if _is_valid(vv):
            d = to_date((jb or {}).get("start_date"))
            if d and (not best_date or d > best_date):
                best, best_date = str(vv).strip(), d
                best_src = {"source": f"jobs[{idx}].custom_attributes", "job_start_date": d}
            elif not best:
                best = str(vv).strip()
                best_src = {"source": f"jobs[{idx}].custom_attributes", "job_start_date": d}
    if best:
        return (best, best_src) if return_debug else best

    # 4) BÚSQUEDA PROFUNDA (recorre todo el objeto)
    candidates = []  # (valor, path_string)
    def walk(obj, path="emp"):
        if isinstance(obj, dict):
            # recoge si tiene la clave
            val = _get(obj)
            if _is_valid(val):
                candidates.append((str(val).strip(), path))
            # sigue recorriendo
            for k, v in obj.items():
                walk(v, f"{path}.{k}")
        elif isinstance(obj, list):
            for i, it in enumerate(obj):
                walk(it, f"{path}[{i}]")
        # otros tipos: nada

    walk(emp)

    if candidates:
        # Heurística de selección:
        #  - prioriza que empiece con 'CL_'
        #  - mayor longitud
        #  - última opción: el primero que aparezca
        def score(val_path):
            val, p = val_path
            sc = 0
            if val.startswith("CL_"):
                sc += 3
            sc += min(len(val), 60) / 10.0  # más largo, mejor
            return sc
        candidates.sort(key=score, reverse=True)
        best_val, best_path = candidates[0]
        return (best_val, {"source": "deep_scan", "path": best_path}) if return_debug else best_val

    # Nada encontrado
    return ("", {"source": "none"}) if return_debug else ""

import re

def get_local_pay_level_best(emp: dict, *, return_debug: bool=False):
    """
    Devuelve el Local Pay Level de forma robusta.
    Prioridad:
      1) current_job.custom_attributes["Local Pay Level"]
      2) emp.custom_attributes["Local Pay Level"]
      3) jobs[].custom_attributes["Local Pay Level"] (job más reciente por start_date)
      4) deep-scan de todo el objeto (clave normalizada)
      5) derivar desde GRIP Position: 'CS-XX-YYY-ZZ09' -> 'CL_CSXXYYYZZ09' (con 'CL_' y sin '-')
      6) regex: primer string que parezca 'CL_' + >=7 chars mayúsculas/dígitos
    """
    def _is_valid(v):
        if v is None: return False
        s = str(v).strip()
        return s and s.upper() != "NOT_APPLICABLE" and len(s) > 9

    def _norm_key(s: str) -> str:
        s = "" if s is None else str(s)
        s = s.replace("\u00A0", " ").lower()  # NBSP → espacio
        s = (s.replace("á","a").replace("é","e").replace("í","i")
               .replace("ó","o").replace("ú","u").replace("ñ","n"))
        for ch in ("-", "_"):
            s = s.replace(ch, " ")
        return " ".join(s.split())

    TARGET = _norm_key("Local Pay Level")

    def _get_from(dct):
        if not isinstance(dct, dict): return None
        for k,v in dct.items():
            if _norm_key(k) == TARGET:
                return v
        return None

    def _to_date(s): return to_yyyymmdd(s) if s else ""

    dbg = {"source": None}

    # 1) current_job.custom_attributes
    job = emp.get("current_job") or {}
    job_ca = job.get("custom_attributes") or {}
    v = _get_from(job_ca)
    if _is_valid(v):
        if return_debug: dbg["source"] = "current_job.custom_attributes"
        return (str(v).strip(), dbg) if return_debug else str(v).strip()

    # 2) emp.custom_attributes
    emp_ca = emp.get("custom_attributes") or {}
    v = _get_from(emp_ca)
    if _is_valid(v):
        if return_debug: dbg["source"] = "emp.custom_attributes"
        return (str(v).strip(), dbg) if return_debug else str(v).strip()

    # 3) jobs[] más reciente
    best, best_date = "", ""
    best_src = None
    for idx, jb in enumerate(emp.get("jobs") or []):
        ca = (jb or {}).get("custom_attributes") or {}
        vv = _get_from(ca)
        if _is_valid(vv):
            d = _to_date((jb or {}).get("start_date"))
            if d and (not best_date or d > best_date):
                best, best_date = str(vv).strip(), d
                best_src = f"jobs[{idx}].custom_attributes"
            elif not best:
                best = str(vv).strip()
                best_src = f"jobs[{idx}].custom_attributes"
    if best:
        if return_debug: dbg["source"] = best_src
        return (best, dbg) if return_debug else best

    # 4) deep scan por clave normalizada
    candidates = []
    def walk(obj, path="emp"):
        if isinstance(obj, dict):
            val = _get_from(obj)
            if _is_valid(val):
                candidates.append((str(val).strip(), path))
            for k, v in obj.items():
                walk(v, f"{path}.{k}")
        elif isinstance(obj, list):
            for i, it in enumerate(obj):
                walk(it, f"{path}[{i}]")
    walk(emp)
    if candidates:
        # preferir que empiece por CL_, luego mayor longitud
        def score(item):
            val, _ = item
            s = 0
            if val.startswith("CL_"): s += 3
            s += min(len(val), 60)/10.0
            return s
        candidates.sort(key=score, reverse=True)
        best_val, best_path = candidates[0]
        if return_debug: dbg.update({"source":"deep_scan", "path": best_path})
        return (best_val, dbg) if return_debug else best_val

    # 5) derivar desde GRIP Position → CL_ + (GRIP sin '-')
    grip = (job_ca.get("GRIP Position") or emp_ca.get("GRIP Position") or "").strip()
    if grip:
        derived = f"CL_{grip.replace('-', '')}"
        if _is_valid(derived):
            if return_debug: dbg["source"] = "derived_from_GRIP"
            return (derived, dbg) if return_debug else derived

    # 6) regex: cualquier valor que parezca un LPL
    #    Ej: CL_CSSOSEROPTL09, CL_MFMRMATECTE999, etc.
    REG = re.compile(r"\bCL_[A-Z0-9]{7,}\b")
    def find_any_string(obj):
        out = []
        def w(o):
            if isinstance(o, dict):
                for k,v in o.items(): w(v)
            elif isinstance(o, list):
                for it in o: w(it)
            else:
                s = str(o)
                for m in REG.findall(s):
                    out.append(m)
        w(obj)
        return out
    regex_hits = find_any_string(emp)
    if regex_hits:
        if return_debug: dbg["source"] = "regex_scan"
        return (regex_hits[0], dbg) if return_debug else regex_hits[0]

    if return_debug: dbg["source"] = "none"
    return ("", dbg) if return_debug else ""



def build_employee_row(emp, filter_reason=None):
    """
    Construye una fila completa para un empleado.
    Si filter_reason se proporciona, se agrega como columna adicional.
    """
    ca = emp.get("custom_attributes") or {}
    job = emp.get("current_job") or {}
    job_ca = job.get("custom_attributes") or {}

    # Identificación / nombres
    # Obtener RUT y separar dígito verificador para ordenamiento correcto
    dni_raw = (emp.get("dni") or emp.get("document_number","")).replace(".","")
    # Si tiene guión, separar parte numérica y verificador
    if "-" in dni_raw:
        parts = dni_raw.split("-")
        dni = parts[0] + parts[1]  # formato: "10213754k" o "65589729"
    else:
        # Si NO tiene guión, asumir que último carácter es verificador
        dni = dni_raw
    
    first_name = emp.get("first_name","").strip()
    first_name_first = first_name.split()[0] if first_name else ""  # solo primer nombre
    s1 = emp.get("surname") or emp.get("last_name","")
    s2 = emp.get("second_surname","")
    surname = " ".join([p for p in [s1, s2] if p]).strip()

    # E,F,G,H vacías
    arist_title = (ca.get("Title") or "").strip()
    gender = map_gender(emp.get("gender"))
    dob = to_yyyymmdd(emp.get("date_of_birth") or emp.get("birth_date") or emp.get("birthday"))
    nat1, nat2, nat3 = nationality_codes(emp, ca)

    # Educación
    highest_edu = ( get_from_attrs(emp, ["Highest Level of Education","Education Level","Nivel educacional"])
                    or find_any(emp, ["Highest Level of Education","Education Level","Nivel educacional"]) )

    # Contratos / horas
    contract_type_raw = ( get_from_attrs(emp, ["Contract Type","Tipo de contrato"], prefer_job=True)
                          or str(job.get("contract_type") or "").strip() )
   
    contract_type_code = map_contract_type_code(contract_type_raw)
    contract_status = map_contract_status_code(emp)
    contractual_weekly = str(job.get("weekly_hours") or "").strip()
    try:
        if contractual_weekly not in ("", None):
            contractual_weekly = f"{float(str(contractual_weekly).replace(',','.')):.2f}"
    except:
        pass
   
    # Standard Work Week: Jornada estándar legal en Chile (45 horas semanales)
    standard_work_week = "45.00"  # ← Fijo según ley laboral chilena (Art. 22 Código del Trabajo)

    # Fechas laborales
    contract_analysis = analyze_employee_contracts(emp)
    service_date = contract_analysis["oldest_start_date"]
    company_entry_date = contract_analysis["oldest_start_date"]
    date_contract_status = to_yyyymmdd(job.get("start_date"))
   
    entry_reason = ( job.get("entry_reason")
                     or get_from_attrs(emp, ["Entry Reason","Razón de entrada"], prefer_job=True) or "" )
    company_exit_date = to_yyyymmdd(job.get("end_date")) or "99991231"

    # Determinar Exit Reason según reglas
    exit_reason = determine_exit_reason(emp, job, company_exit_date)

    workforce_type = normalize_workforce_type(emp)
    mgmt_group = get_from_attrs(emp, ["Management Group"], prefer_job=True)
   
    # Date Management Group
    date_senior_mgmt = get_from_attrs(emp, ["Date Management group"], prefer_job=True, date=True)
    if date_senior_mgmt and date_senior_mgmt != "99991231":
        date_mgmt_group = date_senior_mgmt
    else:
        date_mgmt_group = to_yyyymmdd(job.get("start_date"))
   
    are = get_from_attrs(emp, ["ARE"], prefer_job=True)
    loc_short = get_from_attrs(emp, ["Location / Office (short name)"], prefer_job=True) or emp.get("office_short_name","")
    in_company_mgr = get_from_attrs(emp, ["In-company Manager","Line Manager"], prefer_job=True)
    org_code = get_from_attrs(emp, ["OrgCode"], prefer_job=True)
    tech_pmp_flag = get_from_attrs(emp, ["Technical PMP Flag"], prefer_job=True)
    gpm_status = get_from_attrs(emp, ["GPM Status"], prefer_job=True)
    place_action = get_from_attrs(emp, ["Country/Region - Place of Action"], prefer_job=True) or emp.get("country_code","")
    tax_country = get_from_attrs(emp, ["Tax Country/Region"], prefer_job=True)
    tax_state = get_from_attrs(emp, ["Tax Country/Region State"], prefer_job=True)
    date_loc_change = company_entry_date

    # Direcciones
    addr1 = get_from_attrs(emp, ["Address 1"]) or emp.get("address", "") or emp.get("address_line1","")
    addr2 = get_from_attrs(emp, ["Address 2"]) or emp.get("address_line2","")
    addr3 = get_from_attrs(emp, ["Address 3"]) or emp.get("address_line3","")
    city = emp.get("district", "")
    state = get_from_attrs(emp, ["Tax Country/Region State"], prefer_job=True)
    country_home = get_from_attrs(emp, ["Country/Region - Home Address"], prefer_job=True)
    postal_code = get_from_attrs(emp, ["Postal Code","Código Postal"], prefer_job=False)

    # Compensaciones / estructura
    incentive_payment_type = get_from_attrs(emp, ["Incentive Payment Type"], prefer_job=True)
    cost_center = ( get_from_attrs(emp, ["Cost Center"], prefer_job=True)
                    or emp.get("current_job",{}).get("cost_center","") )
    functional_area = get_from_attrs(emp, ["Functional Area"], prefer_job=True)
    country_region = get_from_attrs(emp, ["Country/Region Sub Entity", "Country/Region"], prefer_job=True) or emp.get("country","")
    hr_service_area = get_from_attrs(emp, ["HR Service Area"], prefer_job=True)
    local_pay_level = get_local_pay_level_best(emp)
    date_workfoce_type = company_entry_date

    contract_date = company_entry_date

    # Base Pay: 2 decimales
    base_pay_raw = get_from_attrs(emp, ["Base Pay","Salario Base","Salary Base"], prefer_job=True)
    base_pay = ""
    if base_pay_raw not in ("", None):
        s = str(base_pay_raw).strip().replace(",", "")
        s = re.sub(r"[^\d.\-]", "", s)
        try:
            base_pay = f"{Decimal(s):.2f}"
        except Exception:
            base_pay = s

    # Target Incentive Amount: cambiar 0, null o vacío por NOT_APPLICABLE
    tia_raw = get_from_attrs(emp, ["Target Incentive Amount"], prefer_job=True)
    if tia_raw in ("", None):
        target_incentive_amount = "NOT_APPLICABLE"
    else:
        # Verificar si representa 0 en cualquier formato (0, "0", "0.0", 0.0, etc.)
        try:
            if float(tia_raw) == 0.0:
                target_incentive_amount = "NOT_APPLICABLE"
            else:
                # Formatear siempre con 2 decimales
                target_incentive_amount = f"{float(tia_raw):.2f}"
        except (ValueError, TypeError):
            # Si no se puede convertir a float, mantener el valor original
            target_incentive_amount = str(tia_raw).strip()

    currency = get_from_attrs(emp, ["Currency"], prefer_job=True) or emp.get("current_job",{}).get("currency_code","")

    # Local Job Title desde role.name, solo antes de "/"
    role_name = ((job.get("role") or {}).get("name") or "").strip()
    if role_name:
        local_job_title = role_name.split("/", 1)[0].strip()
    else:
        local_job_title = get_from_attrs(emp, ["Local Job Title"], prefer_job=True)

    date_local_job_title = get_from_attrs(emp, ["Date Local Job Title"], prefer_job=True, date=True)
    depth_structure = get_from_attrs(emp, ["Depth Structure"], prefer_job=True)
    date_gpm_status = date_contract_status or get_from_attrs(emp, ["Date GPM Status"], prefer_job=True, date=True)
    gpm_exit_status = get_from_attrs(emp, ["GPM Exit Status"], prefer_job=True)
    date_base_pay = get_from_attrs(emp, ["Date Base Pay"], prefer_job=True, date=True)
    date_target_incentive_amount = get_from_attrs(emp, ["Date Target Incentive Amount"], prefer_job=True, date=True)
    global_cost_center = get_from_attrs(emp, ["Global Cost Center"], prefer_job=True)

    # Nombre internacional / otros
    name_international = first_name_first
    surname_international = surname
    preferred_surname = get_from_attrs(emp, ["Preferred Surname","Apellido preferido"], prefer_job=False)
    eligibility_comp = get_from_attrs(emp, ["Eligibility for Compensation Planning"], prefer_job=True)
    grip_position = get_from_attrs(emp, ["GRIP Position"], prefer_job=True)
    sps_elig = get_from_attrs(emp, ["SPS_Eligibility"], prefer_job=True)
    date_sps_elig = to_yyyymmdd(job.get("start_date"))
   
    total_target_cash_raw = get_from_attrs(emp, ["Total Target Cash"], prefer_job=True)
    total_target_cash = ""
    if total_target_cash_raw not in ("", None):
        s_raw = str(total_target_cash_raw).strip()
        s = s_raw.replace(" ", "")
        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "")
                s = s.replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "," in s:
            if s.count(",") == 1 and len(s.split(",")[-1]) <= 2:
                s = s.replace(",", ".")
            else:
                s = s.replace(",", "")
        s = re.sub(r"[^\d.\-]", "", s)
        if s not in ("", ".", "-", "-.", ".-"):
            try:
                total_target_cash = f"{Decimal(s):.2f}"
            except Exception:
                total_target_cash = s_raw
        else:
            total_target_cash = ""
    date_total_target_cash = get_from_attrs(emp, ["Date Total Target Cash"], prefer_job=True, date=True)

    private_email = (
        get_from_attrs(emp, ["Private E-mail Address","Private Email Address","Correo personal","Email personal"], prefer_job=False)
        or emp.get("personal_email") or emp.get("private_email") or ""
    )
    private_mobile = (
        get_from_attrs(emp, ["Private Mobile Phone Number","Private Phone","Mobile personal","Celular personal"], prefer_job=False)
        or emp.get("personal_mobile") or emp.get("private_mobile") or emp.get("mobile") or emp.get("cellphone") or emp.get("phone") or ""
    )
   
    base_wage_val = emp.get("base_wage")
    if base_wage_val is None and isinstance(job, dict):
        base_wage_val = job.get("base_wage")
    if base_wage_val is None:
        base_salary_raw = get_from_attrs(emp, ["Base Salary","Salary Base","Sueldo Base","Base Pay"], prefer_job=True)
    else:
        base_salary_raw = base_wage_val
    base_salary = format_decimal_two_places(base_salary_raw)
    date_base_salary = get_from_attrs(emp, ["Date Base Salary","Base Salary Date"], prefer_job=True, date=True)

    fixed_allowances = get_from_attrs(emp, ["Fixed Allowances"], prefer_job=True)
    date_fixed_allowance = get_from_attrs(emp, ["Date Fixed Allowance"], prefer_job=True, date=True)
    job_region = job_ca.get("JobRegion", "")
    finance_company_code = get_from_attrs(emp, ["Finance Company Code"], prefer_job=True)
    currency_payroll = get_from_attrs(emp, ["Currency – Payroll","Currency - Payroll","Currency Payroll","Currency–Payroll","Currency-Payroll"], prefer_job=True)
    lti_elig = get_from_attrs(emp, ["LTI_Eligibility"], prefer_job=True)
    date_lti_elig = get_from_attrs(emp, ["Date LTI_Eligibility"], prefer_job=True, date=True)
    bank_country = get_from_attrs(emp, ["Bank Country/Region Code","Bank Country/Region"], prefer_job=True)
   
    # Bank Code: buscar primero en custom_attributes, si no existe usar el campo "bank" del empleado
    bank_code_raw = get_from_attrs(emp, ["Bank Code"], prefer_job=True)
    if not bank_code_raw:
        # Si no hay Bank Code en custom_attributes, usar el campo "bank" y mapearlo
        bank_name = emp.get("bank", "")
        bank_code = map_bank_code(bank_name)
    else:
        # Si ya hay Bank Code, usarlo directamente
        bank_code = bank_code_raw
   
    bank_control_key = get_from_attrs(emp, ["Bank Control Key"], prefer_job=True)
    account_number = get_from_attrs(emp, ["Account Number"], prefer_job=False) or emp.get("account_number","")
    iban = get_from_attrs(emp, ["International Bank Account Number","IBAN"], prefer_job=True)
    payroll_area = get_from_attrs(emp, ["Payroll Area"], prefer_job=True)
    termination_date = handle_null_date(emp.get("current_job", {}).get("end_date"))
    last_date_worked = to_yyyymmdd(job.get("end_date"))
    position = get_from_attrs(emp, ["Position"], prefer_job=True)
    legal_entity = get_from_attrs(emp, ["Legal Entity"], prefer_job=True)

    # Campos adicionales
    employee_group = map_contract_type_status(contract_type_raw)
    employee_category = map_employee_category(mgmt_group)
    time_mgmt_status = ( find_any(emp, ["Time Management Status","Time Mgmt Status","Estado de gestión de tiempo"])
                         or get_from_attrs(emp, ["Time Management Status"], prefer_job=True) )
    employee_subgroup = str(job_ca.get("Employee Subgroup") or ca.get("Employee Subgroup") or "").strip()
    pay_scale_type    = str(job_ca.get("Pay Scale Type")     or ca.get("Pay Scale Type")     or "").strip()
    pay_scale_area    = str(job_ca.get("Pay Scale Area")     or ca.get("Pay Scale Area")     or "").strip()
    pay_scale_group = get_from_attrs(emp, ["Pay Scale Group", "Grupo de escala salarial"], prefer_job=True) or ""

    # Nuevos campos
    country_of_birth = map_country_of_birth(emp.get("country_code"))
    salutation = map_salutation(emp.get("gender"))
    line_manager = get_from_attrs(emp, ["Line Manager", "Manager Name", "Jefe directo", "Supervisor"], prefer_job=True) or ""
    successfactors_id = ca.get("Codigo SF") or ca.get("CodigoSF") or ca.get("SuccessFactors ID", "")

    # Construir la fila base
    row = normalize_row_text({
        "Personnel Number": dni,
        "GID": ca.get("GID",""),
        "Surname": surname,
        "Name": first_name_first,
        "Middle Initial": "",
        "Aristocratic Title": "",
        "Surname Prefix": "",
        "Surname Suffix": "",
        "Preferred Name / Nickname": first_name_first,
        "Surname 2": s2,
        "Title": arist_title,
        "Gender": gender,
        "Date of Birth": dob,
        "Nationality 1": nat1,
        "Nationality 2": nat2,
        "Nationality 3": nat3,
        "Highest Level of Education": highest_edu,
        "Contract Type": contract_type_code,
        "Contract Status": contract_status,
        "Contractual Weekly Working Time": contractual_weekly,
        "Standard Work Week": standard_work_week,
        "Company Entry Date": company_entry_date,
        "Service Date": service_date,
        "Entry Reason": entry_reason,
        "Company Exit Date": company_exit_date,
        "Exit Reason": exit_reason,
        "Workforce Type": workforce_type,
        "Management Group": mgmt_group,
        "Date Management Group": date_mgmt_group,
        "ARE": are,
        "Location / Office (short name)": loc_short,
        "In-company Manager": in_company_mgr,
        "OrgCode": org_code,
        "Technical PMP Flag": tech_pmp_flag,
        "GPM Status": gpm_status,
        "Country/Region - Place of Action": place_action,
        "Tax Country/Region": tax_country,
        "Tax Country/Region State": tax_state,
        "Date Location Change": date_loc_change,
        "Address 1": addr1,
        "Address 2": addr2,
        "Address 3": addr3,
        "City": city,
        "State": state,
        "Country/Region - Home Address": country_home,
        "Postal Code": postal_code,
        "Incentive Payment Type": incentive_payment_type,
        "Cost Center": cost_center,
        "Functional Area": functional_area,
        "Country/Region": country_region,
        "HR Service Area": hr_service_area,
        "Local Pay Level": local_pay_level,
        "Date Workforce Type": date_workfoce_type,
        "Contract Date": contract_date,
        "Base Pay": base_pay,
        "Target Incentive Amount": target_incentive_amount,
        "Currency": currency,
        "Local Job Title": local_job_title,
        "Date Local Job Title": date_local_job_title,
        "Depth Structure": depth_structure,
        "Date GPM Status": date_gpm_status,
        "GPM Exit Status": gpm_exit_status,
        "Date Contract Status": date_contract_status,
        "Date Base Pay": date_base_pay,
        "Date Target Incentive Amount": date_target_incentive_amount,
        "Global Cost Center": global_cost_center,
        "Name (International)": first_name_first,
        "Surname (International)": surname,
        "Preferred Surname": preferred_surname,
        "Eligibility for Compensation Planning": eligibility_comp,
        "GRIP Position": grip_position,
        "SPS_Eligibility": sps_elig,
        "Date SPS_Eligibility": date_sps_elig,
        "Total Target Cash": total_target_cash,
        "Date Total Target Cash": date_total_target_cash,
        "Private E-mail Address": private_email,
        "Private Mobile Phone Number": private_mobile,
        "Base Salary": base_salary,
        "Date Base Salary": date_base_salary,
        "Fixed Allowances": fixed_allowances,
        "Date Fixed Allowance": date_fixed_allowance,
        "JobRegion": job_region,
        "Finance Company Code": finance_company_code,
        "Currency Payroll": currency_payroll,
        "LTI_Eligibility": lti_elig,
        "Date LTI_Eligibility": date_lti_elig,
        "Bank Country/Region Code": bank_country,
        "Bank Code": bank_code,
        "Bank Control Key": bank_control_key,
        "Account Number": account_number,
        "International Bank Account Number": iban,
        "Payroll Area": payroll_area,
        "Termination Date": termination_date,
        "Last Date Worked": last_date_worked,
        "Position": position,
        "Legal Entity": legal_entity,
        "Employee Group": employee_group,
        "Employee Category": employee_category,
        "Time Management Status": time_mgmt_status,
        "Employee Subgroup": employee_subgroup,
        "Pay Scale Type": pay_scale_type,
        "Pay Scale Area": pay_scale_area,
        "Pay Scale Group": pay_scale_group,
        "Contract Type ": convert_to_chl_code(contract_type_code),
        "Standard Weekly Hours": contractual_weekly,
        "Country of Birth": country_of_birth,
        "Salutation": salutation,
        "Preferred Name": first_name_first,
        "Line Manager": line_manager,
        "SuccessFactors ID": successfactors_id,
    })
   
    # Si se proporciona un motivo de filtro, agregarlo
    if filter_reason:
        row["Filter Reason"] = filter_reason
   
    return row

def main():
    # --- pedir token con prioridad env var ---
    auth = os.getenv("BUK_AUTH_TOKEN")
    if not auth:
        auth = getpass.getpass("Ingresa tu token BUK: ").strip()
        while not auth:
            auth = getpass.getpass("El token no puede estar vacío. Inténtalo de nuevo: ").strip()
        print("Token ingresado correctamente.")

    # --- sesión http ---
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.4, status_forcelist=[429, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=10))
    session.headers.update({
        "auth_token": auth,
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })

    # --- períodos para filtros ---
    period_closed_start, period_closed_end = fetch_latest_closed_period(session)
    if period_closed_start and period_closed_end:
        print(f"Período CERRADO más reciente: {period_closed_start} a {period_closed_end}")
    else:
        print("No se encontró período 'cerrado'. 'interfaz2_apibuk.csv' quedará vacío.")

    period_open_start, period_open_end = fetch_latest_open_period(session)
    if period_open_start and period_open_end:
        print(f"Período ABIERTO más reciente: {period_open_start} a {period_open_end}")
    else:
        print("No se encontró período 'abierto'. 'interfaz1_apibuk.csv' podría quedar vacío según filtros.")

    # --- contadores / progreso ---
    page = 1
    all_rows = []       # interfaz1 (activos válidos dentro de período ABIERTO)
    filtered_rows = []  # interfaz2 (finiquitados dentro de período CERRADO)
    total_added_activos = 0
    total_added_fini = 0
    processed_global = 0
    expected_global = None  # lo intento leer de payload['pagination']['count']

    while True:
        url = f"{BASE}/employees?page_size={PAGE_SIZE}&page={page}"
        try:
            r = session.get(url, timeout=TIMEOUT)
        except requests.RequestException as e:
            print(f"\n Error de red en página {page}: {e}")
            break

        if r.status_code != 200:
            print(f"\n Error {r.status_code} en página {page}")
            break

        payload = r.json()
        if expected_global is None:
            try:
                expected_global = int(((payload or {}).get("pagination") or {}).get("count") or 0) or None
            except Exception:
                expected_global = None

        empleados = payload.get("data", payload)
        if not empleados:
            break

        rows_activos = []
        added_activos_page = 0
        added_fini_page = 0
        page_total = len(empleados)

        print(f"\nProcesando página {page}...")

        for i, emp in enumerate(empleados, start=1):
            job = emp.get("current_job") or {}
            start_date = to_yyyymmdd(job.get("start_date"))
            end_date   = to_yyyymmdd(job.get("end_date"))

            # progreso en línea (global si hay total, si no por página)
            processed_global += 1
            if expected_global:
                msg = f"\r Progreso global: {processed_global}/{expected_global} | página {page}: {i}/{page_total}"
            else:
                msg = f"\r Progreso página {page}: {i}/{page_total}"
            print(msg, end="", flush=True)

            # Detecta estado
            employee_status = analyze_employee_status(emp)

            # (A) FINIQUITADOS -> interfaz2 si end_date ∈ [periodo cerrado]
            if employee_status["destination"] == "filtered":
                if period_closed_start and period_closed_end and end_date:
                    if period_closed_start <= end_date <= period_closed_end:
                        filtered_rows.append(build_employee_row(emp))
                        added_fini_page += 1
                continue  # no evaluar reglas de activos

            # (B) ACTIVOS -> interfaz1 si end_date es NULL y estaban activos en período ABIERTO
            if period_open_start and period_open_end:
                is_active_now = (end_date in (None, "", "00000000"))
                started_in_or_before_period = (start_date and start_date <= period_open_end)
                if not (is_active_now and started_in_or_before_period):
                    continue

            # Filtros ≥ 20220801 (los que ya tenías)
            contract_analysis = analyze_employee_contracts(emp)
            company_entry_date = contract_analysis["oldest_start_date"]
            service_date = contract_analysis["oldest_start_date"]
            date_contract_status = start_date
            date_sps_elig = start_date

            if not is_valid_date(company_entry_date, "20220801"):
                continue
            if not is_valid_date(service_date, "20220801"):
                continue
            if not is_valid_date(date_contract_status, "20220801"):
                continue
            if not is_valid_date(date_sps_elig, "20220801"):
                continue

            rows_activos.append(build_employee_row(emp))
            added_activos_page += 1

        # fin de página
        print("\n  ✔ Página procesada: "
              f"activos agregados = {added_activos_page}, "
              f"finiquitados agregados = {added_fini_page}")

        all_rows.extend(rows_activos)
        total_added_activos += added_activos_page
        total_added_fini += added_fini_page

        page += 1

    # --- CSV interfaz1 (ACTIVOS) ---
    if all_rows:
        df = pd.DataFrame(all_rows, columns=COLS)
        # Extraer SOLO la parte numérica del RUT (sin dígito verificador) para ordenar
        def extract_rut_number(rut_str):
            rut_str = str(rut_str).strip()
            if not rut_str:
                return 0
            # Quitar el último carácter (dígito verificador) y convertir a número
            # "65589729" → "6558972" → 6558972
            # "10213754k" → "10213754" → 10213754
            rut_sin_verificador = rut_str[:-1]
            try:
                return int(rut_sin_verificador)
            except:
                return 0
        
        df["_pn_num"] = df["Personnel Number"].apply(extract_rut_number)
        df = df.sort_values(
            by=["_pn_num"],
            na_position="last",
            kind="stable"  # stable preserva el orden original en caso de empate
        ).reset_index(drop=True)
        df = df.drop(columns=["_pn_num"])
        df.to_csv(OUT_CSV_SEMI, index=False, sep=";", encoding="utf-8")
        print(f"\n Guardado {OUT_CSV_SEMI} con {len(df)} registros.")
    else:
        print("\n No se agregaron registros a interfaz1_apibuk.csv.")

    # --- CSV interfaz2 (FINIQUITADOS en período CERRADO) ---
    if filtered_rows:
        df_filtered = pd.DataFrame(filtered_rows, columns=COLS)
        # Extraer SOLO la parte numérica del RUT (sin dígito verificador) para ordenar
        def extract_rut_number(rut_str):
            rut_str = str(rut_str).strip()
            if not rut_str:
                return 0
            # Quitar el último carácter (dígito verificador) y convertir a número
            # "65589729" → "6558972" → 6558972
            # "10213754k" → "10213754" → 10213754
            rut_sin_verificador = rut_str[:-1]
            try:
                return int(rut_sin_verificador)
            except:
                return 0
        
        df_filtered["_pn_num"] = df_filtered["Personnel Number"].apply(extract_rut_number)
        df_filtered = df_filtered.sort_values(
            by=["_pn_num"],
            na_position="last",
            kind="stable"  # stable preserva el orden original en caso de empate
        ).reset_index(drop=True)
        df_filtered = df_filtered.drop(columns=["_pn_num"])
        df_filtered.to_csv(OUT_CSV_FILTERED, index=False, sep=";", encoding="utf-8")
        print(f"✅ Guardado {OUT_CSV_FILTERED} con {len(df_filtered)} registros.")
    else:
        print("No se agregaron registros a interfaz2_apibuk.csv.")

    # --- resumen final ---
    print("\n===== RESUMEN =====")
    print(f"Activos añadidos a interfaz1: {total_added_activos}")
    print(f"Finiquitados añadidos a interfaz2: {total_added_fini}")
    if expected_global:
        print(f"Total empleados vistos: {processed_global}")
    else:
        print(f"Total empleados vistos: {processed_global}")

    if os.name == "nt" and not sys.stdout.isatty():
        input("Presiona ENTER para cerrar...")

if __name__ == "__main__":
    main()
