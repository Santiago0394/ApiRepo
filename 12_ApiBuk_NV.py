# buk_export_simple.py
import os
import sys
import re
import unicodedata
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
BASE = "https://deloitte-innomotics-test.buk.cl/api/v1/chile"
PAGE_SIZE = 1000
TIMEOUT = 20
OUT_CSV_SEMI = os.path.join(BASE_DIR, "reporte_generado_apibuk.csv")

# -------- Helpers --------
PREFIXES = ["de la","de los","de las","del","de","van","von","da","di","do"]
SUFFIXES = {"jr","sr","iii","iv","v"}

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

def map_gender(val):
    if not val: return ""
    v = str(val).strip().lower()
    if v in ("m","male","masculino","hombre"): return 1
    if v in ("f","female","femenino","mujer"): return 2
    return ""

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

# -------- Columnas de salida --------
COLS = [
    "Personnel Number","GID","Surname","Name",
    "Middle Initial","Aristocratic Title","Surname Prefix","Surname Suffix",
    "Preferred Name / Nickname","Surname 2","Title","Gender","Date of Birth",
    "Nationality 1","Nationality 2","Nationality 3","Highest Level of Education",
    "Contract Type","Contract Status","Contractual Weekly Working",
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
    "Finance Company Code","Currency – Payroll","LTI_Eligibility","Date LTI_Eligibility","Bank Country/Region","Bank Code",
    "Bank Control Key","Account Number","International Bank Account Number","Payroll Area","Termination Date",
    "Last Date Worked","Position","Legal Entity",
    "Employee Group","Employee Category","Time Management Status","Employee Subgroup","Pay Scale Type","Pay Scale Area",
    "Pay Scale Group","Contract Type","Standard Weekly Hours","Country of Birth","Salutation","Preferred Name","Line Manager",
    "SuccessFactors ID",
]

def main():
    # --- pedir token con prioridad env var ---
    auth = os.getenv("BUK_AUTH_TOKEN")
    if not auth:
        print("⚠ No se encontró la variable de entorno BUK_AUTH_TOKEN.")
        auth = input("🔑 Ingresa tu token BUK (se mostrará): ").strip()
        while not auth:
            auth = input("El token no puede estar vacío. Inténtalo de nuevo: ").strip()

    # --- sesión http ---
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.4, status_forcelist=[429,502,503,504])
    session.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=10))
    session.headers.update({
        "auth_token": auth,
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })

    page = 1
    all_rows = []

    print(f"📄 Generando: {OUT_CSV_SEMI}")
    while True:
        url = f"{BASE}/employees?page_size={PAGE_SIZE}&page={page}"
        try:
            r = session.get(url, timeout=TIMEOUT)
        except requests.RequestException as e:
            print(f"❌ Error de red en página {page}: {e}")
            break

        if r.status_code != 200:
            print(f"❌ Error {r.status_code} en página {page}")
            break

        payload = r.json()
        empleados = payload.get("data", payload)
        if not empleados:
            break

        rows = []
        for emp in empleados:
            ca = emp.get("custom_attributes") or {}
            job = emp.get("current_job") or {}
            job_ca = job.get("custom_attributes") or {}

            # Identificación / nombres
            dni = (emp.get("dni") or emp.get("document_number","")).replace(".","").replace("-","")
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
            contract_status = ( get_from_attrs(emp, ["Contract Status","Estado de contrato"], prefer_job=True)
                                or find_any(emp, ["Contract Status","Estado de contrato"]) )
            contractual_weekly = ( get_from_attrs(emp, ["Contractual Weekly Working","Weekly Hours","Standard Work Week"], prefer_job=True)
                                   or str(job.get("weekly_hours") or "").strip() )
            try:
                if contractual_weekly not in ("", None):
                    contractual_weekly = f"{float(str(contractual_weekly).replace(',','.')):.2f}"
            except:
                pass
            standard_work_week = contractual_weekly

            # Fechas laborales
            company_entry_date = to_yyyymmdd(job.get("start_date"))
            service_date = company_entry_date
            entry_reason = ( job.get("entry_reason")
                             or get_from_attrs(emp, ["Entry Reason","Razón de entrada"], prefer_job=True) or "" )
            company_exit_date = to_yyyymmdd(job.get("end_date")) or "99991231"

            # Org / ubicación / impuestos
            exit_reason = get_from_attrs(emp, ["Exit Reason"], prefer_job=True)
            workforce_type = normalize_workforce_type(emp)
            mgmt_group = get_from_attrs(emp, ["Management Group"], prefer_job=True)
            date_mgmt_group = get_from_attrs(emp, ["Date Management Group"], prefer_job=True, date=True)
            are = get_from_attrs(emp, ["ARE"], prefer_job=True)
            loc_short = get_from_attrs(emp, ["Location / Office (short name)"], prefer_job=True) or emp.get("office_short_name","")
            in_company_mgr = get_from_attrs(emp, ["In-company Manager","Line Manager"], prefer_job=True)
            org_code = get_from_attrs(emp, ["OrgCode"], prefer_job=True)
            tech_pmp_flag = get_from_attrs(emp, ["Technical PMP Flag"], prefer_job=True)
            gpm_status = get_from_attrs(emp, ["GPM Status"], prefer_job=True)
            place_action = get_from_attrs(emp, ["Country/Region - Place of Action"], prefer_job=True) or emp.get("country_code","")
            tax_country = get_from_attrs(emp, ["Tax Country/Region"], prefer_job=True)
            tax_state = get_from_attrs(emp, ["Tax Country/Region State"], prefer_job=True)
            date_loc_change = get_from_attrs(emp, ["Date Location Change"], prefer_job=True, date=True)

            # Direcciones
            addr1 = get_from_attrs(emp, ["Address 1"]) or emp.get("address", "") or emp.get("address_line1","")
            addr2 = get_from_attrs(emp, ["Address 2"]) or emp.get("address_line2","")
            addr3 = get_from_attrs(emp, ["Address 3"]) or emp.get("address_line3","")
            city = emp.get("district", "")
            state = (get_from_attrs(emp, ["State"]) or emp.get("state",""))
            country_home = get_from_attrs(emp, ["Country/Region - Home Address"], prefer_job=True)
            postal_code = get_from_attrs(emp, ["Postal Code","Código Postal"], prefer_job=False)

            # Compensaciones / estructura
            incentive_payment_type = get_from_attrs(emp, ["Incentive Payment Type"], prefer_job=True)
            cost_center = ( get_from_attrs(emp, ["Cost Center"], prefer_job=True)
                            or emp.get("current_job",{}).get("cost_center","") )
            functional_area = get_from_attrs(emp, ["Functional Area"], prefer_job=True)
            country_region = get_from_attrs(emp, ["Country/Region"], prefer_job=True) or emp.get("country","")
            hr_service_area = get_from_attrs(emp, ["HR Service Area"], prefer_job=True)
            local_pay_level = get_from_attrs(emp, ["Local Pay Level"], prefer_job=True, date=True)

            # Más job/custom
            contract_date = (get_from_attrs(emp, ["Contract Date","Date Contract"], prefer_job=True, date=True)
                             or to_yyyymmdd(job.get("start_date")))

            # ---- Base Pay: 2 decimales ----
            base_pay_raw = get_from_attrs(emp, ["Base Pay","Salario Base","Salary Base"], prefer_job=True)
            base_pay = ""
            if base_pay_raw not in ("", None):
                s = str(base_pay_raw).strip().replace(",", "")
                s = re.sub(r"[^\d.\-]", "", s)
                try:
                    base_pay = f"{Decimal(s):.2f}"
                except Exception:
                    base_pay = s

            # ---- Target Incentive Amount: conservar ceros ----
            tia_raw = get_from_attrs(emp, ["Target Incentive Amount"], prefer_job=True)
            if tia_raw in ("", None):
                target_incentive_amount = ""
            else:
                s = str(tia_raw).strip()
                s_clean = s.replace(",", ".")
                s_num = re.sub(r"[^0-9.\-]", "", s_clean)
                try:
                    val = Decimal(s_num) if s_num not in ("", ".", "-") else None
                except Exception:
                    val = None
                if val is not None and val == 0:
                    target_incentive_amount = "0"
                elif s.strip(" 0.,") == "":
                    target_incentive_amount = "0"
                else:
                    target_incentive_amount = s

            currency = get_from_attrs(emp, ["Currency"], prefer_job=True) or emp.get("current_job",{}).get("currency_code","")

            # BF · Local Job Title desde role.name, solo antes de "/"
            role_name = ((job.get("role") or {}).get("name") or "").strip()
            if role_name:
                local_job_title = role_name.split("/", 1)[0].strip()
            else:
                local_job_title = get_from_attrs(emp, ["Local Job Title"], prefer_job=True)

            date_local_job_title = get_from_attrs(emp, ["Date Local Job Title"], prefer_job=True, date=True)
            depth_structure = get_from_attrs(emp, ["Depth Structure"], prefer_job=True)

            # --------- BI usa "Date Contract Status" ----------
            date_contract_status = (
                get_from_attrs(emp, ["Date Contract Status"], prefer_job=True, date=True)
                or get_from_attrs(emp, ["Contract Status Date"], prefer_job=True, date=True)
            )
            date_gpm_status = date_contract_status or get_from_attrs(emp, ["Date GPM Status"], prefer_job=True, date=True)
            # ---------------------------------------------------

            gpm_exit_status = get_from_attrs(emp, ["GPM Exit Status"], prefer_job=True)
            date_base_pay = get_from_attrs(emp, ["Date Base Pay"], prefer_job=True, date=True)

            # >>>>>>> CAMBIO: BM en formato aaaammdd <<<<<<<
            date_target_incentive_amount = get_from_attrs(
                emp, ["Date Target Incentive Amount"], prefer_job=True, date=True
            )
            # >>>>>>> fin cambio <<<<<<<

            global_cost_center = get_from_attrs(emp, ["Global Cost Center"], prefer_job=True)

            # Nombre internacional / otros
            name_international = first_name_first
            surname_international = surname
            preferred_surname = get_from_attrs(emp, ["Preferred Surname","Apellido preferido"], prefer_job=False)

            eligibility_comp = get_from_attrs(emp, ["Eligibility for Compensation Planning"], prefer_job=True)
            grip_position = get_from_attrs(emp, ["GRIP Position"], prefer_job=True)
            sps_elig = get_from_attrs(emp, ["SPS_Eligibility"], prefer_job=True)
            date_sps_elig = get_from_attrs(emp, ["Date SPS_Eligibility"], prefer_job=True, date=True)
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
                base_salary = get_from_attrs(emp, ["Base Salary","Salary Base","Sueldo Base","Base Pay"], prefer_job=True)
            else:
                if isinstance(base_wage_val, str):
                    base_salary = base_wage_val.strip()
                else:
                    base_salary = str(base_wage_val)
            date_base_salary = get_from_attrs(emp, ["Date Base Salary","Base Salary Date"], prefer_job=True, date=True)

            fixed_allowances = get_from_attrs(emp, ["Fixed Allowances"], prefer_job=True)
            date_fixed_allowance = get_from_attrs(emp, ["Date Fixed Allowance"], prefer_job=True, date=True)
            job_region = get_from_attrs(emp, ["JobRegion","Country/Region Sub Entity"], prefer_job=True)

            finance_company_code = get_from_attrs(emp, ["Finance Company Code"], prefer_job=True)
            currency_payroll = get_from_attrs(emp, ["Currency – Payroll","Currency - Payroll","Currency–Payroll"], prefer_job=True)
            lti_elig = get_from_attrs(emp, ["LTI_Eligibility"], prefer_job=True)
            date_lti_elig = get_from_attrs(emp, ["Date LTI_Eligibility"], prefer_job=True, date=True)
            bank_country = get_from_attrs(emp, ["Bank Country/Region Code","Bank Country/Region"], prefer_job=True)
            bank_code = get_from_attrs(emp, ["Bank Code"], prefer_job=True)
            bank_control_key = get_from_attrs(emp, ["Bank Control Key"], prefer_job=True)
            account_number = get_from_attrs(emp, ["Account Number"], prefer_job=False) or emp.get("account_number","")
            iban = get_from_attrs(emp, ["International Bank Account Number","IBAN"], prefer_job=True)
            payroll_area = get_from_attrs(emp, ["Payroll Area"], prefer_job=True)
            termination_date = to_yyyymmdd(emp.get("active_until"))

            last_date_worked = get_from_attrs(emp, ["Last Date Worked"], prefer_job=True, date=True)
            position = get_from_attrs(emp, ["Position"], prefer_job=True)
            legal_entity = get_from_attrs(emp, ["Legal Entity"], prefer_job=True)

            # ← NUEVAS
            employee_group = ( find_any(emp, ["Employee Group","Grupo de empleados","EE Group","Group"])
                               or get_from_attrs(emp, ["Employee Group"], prefer_job=True) )
            employee_category = ( find_any(emp, ["Employee Category","Categoría de empleados","Emp Category","Category"])
                                  or get_from_attrs(emp, ["Employee Category"], prefer_job=True) )
            time_mgmt_status = ( find_any(emp, ["Time Management Status","Time Mgmt Status","Estado de gestión de tiempo"])
                                 or get_from_attrs(emp, ["Time Management Status"], prefer_job=True) )
            employee_subgroup = str(job_ca.get("Employee Subgroup") or ca.get("Employee Subgroup") or "").strip()
            pay_scale_type    = str(job_ca.get("Pay Scale Type")     or ca.get("Pay Scale Type")     or "").strip()
            pay_scale_area    = str(job_ca.get("Pay Scale Area")     or ca.get("Pay Scale Area")     or "").strip()
            pay_scale_group = get_from_attrs(emp, ["Pay Scale Group", "Grupo de escala salarial"], prefer_job=True) or ""

            standard_weekly_hours = (
                get_from_attrs(emp, ["Standard Weekly Hours", "Standard Work Week", "Contractual Weekly Working"], prefer_job=True)
                or str(job.get("weekly_hours") or "")
            )
            try:
                if standard_weekly_hours:
                    standard_weekly_hours = f"{float(str(standard_weekly_hours).replace(',','.')):.2f}"
            except Exception:
                pass

            # --- Nuevos campos ---
            country_of_birth = emp.get("country_code") or ""
            salutation = get_from_attrs(emp, ["Salutation", "Tratamiento", "Título de saludo"], prefer_job=False) or ""
            line_manager = get_from_attrs(emp, ["Line Manager", "Manager Name", "Jefe directo", "Supervisor"], prefer_job=True) or ""
            successfactors_id = emp.get("person_id") or emp.get("SuccessFactors ID") or ""

            # >>> Fila
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
                "Contractual Weekly Working": contractual_weekly,
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
                "Date Workforce Type": "",
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
                "Date Target Incentive Amount": date_target_incentive_amount,  # ← ahora en aaaammdd
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
                "Currency – Payroll": currency_payroll,
                "LTI_Eligibility": lti_elig,
                "Date LTI_Eligibility": date_lti_elig,
                "Bank Country/Region": bank_country,
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
                "Contract Type": contract_type_code,
                "Standard Weekly Hours": standard_weekly_hours,
                "Country of Birth": country_of_birth,
                "Salutation": salutation,
                "Preferred Name": first_name_first,
                "Line Manager": line_manager,
                "SuccessFactors ID": successfactors_id,
            })
            rows.append(row)

        all_rows.extend(rows)
        print(f"✅ Página {page} procesada ({len(rows)} filas)")
        page += 1

    if all_rows:
        df = pd.DataFrame(all_rows, columns=COLS)
        pn_clean = df["Personnel Number"].astype(str).str.replace(r"\D", "", regex=True)
        df["_pn_num"] = pd.to_numeric(pn_clean, errors="coerce")
        df = df.sort_values(
            by=["_pn_num", "Personnel Number"],
            na_position="last",
            kind="mergesort"
        ).drop(columns=["_pn_num"])
        df.to_csv(OUT_CSV_SEMI, index=False, sep=";", encoding="utf-8")

    print(f"🎉 Listo. Archivo generado: {OUT_CSV_SEMI}")

    if os.name == "nt" and not sys.stdout.isatty():
        input("Presiona ENTER para cerrar...")

if __name__ == "__main__":
    main()
