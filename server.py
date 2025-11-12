"""
Accelo → CSV → PostgreSQL (all endpoints)
- Progress bar for EACH endpoint
- Robust retries/timeouts
- Convert seconds → hours (billable/nonbillable/time_allocation)
- Update meta_refresh.last_sql_refreshed = MM.DD.YYYY
- Fast export (to_sql: method='multi', chunksize=10000)
"""
import base64
import logging
import os
import time
import json
from pathlib import Path
import requests
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from pandas.errors import EmptyDataError
from sqlalchemy import create_engine, text
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pandas.api.types import is_numeric_dtype
from tqdm import tqdm
from datetime import datetime
import sys

# ─────────────────────────────────────────────────────────────────────────────
# Settings / initialization
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv(find_dotenv())

CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TOKEN_URL     = os.getenv("BASE_URL")  # https://.../oauth2/v0/token
BASE          = os.getenv("BASE")      # https://.../api/v0/
DB_MASTER     = os.getenv("DB_MASTER") # SQLAlchemy DSN (recommended)

# If you want to run only a subset of entities (e.g., {"Activities"})
RUN_ONLY = set()

ENDPOINTS = {
    f"{BASE}staff": "Staff",
    f"{BASE}rates": "Rates",
    f"{BASE}jobs": "Jobs",
    f"{BASE}invoices": "Invoices",
    f"{BASE}companies": "Companies",
    f"{BASE}issues": "Issues",
    f"{BASE}affiliations": "Affiliations",
    f"{BASE}milestones": "Milestones",
    f"{BASE}contracts": "Contracts",
    f"{BASE}companies/profiles/values": "Company Profiles",
    f"{BASE}issues/profiles/values": "Issues Profiles",
    f"{BASE}jobs/profiles/values": "Jobs Profiles",
    f"{BASE}expenses": "Expenses",
    f"{BASE}contracts/profiles/values": "Contracts Profiles",
    f"{BASE}contracts/periods": "Contract Periods",
    f"{BASE}activities": "Activities",
    f"{BASE}tasks": "Tasks",
    f"{BASE}groups": "Groups",
    f"{BASE}staff/memberships": "Memberships",
    f"{BASE}object_budgets/services": "Services",
    f"{BASE}object_budgets/materials": "Materials",
    f"{BASE}object_budgets/templates": "Templates",
    f"{BASE}invoices/line_items": "Line Items",
    f"{BASE}taxes": "Taxes",
    f"{BASE}contracts/types": "Contract Types",
}
if RUN_ONLY:
    ENDPOINTS = {k: v for k, v in ENDPOINTS.items() if v in RUN_ONLY}

DATA_DIR = r"/home/azureuser/accelocron"
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# HTTP session / retry
# ─────────────────────────────────────────────────────────────────────────────
def make_session(per_request_delay=0.05):
    s = requests.Session()
    retry = Retry(
        total=10,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=100)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": "AcceloFetcher/1.4",
        "Accept-Encoding": "gzip, deflate",
    })
    if per_request_delay > 0:
        original = s.request
        def delayed(method, url, **kw):
            time.sleep(per_request_delay)
            return original(method, url, **kw)
        s.request = delayed
    return s

def honor_retry_after(resp):
    ra = resp.headers.get("Retry-After")
    try:
        wait = int(ra) if ra else 5
    except Exception:
        wait = 5
    time.sleep(min(max(wait, 1), 120))

# ─────────────────────────────────────────────────────────────────────────────
# Atomic CSV write
# ─────────────────────────────────────────────────────────────────────────────
def atomic_write_csv(df: pd.DataFrame, final_path: str):
    p = Path(final_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".part")
    df.to_csv(tmp, index=False)
    tmp.replace(p)

# ─────────────────────────────────────────────────────────────────────────────
# Page checkpoints (restart without re-downloading finished pages)
# ─────────────────────────────────────────────────────────────────────────────
class PageCheckpoints:
    """Structure: { "<endpoint_url>": {"done_pages": [0,1,2,...]} }"""
    def __init__(self, path):
        self.path = path
        self._data = {}
        self._load()
    def _load(self):
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                self._data = json.load(f)
        except Exception:
            self._data = {}
    def _save(self):
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path + ".part"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)
    def is_done(self, endpoint, page):
        return page in set(self._data.get(endpoint, {}).get("done_pages", []))
    def mark_done(self, endpoint, page):
        e = self._data.setdefault(endpoint, {})
        pages = set(e.get("done_pages", []))
        pages.add(page)
        e["done_pages"] = sorted(pages)
        self._save()

CKPT = PageCheckpoints(os.path.join(DATA_DIR, "_accelo_page_checkpoints.json"))

# ─────────────────────────────────────────────────────────────────────────────
# Converters (dates, seconds → hours)
# ─────────────────────────────────────────────────────────────────────────────
def _to_datetime_smart(series: pd.Series) -> pd.Series:
    """
    Smart datetime converter:
    - Detects preformatted YYYY-MM-DD
    - Supports epoch seconds / milliseconds
    - Supports ISO-like strings
    Returns 'YYYY-MM-DD' string column.
    """
    s = series.copy()
    sample = s.dropna().astype(str).head(20)
    if len(sample) and all(len(x) >= 10 and x[4] == '-' and x[7] == '-' for x in sample):
        return s.astype(str).str[:10]
    try:
        if is_numeric_dtype(s):
            s_nonnull = s.dropna()
            if not s_nonnull.empty:
                m = s_nonnull.astype("int64").abs().median()
                unit = 'ms' if m > 10**12 else 's'
                s = pd.to_datetime(s, unit=unit, errors='coerce', utc=False)
            else:
                s = pd.to_datetime(s, unit='s', errors='coerce', utc=False)
        else:
            s = pd.to_datetime(s, errors='coerce', utc=False)
    except Exception:
        s = pd.to_datetime(s, errors='coerce', utc=False)
    try:
        s = s.dt.tz_localize(None)
    except Exception:
        pass
    return s.dt.strftime('%Y-%m-%d')

def convert_columns_to_datetime(df: pd.DataFrame):
    """Convert all *date* columns to 'YYYY-MM-DD' using _to_datetime_smart."""
    for col in df.columns:
        if 'date' in col.lower():
            try:
                df[col] = _to_datetime_smart(df[col])
            except Exception as e:
                print(f"[DATE] {col}: {e}")

def convert_columns_to_hours(df: pd.DataFrame):
    """
    Seconds → hours for billable/nonbillable/time_allocation (heuristic).
    Used only for the Activities table to avoid double conversion.
    """
    candidates = [c for c in df.columns if c.lower() in ('billable', 'nonbillable', 'time_allocation')]
    for col in candidates:
        try:
            s = pd.to_numeric(df[col], errors='coerce')
            if s.notna().sum() == 0:
                continue
            if s.median() >= 3600:  # looks like seconds
                df[col] = s / 3600.0
        except Exception as e:
            print(f"[HOURS] {col}: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────────────────────────────────────
def get_access_token():
    creds = f"{CLIENT_ID}:{CLIENT_SECRET}"
    auth = base64.b64encode(creds.encode()).decode()
    headers = {"Authorization": f"Basic {auth}"}
    params  = {"grant_type": "client_credentials"}
    sess = make_session(per_request_delay=0.05)
    r = sess.post(TOKEN_URL, data=params, headers=headers, timeout=60)
    if r.status_code != 200:
        print(f"Access token request failed: HTTP {r.status_code}")
        sys.exit(1)
    token = r.json().get("access_token")
    if not token:
        print("Access token not obtained.")
        sys.exit(1)
    return token, sess

ACCESS_TOKEN, SESSION = get_access_token()

# ─────────────────────────────────────────────────────────────────────────────
# Page count (count endpoint → fallback: binary search)
# ─────────────────────────────────────────────────────────────────────────────
def binary_page_search(endpoint_url: str) -> int:
    """Find last non-empty page index using binary search."""
    min_page, max_page = 0, 2**20
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    while min_page < max_page:
        mid = (min_page + max_page) // 2
        r = SESSION.get(f"{endpoint_url}?_page={mid}&_limit=100&_fields=_ALL", headers=headers, timeout=60)
        if r.status_code == 429:
            honor_retry_after(r)
            continue
        if r.status_code != 200:
            min_page = mid + 1
            continue
        try:
            payload = r.json()
            empty = not payload.get("response")
        except Exception:
            empty = True
        if empty:
            max_page = mid
        else:
            min_page = mid + 1
    return min_page

def get_total_pages(endpoint_url: str, folder_name: str) -> int:
    """Get total pages from /count endpoint or via binary search."""
    count_url = f"{endpoint_url}/count"
    r = SESSION.get(count_url, headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}, timeout=60)
    if r.status_code == 429:
        honor_retry_after(r)
        r = SESSION.get(count_url, headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}, timeout=60)
    if r.status_code == 200:
        try:
            cnt = int(r.json().get("response", {}).get("count", 0))
            pages = (cnt + 99) // 100
            print(f"Total pages for {folder_name} data: {pages}")
            return pages
        except Exception:
            pass
    print(f"Count API for {folder_name} not available. Using binary search…")
    pages = binary_page_search(endpoint_url)
    print(f"Total pages for {folder_name} data: {pages}")
    return pages

# ─────────────────────────────────────────────────────────────────────────────
# FETCH with progress bar per endpoint
# ─────────────────────────────────────────────────────────────────────────────
def fetch_all():
    for endpoint_url, folder in ENDPOINTS.items():
        save_dir = os.path.join(DATA_DIR, folder)
        os.makedirs(save_dir, exist_ok=True)

        total_pages = get_total_pages(endpoint_url, folder)

        # Requested fields
        if endpoint_url == f"{BASE}activities":
            fields = (
                "subject,thread_id,contract_period_id,parent,nonbillable,against_id,"
                "rate_charged,date_started,date_logged,rate,visibility,invoice_id,class,"
                "time_allocation,standing,owner,activity_class,against,date_modified,"
                "medium,id,activity_priority,date_created,parent_id,staff,owner_id,"
                "owner_type,thread,billable,priority"
            )
        else:
            fields = "_ALL"

        current = 0
        with tqdm(total=total_pages, desc=f"Downloading {folder}", unit="page") as pbar:
            last_tick = time.time()
            while current < total_pages:
                if CKPT.is_done(endpoint_url, current):
                    pbar.update(1)
                    current += 1
                    continue

                url = f"{endpoint_url}?_page={current}&_limit=100&_fields={fields}"
                try:
                    resp = SESSION.get(
                        url,
                        headers={"Authorization": f"Bearer {ACCESS_TOKEN}"},
                        timeout=(10, 180),
                    )
                except Exception as e:
                    print(f"[WARN] {folder} page {current}: {e}; retry in 5s")
                    time.sleep(5)
                    continue

                if resp.status_code == 429:
                    honor_retry_after(resp)
                    continue
                if resp.status_code != 200:
                    print(f"[WARN] {folder} page {current}: HTTP {resp.status_code}; break")
                    break

                rows = resp.json().get("response", [])
                if not rows:
                    break

                df = pd.DataFrame(rows)
                atomic_write_csv(df, os.path.join(save_dir, f"{folder.lower()}_data_page_{current+1}.csv"))

                CKPT.mark_done(endpoint_url, current)
                current += 1
                pbar.update(1)

                if time.time() - last_tick > 30:
                    pbar.set_postfix_str(f"last=page {current}")
                    last_tick = time.time()

# ─────────────────────────────────────────────────────────────────────────────
# MERGE per endpoint into single merged.csv (with conversions)
# ─────────────────────────────────────────────────────────────────────────────
def merge_all():
    for endpoint_url, folder in ENDPOINTS.items():
        print(f"Started processing {folder}")
        merged = pd.DataFrame()
        endpoint_dir = os.path.join(DATA_DIR, folder)
        if not os.path.exists(endpoint_dir):
            print(f"Directory not found for {folder}")
            continue

        files = [f for f in os.listdir(endpoint_dir) if f.endswith(".csv")]
        files.sort(key=lambda x: (len(x), x))  # ensure 2,10,100 order is natural

        for fname in tqdm(files, desc=f"Merging {folder}", unit="file"):
            fpath = os.path.join(endpoint_dir, fname)
            try:
                df = pd.read_csv(fpath, sep=",", low_memory=False)
                convert_columns_to_datetime(df)
                if folder == "Activities":
                    convert_columns_to_hours(df)
                merged = pd.concat([merged, df], ignore_index=True)
            except EmptyDataError:
                pass

        out = os.path.join(DATA_DIR, f"{folder}_merged.csv")
        merged.to_csv(out, index=False)
        print(f"Merged data for {folder} into {out}")

# ─────────────────────────────────────────────────────────────────────────────
# Company Profiles transformation (pivot)
# ─────────────────────────────────────────────────────────────────────────────
def transform_company_profiles():
    merged_file = os.path.join(DATA_DIR, "Company Profiles_merged.csv")
    if not os.path.exists(merged_file):
        return
    try:
        df = pd.read_csv(merged_file, low_memory=False)
        required = {"link_id", "field_name", "value"}
        missing = required - set(df.columns)
        if missing:
            print(f"Error: Missing required columns for pivot: {missing}")
            return
        pivot = pd.pivot_table(
            df[["link_id", "field_name", "value"]],
            index="link_id", columns="field_name", values="value", aggfunc="first"
        ).reset_index()
        pivot.to_csv(merged_file, index=False)
        print(f"Transformed Company Profiles written to: {merged_file}")
    except Exception as e:
        print(f"Error transforming Company Profiles: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# DB DSN helpers
# ─────────────────────────────────────────────────────────────────────────────
def sanitize_dsn(dsn: str) -> str:
    """
    Escape '#' in password and ensure sslmode=require is present.
    """
    if not dsn:
        return dsn
    out = dsn
    qpos = out.find('?')
    before_q = out if qpos == -1 else out[:qpos]
    if '#' in before_q:
        out = out.replace('#', '%23', 1)
    if 'sslmode=' not in out:
        sep = '&' if '?' in out else '?'
        out = f"{out}{sep}sslmode=require"
    return out

if not DB_MASTER:
    # Fallback DSN: adjust to your environment if needed
    # Password Calgary!# must be URL-encoded as Calgary!%23
    DB_MASTER = (
        "postgresql+psycopg2://dashboardadmin:Calgary!%23@"
        "kpidashdb.postgres.database.azure.com:5432/kpidashboard?sslmode=require"
    )
DB_MASTER = sanitize_dsn(DB_MASTER)

# ─────────────────────────────────────────────────────────────────────────────
# EXPORT (+ update meta_refresh.last_sql_refreshed)
# ─────────────────────────────────────────────────────────────────────────────
def export_one(folder_name: str):
    engine = create_engine(DB_MASTER)
    merged_path = os.path.join(DATA_DIR, f"{folder_name}_merged.csv")
    if not os.path.exists(merged_path):
        print(f"Merged file not found for {folder_name}")
        return

    df = pd.read_csv(merged_path, low_memory=False)
    convert_columns_to_datetime(df)
    if folder_name == "Activities":
        convert_columns_to_hours(df)

    table = f"{folder_name.lower()}_data".replace(" ", "_")
    df.to_sql(table, engine, if_exists='replace', index=False, chunksize=10000, method='multi')
    print(f"Exported {folder_name} → {table}")

def export_all_and_mark():
    for _, folder in ENDPOINTS.items():
        export_one(folder)

    # meta_refresh.last_sql_refreshed = MM.DD.YYYY
    engine = create_engine(DB_MASTER)
    stamp = datetime.now().strftime("%m.%d.%Y")
    with engine.begin() as conn:
        conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS meta_refresh (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        conn.execute(
            text("""
                INSERT INTO meta_refresh(key, value)
                VALUES (:k, :v)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """),
            {"k": "last_sql_refreshed", "v": stamp}
        )
    print(f"Updated meta_refresh.last_sql_refreshed = {stamp}")

# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        # 1) Fetch with per-endpoint progress bar
        fetch_all()

        # 2) Merge pages into merged.csv (with conversions)
        merge_all()

        # 3) Special transform for Company Profiles (pivot)
        if not RUN_ONLY or "Company Profiles" in RUN_ONLY:
            transform_company_profiles()

        # 4) Export all tables + update last refresh marker
        export_all_and_mark()

        print("✅ Done")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
