# developed by pkf
# server.py

import os
import sys
import time
import json
import logging

import requests
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def make_session(per_request_delay: float = 0.35) -> requests.Session:
    """Return requests.Session with retries and optional delay."""
    s = requests.Session()
    retry = Retry(
        total=10,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "AcceloRefresh/1.0"})

    if per_request_delay > 0:
        orig_req = s.request

        def delayed(method, url, **kw):
            time.sleep(per_request_delay)
            return orig_req(method, url, **kw)

        s.request = delayed

    return s


def honor_retry_after(resp: requests.Response) -> None:
    """Sleep according to Retry-After header when rate limited."""
    ra = resp.headers.get("Retry-After")
    try:
        wait = int(ra) if ra else 5
    except Exception:
        wait = 5
    wait = max(1, min(wait, 120))
    logger.warning("Rate limited, sleeping for %s seconds…", wait)
    time.sleep(wait)


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
def get_access_token(
    session: requests.Session,
    token_endpoint: str,
    client_id: str,
    client_secret: str,
) -> str:
    """Request OAuth2 client_credentials token."""
    import base64

    creds = f"{client_id}:{client_secret}"
    b64 = base64.b64encode(creds.encode()).decode()
    headers = {"Authorization": f"Basic {b64}"}
    data = {"grant_type": "client_credentials"}

    r = session.post(token_endpoint, headers=headers, data=data, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(
            f"Token request failed: {r.status_code} {r.text[:500]}"
        )

    token = r.json().get("access_token")
    if not token:
        raise RuntimeError("No access_token in token response")
    return token


# ---------------------------------------------------------------------------
# Pagination helpers
# ---------------------------------------------------------------------------
def binary_page_search(
    session: requests.Session,
    endpoint_url: str,
    token: str,
    limit: int = 100,
) -> int:
    """Find last page when /count is not available."""
    headers = {"Authorization": f"Bearer {token}"}
    low, high = 0, 2**20

    while low < high:
        mid = (low + high) // 2
        url = f"{endpoint_url}?_page={mid}&_limit={limit}&_fields=_ALL"
        r = session.get(url, headers=headers, timeout=60)

        if r.status_code == 429:
            honor_retry_after(r)
            continue

        if r.status_code != 200:
            low = mid + 1
            continue

        try:
            payload = r.json()
            empty = not payload.get("response")
        except Exception:
            empty = True

        if empty:
            high = mid
        else:
            low = mid + 1

    return low


def get_total_pages(
    session: requests.Session,
    endpoint_url: str,
    token: str,
    limit: int = 100,
) -> int:
    """Return number of pages using /count or binary search fallback."""
    headers = {"Authorization": f"Bearer {token}"}
    count_url = f"{endpoint_url}/count"

    r = session.get(count_url, headers=headers, timeout=60)
    if r.status_code == 429:
        honor_retry_after(r)
        r = session.get(count_url, headers=headers, timeout=60)

    if r.status_code == 200:
        try:
            j = r.json()
            count = int(j.get("response", {}).get("count", 0))
        except Exception:
            count = 0
    else:
        count = 0

    if count <= 0:
        pages = binary_page_search(session, endpoint_url, token, limit=limit)
        logger.info(
            "Count not available for %s, using binary search pages=%s",
            endpoint_url,
            pages,
        )
        return pages

    pages = (count + limit - 1) // limit
    logger.info("Endpoint %s: count=%s, pages=%s", endpoint_url, count, pages)
    return pages


# ---------------------------------------------------------------------------
# Converters
# ---------------------------------------------------------------------------
def convert_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all columns whose name contains 'date' (case-insensitive) to YYYY-MM-DD.

    Handles both unix seconds and already formatted date strings.
    This covers, in particular:
      - activities: date_logged
      - expenses:  date_incurred
      - invoices:  date_raised
    """
    for col in df.columns:
        if "date" not in col.lower():
            continue

        s = df[col]
        if s is None or s.isna().all():
            continue

        try:
            numeric = pd.to_numeric(s, errors="coerce")
            if numeric.notna().sum() > 0:
                dt = pd.to_datetime(numeric, unit="s", errors="coerce", utc=True)
                mask = numeric.isna() & s.notna()
                if mask.any():
                    dt_alt = pd.to_datetime(s[mask], errors="coerce", utc=True)
                    dt.loc[mask] = dt_alt
            else:
                dt = pd.to_datetime(s, errors="coerce", utc=True)

            df[col] = dt.dt.strftime("%Y-%m-%d")
            df.loc[dt.isna(), col] = None
        except Exception as e:
            logger.warning("Date convert failed for %s: %s", col, e)

    return df


def sanitize_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert columns that should be numeric (but may contain empty strings)
    to proper numeric types. Empty strings become NaN/None.

    This helps avoid Power BI errors like VT_BSTR → VT_R8 (e.g. 'ordering').
    """
    for col in df.columns:
        name = col.lower()

        # Skip date-like columns
        if "date" in name:
            continue

        series = df[col]

        # Skip if already numeric
        if pd.api.types.is_numeric_dtype(series):
            continue

        # Detect numeric-like columns based on a sample
        sample = series.dropna().astype(str).str.strip().head(50)
        if sample.empty:
            continue

        numeric_like = sample.str.match(r"^-?\d+(\.\d+)?$")
        if numeric_like.mean() >= 0.6:
            try:
                df[col] = pd.to_numeric(series.astype(str).str.strip(), errors="coerce")
            except Exception as e:
                logger.warning("Numeric sanitize failed for %s: %s", col, e)

    return df


def convert_activities_hours(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert duration-related columns from seconds to hours for activities.

    Any column whose name contains one of:
      - 'billable'
      - 'nonbillable'
      - 'seconds'
    is divided by 3600.0.
    """
    for col in df.columns:
        name = col.lower()
        if any(key in name for key in ["billable", "nonbillable", "seconds"]):
            try:
                df[col] = pd.to_numeric(df[col], errors="coerce") / 3600.0
            except Exception as e:
                logger.warning("Hour convert failed for %s: %s", col, e)
    return df


# ---------------------------------------------------------------------------
# Fetchers
# ---------------------------------------------------------------------------
def fetch_generic(
    session: requests.Session,
    endpoint_url: str,
    token: str,
    limit: int = 100,
    extra_response_key: str | None = None,
    fields: str = "_ALL",
) -> pd.DataFrame:
    """Fetch all pages for a generic endpoint into a DataFrame."""
    headers = {"Authorization": f"Bearer {token}"}
    pages = get_total_pages(session, endpoint_url, token, limit=limit)

    all_rows: list[dict] = []
    desc = endpoint_url.split("/")[-1] or endpoint_url

    with tqdm(total=pages, desc=f"Downloading {desc}") as bar:
        for page in range(0, pages):
            url = (
                f"{endpoint_url}?_page={page}"
                f"&_limit={limit}&_fields={fields}"
            )
            r = session.get(url, headers=headers, timeout=120)
            if r.status_code == 429:
                honor_retry_after(r)
                r = session.get(url, headers=headers, timeout=120)

            if r.status_code != 200:
                raise RuntimeError(
                    f"Endpoint {endpoint_url} page {page} failed: "
                    f"{r.status_code} {r.text[:500]}"
                )

            j = r.json()
            resp = j.get("response", [])
            if extra_response_key:
                resp = j.get("response", {}).get(extra_response_key, [])

            if not isinstance(resp, list):
                raise RuntimeError(
                    f"Unexpected response format on page {page}: {json.dumps(j)[:500]}"
                )

            all_rows.extend(resp)
            bar.update(1)

    if not all_rows:
        logger.warning("No rows downloaded for %s", endpoint_url)
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    logger.info(
        "Endpoint %s: downloaded %s rows", endpoint_url, len(df)
    )
    return df


def fetch_activities(
    session: requests.Session,
    activities_url: str,
    token: str,
    limit: int = 100,
) -> pd.DataFrame:
    """Fetch activities with explicit field list and apply conversions."""
    fields = (
        "subject,thread_id,contract_period_id,parent,nonbillable,against_id,"
        "rate_charged,date_started,date_logged,rate,visibility,invoice_id,"
        "class,time_allocation,standing,owner,activity_class,against,"
        "date_modified,medium,id,activity_priority,date_created,parent_id,"
        "staff,owner_id,owner_type,thread,billable,priority"
    )
    df = fetch_generic(
        session=session,
        endpoint_url=activities_url,
        token=token,
        limit=limit,
        extra_response_key=None,
        fields=fields,
    )
    df = convert_date_columns(df)
    df = convert_activities_hours(df)
    df = sanitize_numeric_columns(df)
    return df


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------
def export_df_to_postgres(
    df: pd.DataFrame,
    db_master_url: str,
    table_name: str,
) -> None:
    """Write DataFrame to PostgreSQL, replacing existing table."""
    if df is None or df.empty:
        logger.warning(
            "Table %s: DataFrame is empty, skipping export", table_name
        )
        return

    if not db_master_url:
        raise RuntimeError("DB_MASTER is not set")

    engine = create_engine(db_master_url)
    logger.info(
        "Writing %s rows to table %s", len(df), table_name
    )

    df.to_sql(
        table_name,
        engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )
    logger.info("Export to %s completed", table_name)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    load_dotenv(find_dotenv())

    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    token_endpoint = os.getenv("BASE_URL")
    base = os.getenv("BASE")
    db_master = os.getenv("DB_MASTER")

    if not all([client_id, client_secret, token_endpoint, base, db_master]):
        raise RuntimeError(
            "Please set CLIENT_ID, CLIENT_SECRET, BASE_URL, BASE and DB_MASTER in .env"
        )

    session = make_session(per_request_delay=0.35)
    token = get_access_token(session, token_endpoint, client_id, client_secret)

    # Endpoint configuration: (url, table_name, extra_response_key, use_activity_logic)
    endpoints = [
        (f"{base}invoices", "invoices_data", None, False),
        (f"{base}staff", "staff_data", None, False),
        (f"{base}jobs", "jobs_data", None, False),
        (f"{base}expenses", "expenses_data", "expenses", False),
        (f"{base}rates", "rates_data", None, False),
        (f"{base}activities", "activities_data", None, True),
        (f"{base}companies", "companies_data", None, False),
        (f"{base}affiliations", "affiliations_data", None, False),
        (f"{base}issues", "issues_data", None, False),
        (f"{base}milestones", "milestones_data", None, False),
        (f"{base}contracts", "contracts_data", None, False),
        (f"{base}contracts/profiles/values", "contracts_profiles_data", None, False),
        (f"{base}companies/profiles/values", "company_profiles_data", None, False),
        (f"{base}issues/profiles/values", "issues_profiles_data", None, False),
        (f"{base}jobs/profiles/values", "jobs_profiles_data", None, False),
        (f"{base}tasks", "tasks_data", None, False),
        (f"{base}staff/memberships", "memberships_data", None, False),
        (f"{base}groups", "groups_data", None, False),
        (f"{base}contracts/periods", "contract_periods_data", None, False),
        (f"{base}object_budgets/services", "services_data", None, False),
        (f"{base}object_budgets/materials", "materials_data", None, False),
        (f"{base}object_budgets/templates", "templates_data", None, False),
        (f"{base}invoices/line_items", "line_items_data", None, False),
        (f"{base}taxes", "taxes_data", None, False),
        (f"{base}contracts/types", "contract_types_data", None, False),
    ]

    # --- internal selection: run only specific tables ---
    # Leave RUN_ONLY empty to refresh ALL tables.
    RUN_ONLY = [
        # "invoices_data",
        # "tasks_data",
        # "invoices_data",
        # "jobs_data",
        # "companies_data",
    ]

    if RUN_ONLY:
        logger.info("RUN_ONLY enabled → refreshing only: %s", ", ".join(RUN_ONLY))
    else:
        logger.info("RUN_ONLY empty → refreshing ALL endpoints")

    for url, table, extra_key, is_activities in endpoints:

        if RUN_ONLY and table not in RUN_ONLY:
            continue

        try:
            logger.info("Processing endpoint %s → table %s", url, table)

            if is_activities:
                df = fetch_activities(session, url, token, limit=100)
            else:
                df = fetch_generic(
                    session=session,
                    endpoint_url=url,
                    token=token,
                    limit=100,
                    extra_response_key=extra_key,
                    fields="_ALL",
                )
                df = convert_date_columns(df)
                df = sanitize_numeric_columns(df)

            export_df_to_postgres(df, db_master, table)

        except Exception as e:
            logger.exception(
                "Failed processing endpoint %s → table %s: %s", url, table, e
            )

    logger.info("All endpoints processed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("Fatal error: %s", e)
        sys.exit(1)
