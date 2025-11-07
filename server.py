#developed by pkf
import base64
import logging
import os
import time
import json
from pathlib import Path
import sqlite3  # (unused; safe to keep/remove)
import requests
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from pandas.errors import EmptyDataError
from sqlalchemy import create_engine
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sys

try:
    # ──────────────────────────────────────────────────────────────────────────
    # Logging
    # ──────────────────────────────────────────────────────────────────────────
    logging.basicConfig()
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    load_dotenv(find_dotenv())

    # Credentials and endpoints
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    token_endpoint = os.getenv("BASE_URL")
    base = os.getenv("BASE")
    db_master = os.getenv("DB_MASTER")

    endpoints = {
        f"{base}staff": "Staff",
        f"{base}rates": "Rates",
        f"{base}jobs": "Jobs",
        f"{base}invoices": "Invoices",
        f"{base}companies": "Companies",
        f"{base}issues": "Issues",
        f"{base}affiliations": "Affiliations",
        f"{base}milestones": "Milestones",
        f"{base}contracts": "Contracts",
        f"{base}companies/profiles/values": "Company Profiles",
        f"{base}issues/profiles/values": "Issues Profiles",
        f"{base}jobs/profiles/values": "Jobs Profiles",
        f"{base}expenses": "Expenses",
        f"{base}contracts/profiles/values": "Contracts Profiles",
        f"{base}contracts/periods": "Contract Periods",
        f"{base}activities": "Activities",
        f"{base}tasks": "Tasks",
        f"{base}groups": "Groups",
        f"{base}staff/memberships": "Memberships",
        f"{base}object_budgets/services": "Services",
        f"{base}object_budgets/materials": "Materials",
        f"{base}object_budgets/templates": "Templates",
        f"{base}invoices/line_items": "Line Items",
        f"{base}taxes": "Taxes",
        f"{base}contracts/types": "Contract Types"
        # Add more endpoints as needed
    }

    # Encode client credentials to base64 (unchanged logic)
    client_credentials = f"{client_id}:{client_secret}"
    base64_credentials = base64.b64encode(client_credentials.encode()).decode()

    # Prepare headers for token request
    token_headers = {
        "Authorization": f"Basic {base64_credentials}"
    }

    # Token request parameters
    token_params = {
        "grant_type": "client_credentials"
    }

    # ──────────────────────────────────────────────────────────────────────────
    # Robust requests session + backoff
    # ──────────────────────────────────────────────────────────────────────────
    def make_session(per_request_delay=0.35):
        s = requests.Session()
        retry = Retry(
            total=10,
            backoff_factor=1.2,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"]),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=50)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({"User-Agent": "AcceloFetcher/1.0"})
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

    # ──────────────────────────────────────────────────────────────────────────
    # Atomic CSV write (prevents partial files)
    # ──────────────────────────────────────────────────────────────────────────
    def atomic_write_csv(df: pd.DataFrame, final_path: str):
        p = Path(final_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".part")
        df.to_csv(tmp, index=False)
        tmp.replace(p)

    # ──────────────────────────────────────────────────────────────────────────
    # Super-light JSON checkpoints per endpoint/page
    # ──────────────────────────────────────────────────────────────────────────
    class PageCheckpoints:
        """
        Stores which pages are DONE per endpoint so reruns skip finished work.
        File format:
        {
          "<endpoint_url>": {"done_pages": [0,1,2,...]}
        }
        """
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

    # Request access token (using robust session)
    session = make_session(per_request_delay=0.35)
    token_response = session.post(token_endpoint, data=token_params, headers=token_headers, timeout=60)

    if token_response.status_code != 200:
        print(f"Access token request failed with status code {token_response.status_code}")
        sys.exit(1)

    token_data = token_response.json()
    access_token = token_data.get("access_token")

    if not access_token:
        print("Access token not obtained.")
        sys.exit(1)

    # Define the directory where CSV files will be saved
    data_directory = r'/home/azureuser/accelocron'
    Path(data_directory).mkdir(parents=True, exist_ok=True)

    # Initialize checkpoints file
    ckpt = PageCheckpoints(os.path.join(data_directory, "_accelo_page_checkpoints.json"))

    # Helpers to massage columns
    def convert_columns_to_datetime(data):
        for column in data.columns:
            if 'date' in column.lower():
                try:
                    data[column] = pd.to_datetime(data[column], unit='s')
                    data[column] = data[column].dt.strftime('%Y-%m-%d')
                except Exception as e:
                    print(f"Error converting column {column} to date: {e}")

    def convert_columns_to_hours(data):
        for column in data.columns:
            if 'billable' in column.lower():
                try:
                    data[column] = data[column] / 3600.0  # Convert seconds to hours
                except Exception as e:
                    print(f"Error converting column {column} to hours: {e}")

    # Discover last page if count API is absent
    def binary_page_search(endpoint_url, access_token):
        min_page, max_page = 0, 2 ** 20
        headers = {"Authorization": f"Bearer {access_token}"}

        while min_page < max_page:
            mid_page = (min_page + max_page) // 2
            r = session.get(
                f"{endpoint_url}?_page={mid_page}&_limit=100&_fields=_ALL",
                headers=headers, timeout=60
            )

            if r.status_code == 429:
                honor_retry_after(r)
                continue

            if r.status_code != 200:
                # assume exists and move right slightly to avoid deadlock
                min_page = mid_page + 1
                continue

            try:
                payload = r.json()
                empty = not payload.get("response")
            except Exception:
                empty = True

            if empty:
                max_page = mid_page
            else:
                min_page = mid_page + 1

        return min_page

    # ──────────────────────────────────────────────────────────────────────────
    # FETCH: iterate endpoints and write per-page CSVs (atomic)
    # ──────────────────────────────────────────────────────────────────────────
    for endpoint_url, folder_name in endpoints.items():
        save_directory = os.path.join(data_directory, folder_name)
        os.makedirs(save_directory, exist_ok=True)

        count_endpoint = f"{endpoint_url}/count"
        count_response = session.get(count_endpoint, headers={"Authorization": f"Bearer {access_token}"}, timeout=60)
        if count_response.status_code == 429:
            honor_retry_after(count_response)
            count_response = session.get(count_endpoint, headers={"Authorization": f"Bearer {access_token}"}, timeout=60)
        count_data = count_response.json() if count_response.status_code == 200 else {}

        if "response" in count_data and "count" in count_data["response"]:
            data_count = int(count_data["response"]["count"])
            total_pages = (data_count + 99) // 100
            print(f"Total pages for {folder_name} data: {total_pages}")
        else:
            print(f"Count API Request for {folder_name} failed or did not provide count data. Using binary search to find the last page.")
            total_pages = binary_page_search(endpoint_url, access_token)
            print(f"Total pages for {folder_name} data: {total_pages}")

        current_page = 0
        with tqdm(total=total_pages, desc=f"Processing {folder_name}") as progress_bar:
            while current_page < total_pages:
                if ckpt.is_done(endpoint_url, current_page):
                    progress_bar.update(1)
                    current_page += 1
                    continue

                if endpoint_url == f"{base}activities":
                    url = (
                        f"{endpoint_url}?_page={current_page}&_limit=100&_fields=subject,thread_id,contract_period_id,parent,nonbillable,against_id,"
                        f"rate_charged,date_started,date_logged,rate,visibility,invoice_id,class,time_allocation,standing,owner,activity_class,"
                        f"against,date_modified,medium,id,activity_priority,date_created,parent_id,staff,owner_id,owner_type,thread,billable,priority"
                    )
                else:
                    url = f"{endpoint_url}?_page={current_page}&_limit=100&_fields=_ALL"

                headers = {"Authorization": f"Bearer {access_token}"}
                response = session.get(url, headers=headers, timeout=120)

                if response.status_code == 429:
                    honor_retry_after(response)
                    response = session.get(url, headers=headers, timeout=120)

                if response.status_code == 200:
                    resp_json = response.json()
                    data = resp_json["response"]["expenses"] if endpoint_url == f"{base}expenses" else resp_json["response"]

                    filename = f"{folder_name.lower()}_data_page_{current_page + 1}.csv"
                    file_path = os.path.join(save_directory, filename)
                    atomic_write_csv(pd.DataFrame(data), file_path)

                    ckpt.mark_done(endpoint_url, current_page)
                    current_page += 1
                    progress_bar.update(1)
                else:
                    print(f"API Request failed with status code {response.status_code}")
                    break

    # ──────────────────────────────────────────────────────────────────────────
    # MERGE: combine each endpoint's CSV pages into a single merged CSV
    # ──────────────────────────────────────────────────────────────────────────
    def merge_csv_files(endpoint_name, folder_name):
        print(f"Started processing {endpoint_name}")
        merged_data = pd.DataFrame()

        endpoint_directory = os.path.join(data_directory, folder_name)
        if not os.path.exists(endpoint_directory):
            print(f"Directory not found for {endpoint_name}")
            return

        for filename in os.listdir(endpoint_directory):
            if filename.endswith(".csv"):
                file_path = os.path.join(endpoint_directory, filename)
                print(f"FILEPATH: {file_path}")

                try:
                    data = pd.read_csv(file_path, sep=",")
                    convert_columns_to_datetime(data)
                    merged_data = pd.concat([merged_data, data], ignore_index=True)
                except EmptyDataError:
                    pass

        merged_file_path = os.path.join(data_directory, f"{folder_name}_merged.csv")
        merged_data.to_csv(merged_file_path, index=False)
        print(f"Merged data for {endpoint_name} into {merged_file_path}")

    for endpoint_url, folder_name in endpoints.items():
        merge_csv_files(endpoint_url.split("/")[-1], folder_name)

    # ──────────────────────────────────────────────────────────────────────────
    # TRANSFORM (Company Profiles ONLY): restore original pivot behavior
    # ──────────────────────────────────────────────────────────────────────────
    def transform_company_profiles(data_directory):
        """
        Reads 'Company Profiles_merged.csv', pivots field_name -> columns, and keeps:
        ['Partner','Office_Responsible','Department'] if present. Overwrites same file.
        """
        merged_file = os.path.join(data_directory, "Company Profiles_merged.csv")
        if not os.path.exists(merged_file):
            print("Company Profiles merged file not found:", merged_file)
            return None

        try:
            df = pd.read_csv(merged_file)

            # Required columns for the pivot
            required = {"link_id", "field_name", "value"}
            missing = required - set(df.columns)
            if missing:
                print(f"Error: Missing required columns for pivot: {missing}")
                return None

            # Pivot to wide format
            pivot = pd.pivot_table(
                df[["link_id", "field_name", "value"]],
                index="link_id",
                columns="field_name",
                values="value",
                aggfunc="first"
            ).reset_index()

            # Keep only desired columns that exist
            #desired = ["Partner", "Office_Responsible", "Department"]
            #keep = ["link_id"] + [c for c in desired if c in pivot.columns]
            #if len(keep) == 1:
                #print("Warning: none of the desired columns were found in the pivot.")
            #pivot = pivot[keep]

            # Overwrite merged file with transformed data (old behavior)
            pivot.to_csv(merged_file, index=False)
            print(f"Transformed Company Profiles written to: {merged_file}")
            return pivot

        except Exception as e:
            print(f"Error transforming Company Profiles: {e}")
            return None

    # Run the Company Profiles transform once
    transform_company_profiles(data_directory)

    # ──────────────────────────────────────────────────────────────────────────
    # EXPORT: push merged (and transformed) CSVs to PostgreSQL
    # ──────────────────────────────────────────────────────────────────────────
    def export_merged_csv_to_postgresql(data_directory, folder_name):
        # Create a SQLAlchemy engine
        engine = create_engine(f'postgresql://dashboardadmin:Calgary20!#@kpidashdb.postgres.database.azure.com:5432/kpidashboard')

        merged_file_path = os.path.join(data_directory, f"{folder_name}_merged.csv")
        if not os.path.exists(merged_file_path):
            print(f"Merged file not found for {folder_name}")
            return

        data = pd.read_csv(merged_file_path)
        convert_columns_to_datetime(data)
        convert_columns_to_hours(data)

        table_name = f"{folder_name.lower()}_data"
        data.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"Exported merged data for {folder_name} to {table_name} in PostgreSQL database")

    # Export (unchanged list)
    export_merged_csv_to_postgresql(data_directory, "Invoices")
    export_merged_csv_to_postgresql(data_directory, "Staff")
    export_merged_csv_to_postgresql(data_directory, "Jobs")
    export_merged_csv_to_postgresql(data_directory, "Expenses")
    export_merged_csv_to_postgresql(data_directory, "Rates")
    export_merged_csv_to_postgresql(data_directory, "Activities")
    export_merged_csv_to_postgresql(data_directory, "Companies")
    export_merged_csv_to_postgresql(data_directory, "Affiliations")
    export_merged_csv_to_postgresql(data_directory, "Issues")
    export_merged_csv_to_postgresql(data_directory, "Milestones")
    export_merged_csv_to_postgresql(data_directory, "Contracts")
    export_merged_csv_to_postgresql(data_directory, "Contracts Profiles")
    export_merged_csv_to_postgresql(data_directory, "Company Profiles")
    export_merged_csv_to_postgresql(data_directory, "Issues Profiles")
    export_merged_csv_to_postgresql(data_directory, "Jobs Profiles")
    export_merged_csv_to_postgresql(data_directory, "Tasks")
    export_merged_csv_to_postgresql(data_directory, "Memberships")
    export_merged_csv_to_postgresql(data_directory, "Groups")
    export_merged_csv_to_postgresql(data_directory, "Contract Periods")
    pass

except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)  # Exit with a non-zero status code in case of an error
