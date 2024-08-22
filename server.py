import base64
import logging
import os

import pandas as pd
import requests
from dotenv import load_dotenv, find_dotenv
from pandas.errors import EmptyDataError
from sqlalchemy import create_engine
from tqdm import tqdm
import sys


try:
    logging.basicConfig()
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

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
        # Add more endpoints as needed
    }

    # Encode client credentials to base64
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

    # Request access token
    token_response = requests.post(token_endpoint, data=token_params, headers=token_headers)

    if token_response.status_code != 200:
        print(f"Access token request failed with status code {token_response.status_code}")
        exit()

    token_data = token_response.json()
    access_token = token_data.get("access_token")

    if not access_token:
        print("Access token not obtained.")
        exit()

    # Define the directory where CSV files will be saved
    data_directory = "./data/API FIle"


    # Function to convert columns with "date" in their names to date (without time)
    def convert_columns_to_datetime(data):
        for column in data.columns:
            if 'date' in column.lower():
                try:
                    # Convert the Unix timestamp (seconds) to datetime
                    data[column] = pd.to_datetime(data[column], unit='s')
                    # Format the datetime values as YYYY-MM-DD
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


    def binary_page_search(endpoint_url, access_token):
        min_page, max_page = 0, 2 ** 20  # Using 2^20 as an arbitrary high number

        while min_page < max_page:
            mid_page = (min_page + max_page) // 2

            response = requests.get(
                f"{endpoint_url}?_page={mid_page}&_limit=100&_fields=_ALL",
                headers={"Authorization": f"Bearer {access_token}"}
            )

            if response.status_code == 200 and not response.json()["response"]:
                max_page = mid_page
            else:
                min_page = mid_page + 1

        return min_page


    def transform_data(data):
        """
        Transforms the data for the Company Profiles endpoint.
        """
        # Create a DataFrame from the input data
        df = pd.DataFrame(data)

        # Use pivot_table to handle missing data and aggregation
        df_pivot = pd.pivot_table(df, index='link_id', columns='field_name', values='value', aggfunc='first')

        # Select only the desired columns that exist
        desired_columns = ['Partner', 'Office_Responsible', 'Department']
        df_filtered = df_pivot[desired_columns].dropna(how='all', axis=1)

        return df_filtered


    # Iterate through endpoints, which contains URL and folder name pairs
    for endpoint_url, folder_name in endpoints.items():
        # Create a folder for the endpoint data
        save_directory = os.path.join(data_directory, folder_name)
        os.makedirs(save_directory, exist_ok=True)

        # Request count of data for the current endpoint
        count_endpoint = f"{endpoint_url}/count"
        count_response = requests.get(count_endpoint, headers={"Authorization": f"Bearer {access_token}"})
        count_data = count_response.json()

        # Check if count data is available in the response
        if "response" in count_data and "count" in count_data["response"]:
            data_count = int(count_data["response"]["count"])
            total_pages = (data_count + 99) // 100  # Calculate total pages, rounding up
            print(f"Total pages for {folder_name} data: {total_pages}")
        else:
            # If count data is not available, use binary search to find the last page
            print(
                f"Count API Request for {folder_name} failed or did not provide count data. Using binary search to find the last page.")
            total_pages = binary_page_search(endpoint_url, access_token)
            print(f"Total pages for {folder_name} data: {total_pages}")

        current_page = 0
        with tqdm(total=total_pages, desc=f"Processing {folder_name}") as progress_bar:
            while current_page < total_pages:
                if endpoint_url == f"{base}activities":
                    # Make a GET request to retrieve data with specific fields for activities
                    response = requests.get(
                        f"{endpoint_url}?_page={current_page}&_limit=100&_fields=subject,thread_id,contract_period_id,parent,nonbillable,against_id,"
                        f"rate_charged,date_started,date_logged,rate,visibility,invoice_id,class,time_allocation,standing,owner,activity_class,"
                        f"against,date_modified,medium,id,activity_priority,date_created,parent_id,staff,owner_id,owner_type,thread,billable,priority",
                        headers={"Authorization": f"Bearer {access_token}"},
                    )
                else:
                    # Make a GET request to retrieve data with all fields for other endpoints
                    response = requests.get(
                        f"{endpoint_url}?_page={current_page}&_limit=100&_fields=_ALL",
                        headers={"Authorization": f"Bearer {access_token}"},
                    )

                if response.status_code == 200:
                    # Parse JSON response and process the data
                    data = response.json()["response"]["expenses"] if endpoint_url == f"{base}expenses" else \
                        response.json()["response"]

                    # Write the data to a CSV file
                    filename = f"{folder_name.lower()}_data_page_{current_page + 1}.csv"
                    file_path = os.path.join(save_directory, filename)
                    pd.DataFrame(data).to_csv(file_path, index=False)
                    current_page += 1
                    progress_bar.update(1)
                else:
                    # Handle API request failure
                    print(f"API Request failed with status code {response.status_code}")
                    break


    # Merge CSV files for each endpoint
    def merge_csv_files(endpoint_name, folder_name):
        print(f"Started processing {endpoint_name}")
        merged_data = pd.DataFrame()  # Initialize an empty DataFrame

        # Define the directory path for this endpoint
        endpoint_directory = os.path.join(data_directory, folder_name)

        # Check if the directory exists
        if not os.path.exists(endpoint_directory):
            print(f"Directory not found for {endpoint_name}")
            return

        # Iterate through the files in the directory
        for filename in os.listdir(endpoint_directory):
            if filename.endswith(".csv"):
                file_path = os.path.join(endpoint_directory, filename)

                print(f"FILEPATH: {file_path}")

                try:
                    # Read the CSV file and append it to the merged_data DataFrame
                    data = pd.read_csv(file_path, sep=",")

                    # Convert columns with "date" in their names to datetime
                    convert_columns_to_datetime(data)
                    merged_data = pd.concat([merged_data, data], ignore_index=True)
                except EmptyDataError:
                    pass

        # Define the path for the merged CSV file
        merged_file_path = os.path.join(data_directory, f"{folder_name}_merged.csv")

        # Save the merged data to a CSV file
        merged_data.to_csv(merged_file_path, index=False)
        print(f"Merged data for {endpoint_name} into {merged_file_path}")


    # Merge CSV files for each endpoint
    for endpoint_url, folder_name in endpoints.items():
        merge_csv_files(endpoint_url.split("/")[-1], folder_name)


    def transform_data(data_file, desired_columns):
        """
        Transforms the merged data.v

        Args:
        - data_file (str): Path to the CSV file containing the merged data.
        - desired_columns (list): List of column names to include in the transformed DataFrame.

        Returns:
        - pd.DataFrame: Transformed DataFrame with selected columns.
        """
        try:
            # Read the merged data from the CSV file
            data = pd.read_csv(data_file)

            # Print columns in the data
            print(f"Columns in the data:\n{data.columns}")

            # Filter the DataFrame to include only the desired columns
            filtered_data = data[desired_columns]

            # Pivot the table to create columns for each unique 'field_name'
            pivot_table = pd.pivot_table(filtered_data, index=['link_id'], columns='field_name', values='value',
                                         aggfunc='first').reset_index()

            # Save the transformed data back to the CSV file
            pivot_table.to_csv(data_file, index=False)

            return pivot_table

        except Exception as e:
            print(f"Error: {e}. Skipping transformation for {data_file}")
            return None


    # Example usage:
    data_file_path = './data/API FIle/Company Profiles_merged.csv'
    desired_columns_companies = ['link_id', 'field_name', 'value']
    transformed_data_companies = transform_data(data_file_path, desired_columns_companies)

    # # Example usage for a different dataset:
    # data_file_path_contracts = './data/API FIle/Contracts Profiles_merged.csv'
    # desired_columns_contracts = ['link_id', 'field_name', 'value']
    # transformed_data_contracts = transform_data(data_file_path_contracts, desired_columns_contracts)
    #
    # data_file_path_issues = 'C:/API FIle/Issues Profiles_merged.csv'
    # desired_columns_issues = ['link_id', 'field_name', 'value']
    # transformed_data_issues = transform_data(data_file_path_issues, desired_columns_issues)


    # Function to export merged CSV data to PostgreSQLL
    def export_merged_csv_to_postgresql(data_directory, folder_name):
        # Create a SQLAlchemy engine
        engine = create_engine(f'postgresql://dashboardadmin:Calgary20!#@kpidashdb.postgres.database.azure.com:5432/kpidashboard')

#'postgresql://dashboardadmin:Calgary20%40%23@pkfresourcedb.postgres.database.azure.com:5432/postgres') #azure
#'postgresql://pbk_admin:{db_master}:6p@24.144.94.16:5432/resource_db')
#            f'postgresql://doadmin:AVNS_5H7J5toQ6bb_9ebZI2o@db-postgresql-sfo3-dwh-do-user-14386803-0.c.db.ondigitalocean.com:25060/defaultdb?sslmode=require'
#        )

        # Define the path for the merged CSV file
        merged_file_path = os.path.join(data_directory, f"{folder_name}_merged.csv")

        # Check if the merged file exists
        if not os.path.exists(merged_file_path):
            print(f"Merged file not found for {folder_name}")
            return

        # Read the merged CSV file
        data = pd.read_csv(merged_file_path)

        # Convert columns with "date" in their names to datetime
        convert_columns_to_datetime(data)

        # Convert columns with "billable" in their names to hours
        convert_columns_to_hours(data)

        # Export the data to the PostgreSQL database
        table_name = f"{folder_name.lower()}_data"  # Define a table name based on folder_name
        data.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"Exported merged data for {folder_name} to {table_name} in PostgreSQL database")


    # Example usage:
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



