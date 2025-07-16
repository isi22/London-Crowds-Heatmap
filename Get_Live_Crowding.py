#!/usr/bin/env python3

import requests
import time
import json
import os
import pandas as pd
from datetime import datetime
import gspread  # Import gspread for Google Sheets interaction

# --- API Endpoints and File Paths ---
TFL_STOPPOINT_URL = "https://api.tfl.gov.uk/crowding/{Naptan}/Live"
FILE_PATH_STATION_FOOTFALL_BASELINE = os.getenv("STATIONS_BASELINE_FOOTFALL_PATH")

# --- Configuration from Environment Variables ---
# TFL API Key for querying TfL data
TFL_API_KEY = os.getenv("TFL_API_KEY")

# Google Sheets API credentials and sheet details
GOOGLE_SERVICE_ACCOUNT_KEY_PATH = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY_PATH")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_WORKSHEET_NAME = "Sheet1"  # Always save to Sheet1

# --- Data Retention Configuration ---
MAX_ROWS_GOOGLE_SHEET = 100000  # Maximum desired rows in Google Sheet


def query_TFL(
    url: str,
    params: dict = None,
    max_retries: int = 3,
    _session: requests.Session = None,
) -> list:
    """Queries the TfL API with retry logic."""
    session_to_use = _session if _session else requests.Session()
    for retry_attempt in range(max_retries):
        try:
            response = session_to_use.get(url, params=params, timeout=10)
            response.raise_for_status()
            json_response = response.json()
            return json_response if json_response else []
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            print(
                f"Error querying TfL API (Attempt {retry_attempt + 1}/{max_retries}): {e}"
            )
            if retry_attempt == max_retries - 1:
                raise RuntimeError(
                    f"Failed to fetch data from {url} after {max_retries} retries: {e}"
                )
        time.sleep(0.1)  # Small delay between retries
    return []


def load_station_footfall_baseline(file_path):
    """Loads TfL station ID mapping from an Excel file into a Pandas DataFrame."""
    if not os.path.exists(file_path):
        print(f"Error: Baseline file not found at '{file_path}'")
        return pd.DataFrame()

    print(f"Loading station ids from '{file_path}'...")
    try:
        df_loaded = pd.read_excel(file_path)
        print(f"Successfully loaded {len(df_loaded)} stop points.")
        return df_loaded
    except Exception as e:
        print(f"Error loading baseline data: {e}")
        return pd.DataFrame()


def get_Live_Crowding(tfl_url_pattern, df_stations):
    """Fetches live crowding data for stations and updates DataFrame."""
    df_stations_crowding = df_stations.copy()
    df_stations_crowding["live_percentage_baseline"] = pd.NA

    api_params = {"app_key": TFL_API_KEY}

    print("Fetching live crowding data...")
    for idx, station_row in df_stations_crowding.iterrows():
        station_id = station_row["stop_id"]

        current_station_url = tfl_url_pattern.format(Naptan=station_id)

        try:
            response = query_TFL(current_station_url, api_params)
            percentage_value = response.get("percentageOfBaseline")
            df_stations_crowding.loc[idx, "live_percentage_baseline"] = percentage_value
            time.sleep(0.1)  # Delay between API calls to avoid rate limits
        except RuntimeError as e:
            print(f"API call failed for {station_id}: {e}")
            df_stations_crowding.loc[idx, "live_percentage_baseline"] = pd.NA
        except Exception as e:
            print(f"Unexpected error for {station_id}: {e}")
            df_stations_crowding.loc[idx, "live_percentage_baseline"] = pd.NA

    df_stations_crowding["timestamp"] = datetime.now()
    df_stations_crowding["live_footfall"] = (
        df_stations_crowding["footfall_baseline"]
        * df_stations_crowding["live_percentage_baseline"]
    )
    df_stations_crowding = df_stations_crowding.drop(
        columns=[
            "footfall_baseline",
            "live_percentage_baseline",
        ]
    )

    print("\n--- Updated DataFrame with Crowding Data (head) ---")
    print(df_stations_crowding.head(20))

    return df_stations_crowding


def save_dataframe_to_google_sheet(
    df: pd.DataFrame, sheet_id: str, worksheet_name: str, credentials_path: str
):
    """
    Appends a DataFrame to a Google Sheet worksheet.
    If adding new data would exceed MAX_ROWS_GOOGLE_SHEET, it deletes old rows first.
    """
    if not os.path.exists(credentials_path):
        print(f"Error: Google credentials file not found at {credentials_path}")
        return

    if not sheet_id:
        print("Error: Google Sheet ID not provided.")
        return

    if not worksheet_name:
        print("Error: Google Worksheet Name not provided.")
        return

    try:
        gc = gspread.service_account(filename=credentials_path)
        spreadsheet = gc.open_by_key(sheet_id)

        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            # Create with minimal rows/cols; gspread will expand as needed
            worksheet = spreadsheet.add_worksheet(
                title=worksheet_name, rows="1", cols="1"
            )
            print(f"Created new worksheet: {worksheet_name}")

        # Get current row count (including header and empty rows)
        current_total_rows = worksheet.row_count
        rows_to_add = len(df)

        # Determine if headers need to be written
        # Assumes if row_count is 0, or 1 and A1 is empty, then headers are needed
        # Otherwise, assume headers already exist
        needs_headers = (current_total_rows == 0) or (
            current_total_rows == 1 and not worksheet.acell("A1").value
        )

        # If headers are needed, write them first
        if needs_headers:
            print(f"Worksheet '{worksheet_name}' is empty. Appending headers.")
            worksheet.append_rows([df.columns.values.tolist()])
            current_total_rows = (
                worksheet.row_count
            )  # Update count after adding headers

        # --- Trimming Logic (before appending new data) ---
        if (current_total_rows + rows_to_add) > MAX_ROWS_GOOGLE_SHEET:
            rows_to_delete = rows_to_add  # Delete a chunk equal to the new data size

            # Ensure we don't try to delete the header row (row 1)
            # Deletion starts from row 2 (first data row)
            delete_start_index = 2
            delete_end_index = delete_start_index + rows_to_delete - 1

            print(
                f"Google Sheet rows ({current_total_rows}) + new rows ({rows_to_add}) exceed limit ({MAX_ROWS_GOOGLE_SHEET}). Deleting {rows_to_delete} oldest rows (from row {delete_start_index} to {delete_end_index})."
            )

            # Perform the deletion
            worksheet.delete_rows(delete_start_index, delete_end_index)
            print(f"Successfully deleted rows {delete_start_index}-{delete_end_index}.")
            # current_total_rows will implicitly be reduced on Google's side

        # --- Convert Timestamp column to string before appending ---
        # Format to string, which Google Sheets understands
        df["timestamp"] = df["timestamp"].astype(str)

        # --- Append new data rows ---
        print(f"Appending {rows_to_add} new rows to Google Sheet '{worksheet_name}'...")
        worksheet.append_rows(df.values.tolist())

        print(
            f"DataFrame operations completed for Google Sheet '{spreadsheet.title}' (Worksheet: '{worksheet_name}')."
        )

    except gspread.exceptions.APIError as e:
        print(f"Error saving to Google Sheet (API): {e.response.text}")
    except Exception as e:
        print(f"Error saving to Google Sheet: {e}")


# --- APPLICATION ENTRY POINT ---
if __name__ == "__main__":
    # --- Validate Environment Variables ---
    if not TFL_API_KEY:
        print("Error: TFL_API_KEY environment variable not set. Exiting.")
        exit(1)
    if not GOOGLE_SERVICE_ACCOUNT_KEY_PATH:
        print(
            "Error: GOOGLE_SERVICE_ACCOUNT_KEY_PATH environment variable not set. Exiting."
        )
        exit(1)
    if not GOOGLE_SHEET_ID:
        print("Error: GOOGLE_SHEET_ID environment variable not set. Exiting.")
        exit(1)
    # GOOGLE_WORKSHEET_NAME is hardcoded to "Sheet1" now

    # --- Data Loading and Processing ---
    df_stations = load_station_footfall_baseline(FILE_PATH_STATION_FOOTFALL_BASELINE)
    if df_stations.empty:
        print("Exiting due to failure to load station baseline data.")
        exit(1)

    df_stations_crowding = get_Live_Crowding(TFL_STOPPOINT_URL, df_stations)
    if df_stations_crowding.empty:
        print("No live crowding data fetched. Skipping save operations.")
        exit(0)  # Exit gracefully if no data

    # --- Data Saving Phase ---

    # Save to Google Sheets (handles appending and conditional trimming)
    save_dataframe_to_google_sheet(
        df_stations_crowding,
        GOOGLE_SHEET_ID,
        GOOGLE_WORKSHEET_NAME,  # Will be "Sheet1"
        GOOGLE_SERVICE_ACCOUNT_KEY_PATH,
    )

    print("\n--- Script Execution Finished ---")
