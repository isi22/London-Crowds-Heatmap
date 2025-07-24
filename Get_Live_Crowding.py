#!/usr/bin/env python3

import requests
import time
import json
import os
import pandas as pd
from datetime import datetime, timedelta
import gspread  # Import gspread for Google Sheets interaction

# --- API Endpoints and File Paths ---
TFL_STOPPOINT_URL = "https://api.tfl.gov.uk/crowding/{Naptan}/Live"
FILE_PATH_STATION_INFO = "data/station_info.xlsx"  # Needed for JSON generation
FILE_PATH_STATION_FOOTFALL_BASELINE = "data/stations_baseline_footfall.xlsx"
OUTPUT_HTML_JSON_FILE = (
    "data/live_crowding_for_heatmap.json"  # Local JSON output for HTML
)

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
        # Ensure stop_id is string, as NaPTAN codes are alphanumeric
        df_loaded["stop_id"] = df_loaded["stop_id"].astype(str)
        print(f"Successfully loaded {len(df_loaded)} stop points.")
        return df_loaded
    except Exception as e:
        print(f"Error loading baseline data: {e}")
        return pd.DataFrame()


def load_excel_file(file_path):
    """Loads an Excel file into a Pandas DataFrame."""
    if not os.path.exists(file_path):
        print(f"Warning: File not found at '{file_path}'. Returning empty DataFrame.")
        return pd.DataFrame()
    try:
        df_loaded = pd.read_excel(file_path)
        print(f"Successfully loaded {len(df_loaded)} rows from '{file_path}'.")
        return df_loaded
    except Exception as e:
        print(f"Error loading data from '{file_path}': {e}")
        return pd.DataFrame()


def get_Live_Crowding(tfl_url_pattern, df_stations_with_baseline_and_name):
    """
    Fetches live crowding data for stations and returns a DataFrame
    containing only stop_id, live_footfall, and timestamp, in the specified order.
    """
    api_params = {"app_key": TFL_API_KEY}
    current_timestamp = datetime.now()  # Capture current time once for all rows

    print("Fetching live crowding data...")
    all_live_data_for_sheet = []  # To collect data for the Google Sheet

    with requests.Session() as session:  # Use a single session for all API calls
        for idx, station_row in df_stations_with_baseline_and_name.iterrows():
            station_id = str(
                station_row["stop_id"]
            )  # Ensure stop_id is string for API call
            baseline_footfall = station_row["footfall_baseline"]  # Used for calculation

            current_station_url = tfl_url_pattern.format(Naptan=station_id)

            try:
                response = query_TFL(current_station_url, api_params, _session=session)
                percentage_value = response.get("percentageOfBaseline")

                if percentage_value is not None:
                    # Calculate live_footfall based on baseline and percentage ratio
                    live_footfall = baseline_footfall * percentage_value
                    all_live_data_for_sheet.append(
                        {
                            "stop_id": station_id,
                            "timestamp": current_timestamp,  # Explicitly placing timestamp second
                            "live_footfall": float(
                                live_footfall
                            ),  # Explicitly placing live_footfall third
                        }
                    )
                else:
                    print(
                        f"No 'percentageOfBaseline' found for {station_id}. Skipping this data point for sheet."
                    )

                time.sleep(0.05)  # Delay between API calls to avoid rate limits

            except RuntimeError as e:
                print(f"API call failed for {station_id}: {e}")
            except Exception as e:
                print(f"Unexpected error for {station_id}: {e}")

    # Create the DataFrame with only the desired columns for the Google Sheet
    # The order is already set by appending dictionaries in the desired order
    df_live_crowding_for_sheet = pd.DataFrame(all_live_data_for_sheet)

    if not df_live_crowding_for_sheet.empty:
        # Ensure correct types after DataFrame creation
        df_live_crowding_for_sheet["stop_id"] = df_live_crowding_for_sheet[
            "stop_id"
        ].astype(str)
        df_live_crowding_for_sheet["live_footfall"] = df_live_crowding_for_sheet[
            "live_footfall"
        ].astype(float)
        df_live_crowding_for_sheet["timestamp"] = pd.to_datetime(
            df_live_crowding_for_sheet["timestamp"]
        )
    else:  # Ensure columns are correct even if no data fetched
        df_live_crowding_for_sheet = pd.DataFrame(
            columns=["stop_id", "timestamp", "live_footfall"]
        )  # Explicitly define order for empty df
        df_live_crowding_for_sheet["timestamp"] = pd.to_datetime(
            []
        )  # Create empty datetime series for empty df

    print("\n--- Fetched Live Crowding Data for Sheet (head) ---")
    print(df_live_crowding_for_sheet.head(20))

    return df_live_crowding_for_sheet


def load_historical_data_from_google_sheet(
    sheet_id: str, worksheet_name: str, credentials_path: str
) -> pd.DataFrame:
    """
    Loads all historical data from a Google Sheet worksheet into a Pandas DataFrame.
    Returns an empty DataFrame if the sheet or worksheet is empty or inaccessible.
    """
    if not os.path.exists(credentials_path):
        print(f"Error: Google credentials file not found at {credentials_path}")
        return pd.DataFrame()

    if not sheet_id:
        print("Error: Google Sheet ID not provided.")
        return pd.DataFrame()

    try:
        gc = gspread.service_account(filename=credentials_path)
        spreadsheet = gc.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)

        # Get all records as a list of dictionaries
        data = worksheet.get_all_records()
        if not data:
            print(f"Worksheet '{worksheet_name}' is empty or contains only headers.")
            return pd.DataFrame()

        df = pd.DataFrame(data)

        # Ensure correct data types for processing, especially for timestamp and stop_id
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        if "stop_id" in df.columns:
            df["stop_id"] = df["stop_id"].astype(
                str
            )  # stop_id should be string (NaPTAN code)
        if "live_footfall" in df.columns:
            df["live_footfall"] = pd.to_numeric(df["live_footfall"], errors="coerce")

        # Drop rows where essential data (timestamp, stop_id, live_footfall) is missing
        df = df.dropna(subset=["timestamp", "stop_id", "live_footfall"])
        print(
            f"Successfully loaded {len(df)} historical rows from Google Sheet '{worksheet_name}'."
        )
        return df
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{worksheet_name}' not found. Returning empty DataFrame.")
        return pd.DataFrame()
    except gspread.exceptions.APIError as e:
        print(f"Error loading from Google Sheet (API): {e.response.text}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error loading from Google Sheet: {e}")
        return pd.DataFrame()


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

            # Protect against deleting more rows than exist (excluding header)
            if delete_end_index >= current_total_rows:
                # Adjust rows_to_delete to ensure we don't delete header or go out of bounds
                rows_to_delete = max(0, current_total_rows - delete_start_index)
                delete_end_index = delete_start_index + rows_to_delete - 1
                print(f"Adjusted deletion: will delete {rows_to_delete} rows.")

            if rows_to_delete > 0:
                # Perform the deletion
                worksheet.delete_rows(delete_start_index, delete_end_index)
                print(
                    f"Successfully deleted rows {delete_start_index}-{delete_end_index}."
                )
            else:
                print(
                    "No rows to delete as current rows are within limit or too few to delete."
                )

        # --- Prepare DataFrame for Google Sheets appending ---
        df_to_append = df.copy()

        # Fill NaN values in 'live_footfall' with 0 to make it JSON compliant for gspread
        if "live_footfall" in df_to_append.columns:
            df_to_append["live_footfall"] = df_to_append["live_footfall"].fillna(
                0
            )  # FIX: Handles NaN error

        # Convert Timestamp column to string before appending (as in original code)
        df_to_append["timestamp"] = df_to_append["timestamp"].astype(str)

        # --- Append new data rows ---
        print(f"Appending {rows_to_add} new rows to Google Sheet '{worksheet_name}'...")
        worksheet.append_rows(df_to_append.values.tolist())

        print(
            f"DataFrame operations completed for Google Sheet '{spreadsheet.title}' (Worksheet: '{worksheet_name}')."
        )

    except gspread.exceptions.APIError as e:
        print(f"Error saving to Google Sheet (API): {e.response.text}")
    except Exception as e:
        print(f"Error saving to Google Sheet: {e}")


def generate_heatmap_json(
    df_live_crowding: pd.DataFrame,
    df_station_info: pd.DataFrame,
    df_baseline_footfall: pd.DataFrame,
    output_json_path: str,
):
    """
    Processes all historical live crowding data to generate the JSON structure
    required by the HTML heatmap, for hourly, daily, and weekly resolutions.
    """
    print(f"Generating heatmap JSON for HTML to '{output_json_path}'...")

    if df_live_crowding.empty or df_station_info.empty or df_baseline_footfall.empty:
        print("Warning: One or more input DataFrames are empty. Generating empty JSON.")
        os.makedirs(
            os.path.dirname(output_json_path), exist_ok=True
        )  # Ensure dir exists
        with open(output_json_path, "w") as f:
            json.dump({}, f)
        return

    # Ensure 'timestamp' is datetime and sort for proper grouping
    df_live_crowding["timestamp"] = pd.to_datetime(df_live_crowding["timestamp"])
    df_live_crowding = df_live_crowding.sort_values("timestamp").reset_index(drop=True)

    # Ensure 'footfall_baseline' is numeric, as it comes from Excel
    df_baseline_footfall["footfall_baseline"] = pd.to_numeric(
        df_baseline_footfall["footfall_baseline"], errors="coerce"
    ).fillna(0)
    max_baseline_footfall = df_baseline_footfall["footfall_baseline"].max()

    if max_baseline_footfall == 0:
        print("Warning: Max baseline footfall is zero, crowding metrics will be zero.")

    # Merge station info and baseline into live crowding data for calculations
    # This creates a combined DataFrame for processing each time unit
    df_combined_data = pd.merge(
        df_live_crowding,
        df_station_info[["stop_id", "station", "lat", "lon"]],
        on="stop_id",
        how="left",
    )
    df_combined_data["lat"] = pd.to_numeric(df_combined_data["lat"], errors="coerce")
    df_combined_data["lon"] = pd.to_numeric(df_combined_data["lon"], errors="coerce")
    df_combined_data["station"] = df_combined_data["station"].astype(
        str
    )  # Ensure station names are strings

    processed_all_data = {}

    # --- Helper to process data for a specific resolution ---
    def _process_resolution_data(
        df_data_to_group, current_max_baseline, resolution_type
    ):
        res_data = {}

        # Determine the time grouping column
        if resolution_type == "hourly":
            df_data_to_group["time_unit"] = df_data_to_group["timestamp"].dt.round("h")
        elif resolution_type == "daily":
            df_data_to_group["time_unit"] = df_data_to_group["timestamp"].dt.normalize()
        elif resolution_type == "weekly":
            # Ensure week_start is Monday
            df_data_to_group["time_unit"] = (
                df_data_to_group["timestamp"]
                .dt.to_period("W")
                .apply(lambda r: r.start_time)
            )
        else:
            raise ValueError("Invalid resolution type for JSON generation")

        # Group data and calculate average live_footfall per station per time unit
        grouped_agg_data = (
            df_data_to_group.groupby(["time_unit", "stop_id"])
            .agg(
                live_footfall=("live_footfall", "mean"),
                station=("station", "first"),  # Keep first station name
                lat=("lat", "first"),  # Keep first lat
                lon=("lon", "first"),  # Keep first lon
            )
            .reset_index()
        )

        unique_time_units = sorted(grouped_agg_data["time_unit"].unique())

        for time_unit in unique_time_units:
            current_time_unit_data = grouped_agg_data[
                grouped_agg_data["time_unit"] == time_unit
            ].copy()

            # Calculate crowding metric (as percentage)
            if current_max_baseline == 0:
                current_time_unit_data.loc[:, "crowding_metric"] = 0
            else:
                # The percentage_value is treated as a ratio (e.g., 0.8), so multiply by 100 for metric
                current_time_unit_data.loc[:, "crowding_metric"] = (
                    current_time_unit_data["live_footfall"] / current_max_baseline
                ) * 100

            current_time_unit_data.loc[:, "crowding_metric"] = (
                current_time_unit_data["crowding_metric"]
                .replace([float("inf"), -float("inf")], pd.NA)
                .fillna(0)  # Replace any remaining NaN with 0
            )

            # Drop rows where lat/lon/station might have been coerced to NaN due to issues
            heatmap_data_for_ts = (
                current_time_unit_data[["lat", "lon", "crowding_metric", "station"]]
                .dropna(subset=["lat", "lon", "station"])  # Ensure lat/lon are not NaN
                .values.tolist()
            )
            res_data[str(time_unit)] = heatmap_data_for_ts
        return res_data

    # Generate data for each resolution, passing a copy
    processed_all_data["hourly"] = _process_resolution_data(
        df_combined_data.copy(), max_baseline_footfall, "hourly"
    )
    processed_all_data["daily"] = _process_resolution_data(
        df_combined_data.copy(), max_baseline_footfall, "daily"
    )
    processed_all_data["weekly"] = _process_resolution_data(
        df_combined_data.copy(), max_baseline_footfall, "weekly"
    )

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_json_path), exist_ok=True)

    # Save to JSON
    with open(output_json_path, "w") as f:
        json.dump(processed_all_data, f, indent=2)
    print(f"Successfully generated heatmap JSON to '{output_json_path}'.")


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

    # --- Data Loading (Static and Baseline) ---
    df_station_info = load_excel_file(FILE_PATH_STATION_INFO)  # NEW: load station info
    df_baseline_footfall = load_station_footfall_baseline(
        FILE_PATH_STATION_FOOTFALL_BASELINE
    )  # Original function

    if df_station_info.empty:
        print("Exiting due to failure to load station info data.")
        exit(1)
    if df_baseline_footfall.empty:
        print("Exiting due as no footfall baseline data was loaded.")
        exit(1)

    # Ensure stop_id is string in info for merging later
    df_station_info["stop_id"] = df_station_info["stop_id"].astype(str)

    # Prepare for get_Live_Crowding: Pass only stop_id and footfall_baseline
    # The 'station' column is intentionally excluded here to prevent it from being passed
    # to get_Live_Crowding and subsequently saved to Google Sheets.
    df_stations_for_api = df_baseline_footfall[["stop_id", "footfall_baseline"]].copy()

    # --- Fetch Current Live Crowding Data ---
    df_current_live_crowding = get_Live_Crowding(TFL_STOPPOINT_URL, df_stations_for_api)

    if df_current_live_crowding.empty:
        print(
            "No live crowding data fetched for current run. Skipping save operations."
        )
        exit(0)  # Exit gracefully if no data

    # --- Save Current Data to Google Sheets ---
    save_dataframe_to_google_sheet(
        df_current_live_crowding,
        GOOGLE_SHEET_ID,
        GOOGLE_WORKSHEET_NAME,
        GOOGLE_SERVICE_ACCOUNT_KEY_PATH,
    )

    # --- Load ALL historical data from Google Sheets for JSON generation ---
    df_historical_live_crowding = load_historical_data_from_google_sheet(
        GOOGLE_SHEET_ID,
        GOOGLE_WORKSHEET_NAME,
        GOOGLE_SERVICE_ACCOUNT_KEY_PATH,
    )

    if df_historical_live_crowding.empty:
        print(
            "No historical live crowding data found in Google Sheet to generate JSON from. Exiting."
        )
        exit(1)

    # --- Generate JSON for HTML heatmap ---
    # Pass all necessary DataFrames for JSON generation
    generate_heatmap_json(
        df_historical_live_crowding,
        df_station_info,  # Passed for lat/lon/station name
        df_baseline_footfall,  # Passed for max_baseline_footfall
        OUTPUT_HTML_JSON_FILE,
    )

    print("\n--- Script Execution Finished ---")
