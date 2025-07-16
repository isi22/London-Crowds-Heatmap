import requests
import time
import json
import pandas as pd  # Import pandas for DataFrame operations

# --- Configuration ---
TFL_STOPPOINT_URL = "https://api.tfl.gov.uk/StopPoint/Mode/tube"
STATION_MAP_FILENAME = "data/station_info.xlsx"  # File path to save data files


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
                f"Error calling TfL API (Attempt {retry_attempt + 1}/{max_retries}): {e}"
            )
            if retry_attempt == max_retries - 1:
                raise RuntimeError(
                    f"Failed to fetch data from {url} after {max_retries} retries: {e}"
                )
        time.sleep(1)  # Delay between retries
    return []


# --- APPLICATION ENTRY POINT ---
if __name__ == "__main__":
    print("Fetching TfL StopPoint data for Tube stations...")
    response = query_TFL(TFL_STOPPOINT_URL)

    stop_points_data = []

    # Extract relevant data for NaptanMetroStation entries
    for entry in response.get("stopPoints", []):  # Use .get() for safety
        if entry.get("stopType") == "NaptanMetroStation":
            original_common_name = entry.get("commonName", "")

            # Remove " Underground Station" suffix for cleaner station names
            cleaned_common_name = original_common_name.removesuffix(
                " Underground Station"
            )

            stop_points_data.append(
                {
                    "stop_id": entry.get("id"),
                    "station": cleaned_common_name,
                    "lat": entry.get("lat"),
                    "lon": entry.get("lon"),
                }
            )

    print(f"\nFound {len(stop_points_data)} NaptanMetroStation entries.")
    # print(json.dumps(stop_points_data, indent=2)) # Uncomment to print to console

    # Create DataFrame from the collected data
    df_stop_points = pd.DataFrame(stop_points_data)

    # Save DataFrame to Excel file
    try:
        df_stop_points.to_excel(
            STATION_MAP_FILENAME, index=False
        )  # index=False prevents writing DataFrame index
        print(f"Stop points DataFrame successfully saved to '{STATION_MAP_FILENAME}'")
    except Exception as e:
        print(f"Error saving stop points DataFrame to Excel: {e}")

    print("\nProcess finished.")
