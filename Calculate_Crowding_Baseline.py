import requests
import time
import os
from urllib.parse import urljoin, quote
from bs4 import BeautifulSoup
import pandas as pd
import matplotlib.pyplot as plt
import re

# --- Configuration ---
# Base URL for the S3 bucket API endpoint.
S3_BUCKET_API_BASE_URL = "https://s3-eu-west-1.amazonaws.com/crowding.data.tfl.gov.uk/"

# The specific folder path within the S3 bucket to list.
TARGET_S3_PREFIX = "Network Demand/"

# The specific folder path within the S3 bucket to list.
TARGET_FILENAME_PREFIX = "StationFootfall"

# Local folder to save the downloaded CSV files.
DOWNLOAD_FOLDER = "NetworkDemand/"

FILE_PATH_STATION_INFO = "data/station_info.xlsx"
FILE_PATH_FOOTFALL_BASELINE = "data/stations_baseline_footfall.xlsx"


def list_s3_bucket_files(base_url, s3_prefix):
    """
    Fetches the XML listing of an S3 bucket path and extracts file keys.
    """
    # Construct the S3 API request URL with the specified prefix.
    # quote() is used to URL-encode the prefix (ee.g., spaces to %20).
    s3_list_url = f"{base_url}?list-type=2&max-keys=1000&prefix={quote(s3_prefix)}"

    response = requests.get(s3_list_url)
    response.raise_for_status()  # Raises HTTPError for bad responses.

    soup = BeautifulSoup(response.text, "xml")  # Parses the XML response.

    file_keys = []
    # Extracts file keys from <Contents> tags in the XML.
    for content_tag in soup.find_all("Contents"):
        key = content_tag.find("Key").text
        # Excludes folder markers and the index.html file.
        if key.endswith("/") or key == "index.html":
            continue
        file_keys.append(key)

    return file_keys


def download_file(url, local_filename=None):
    """
    Downloads a single file from a URL to a local path.
    Handles basic HTTP and file I/O errors.
    """

    with requests.get(url, stream=True) as r:
        r.raise_for_status()  # Checks for HTTP errors (e.g., 404).

        with open(local_filename, "wb") as f:
            # Writes content in 8KB chunks.
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"File '{local_filename}' downloaded successfully.")
    return local_filename


def get_network_demand_files(base_url, s3_prefix, filename_prefix, save_folder):

    # Creates parent directories if they don't exist.
    os.makedirs(os.path.dirname(save_folder) or ".", exist_ok=True)

    # Delete the last file that contains last years and current years data.
    # When a new year starts, this file is replaced by two separate files.
    # E.g. StationFootfall_2024_2025.csv -> StationFootfall_2024.csv and StationFootfall_2025.csv.

    files = sorted(os.listdir(save_folder))
    print(f"Deleting the last file in '{save_folder}' ({files[-1]})...")
    os.remove(os.path.join(save_folder, files[-1]))

    print(f"Getting files in S3 prefix: {s3_prefix}")

    # Get all file keys within the target S3 prefix.
    all_s3_keys = list_s3_bucket_files(base_url, s3_prefix)

    downloaded_count = 0

    # Filter for files matching the "StationFootfall_XX.csv" pattern.
    for s3_key in all_s3_keys:
        filename = s3_key.split("/")[-1]  # Extract just the filename from the S3 key.
        if (
            filename.startswith(filename_prefix)
            and filename.endswith(".csv")
            and (filename not in files[:-1])
        ):
            # Construct the full public HTTP URL for downloading the file.
            # urljoin combines the base public URL with the S3 key.
            full_download_url = urljoin("https://crowding.data.tfl.gov.uk/", s3_key)

            # Define the local path to save the file.
            local_file_path = os.path.join(save_folder, filename)

            # Download the file.
            downloaded_path = download_file(full_download_url, local_file_path)

            if downloaded_path:
                downloaded_count += 1

            time.sleep(0.1)  # Small delay to be polite to the server.

    print(
        f"\nCompleted: Downloaded {downloaded_count} matching CSV files to '{save_folder}'."
    )


def load_station_info(file_path):
    """
    Loads TfL station info mapping from an Excel file into a Pandas DataFrame.
    """
    if not os.path.exists(file_path):
        print(f"Error: Station info file not found at '{file_path}'")
        return pd.DataFrame()

    print(f"Loading station info from '{file_path}'...")
    try:
        df_loaded = pd.read_excel(file_path)
        print(f"Successfully loaded {len(df_loaded)} stop points into DataFrame.")
        return df_loaded

    except Exception as e:
        print(f"Error loading station info from Excel: {e}")
        return pd.DataFrame()


def make_station_footfall_dataframe(folder_path):

    # Empty list to store DataFrames from each CSV file.
    all_footfall_dfs = []

    # Custom column names
    custom_headers = ["date", "weekday", "station", "entries", "exits"]

    # Loops through Excel files in alphabetical order (which sorts by year).
    for excel_file in sorted(os.listdir(folder_path)):
        full_excel_path = os.path.join(folder_path, excel_file)

        try:
            df_loaded = pd.read_csv(full_excel_path, header=0, names=custom_headers)

            # Append the loaded DataFrame to our list.
            all_footfall_dfs.append(df_loaded)

        except Exception as e:
            print(f"  Error reading or processing {excel_file}: {e}. Skipping.")

    # Concatenate all DataFrames in the list into a single DataFrame.
    df_station_footfall = pd.concat(all_footfall_dfs, ignore_index=True)

    # Convert 'date' column to datetime objects
    df_station_footfall["date"] = pd.to_datetime(
        df_station_footfall["date"], format="%Y%m%d"
    )

    # Calculate total station count (entries + exits)
    df_station_footfall["total_count"] = (
        df_station_footfall["entries"] + df_station_footfall["exits"]
    )

    print("\nDataFrame head after date conversion and total_count calculation:")
    print(df_station_footfall.head())
    print("\nDataFrame info (after processing):")
    df_station_footfall.info()
    print("-" * 50)

    return df_station_footfall


def plot_station_footfall(df_all, stations):

    # --- Plotting Station Count vs. Date for Specified Stations ---

    plt.figure(figsize=(15, 8))  # Set a larger figure size for better readability

    for station in stations:
        # Filter the DataFrame for the current station
        df_station = df_all[df_all["station"] == station]

        # Plot 'total_count' vs 'date' for this station
        # Use 'label' for the legend
        plt.plot(
            df_station["date"],
            df_station["total_count"],
            marker="o",
            linestyle="-",
            label=station,
        )

    plt.xlabel("Date")  # X-axis label
    plt.ylabel("Total Passenger Count (Entries + Exits)")  # Y-axis label
    plt.title("Total Passenger Count Over Time for Each Station")  # Plot title

    # Add a legend to identify each station's line.
    # bbox_to_anchor places the legend outside the plot area to avoid overlap.
    # loc='center left' and 'bbox_to_anchor=(1, 0.5)' puts it to the right, centered vertically.
    plt.legend(
        title="Station",
        bbox_to_anchor=(1.02, 0.5),
        loc="center left",
        borderaxespad=0.0,
    )

    plt.grid(True, linestyle="--", alpha=0.6)  # Add a grid for readability
    plt.tight_layout(rect=[0, 0, 0.88, 1])  # Adjust layout to make space for the legend

    # Save the plot to a file
    plot_filename = "station_total_count_vs_date.png"
    plt.savefig(plot_filename)
    print(f"\nPlot saved as '{plot_filename}'")

    # Display the plot (optional, will show in a pop-up window if not in interactive mode)
    plt.show()


def combine_station_ids_and_footfall(df_station_info, df_station_footfall):

    # --- Apply the cleaning function to create temporary merge columns ---

    # Create a new column in df_station_info for merging
    df_station_info["merge_key"] = df_station_info["station"].apply(
        clean_station_name_for_merge
    )

    # Create a new column in df_footfall_baseline for merging
    df_station_footfall["merge_key"] = df_station_footfall["station"].apply(
        clean_station_name_for_merge
    )

    # Remove the station name from footfall dataframe.
    # We will only keep the station name from the station info dataframe since this is the name from the API
    df_station_footfall = df_station_footfall.drop(columns=["station"])

    # --- Create DataFrame with Station Name and Footfall Baseline (i.e., Max Footfall) ---

    df_footfall_baseline = (
        df_station_footfall.groupby("merge_key")["total_count"].max().reset_index()
    )
    df_footfall_baseline = df_footfall_baseline.rename(
        columns={"total_count": "footfall_baseline"}
    )

    # --- Merge df_footfall_baseline into df_station_info DataFrame. ---

    # Assumes 'station' column is the common key for merging.
    df_stations = pd.merge(
        df_station_info, df_footfall_baseline, on="merge_key", how="left"
    )
    # Remove "Paddington (H&C Line)-Underground" station, since no footfall data available for this station
    df_stations = df_stations[
        ~(df_stations["station"] == "Paddington (H&C Line)-Underground")
    ]
    df_stations = df_stations.drop(columns=["merge_key", "station", "lat", "lon"])

    print("\nDataFrame with Station IDs and Maximum Total Footfall Count:")
    print(df_stations)

    return df_stations


def clean_station_name_for_merge(name):
    """
    Cleans a station name by removing non-alphanumeric characters (except spaces)
    and standardizing whitespace. Converts to lowercase.
    """
    if pd.isna(name):  # Handle NaN values
        return name

    # Change names from footfall data to match names from station info data (from API)
    if name == "Edgware Road B":
        name = "Edgware Road (Bakerloo)"
    if name == "Edgware Road C&H":
        name = "Edgware Road (Circle Line)"
    if name == "Heathrow Terminals 2&3":
        name = "Heathrow Terminals 2 & 3"
    if name == "Hammersmith C&H":
        name = "Hammersmith (H&C Line)"
    if name == "Hammersmith D&P":
        name = "Hammersmith (Dist&Picc Line)"
    if name == "Shepherds Bush":
        name = "Shepherd's Bush (Central)"
    if name == "Watford Met":
        name = "Watford"

    name = str(name).lower()  # Convert to string and lowercase
    # Remove any characters that are not letters, numbers, or spaces
    name = re.sub(r"[^a-z0-9\s]", "", name)
    # Replace multiple spaces with a single space and strip leading/trailing spaces
    name = re.sub(r"\s+", " ", name).strip()

    return name


# --- Main Execution ---
if __name__ == "__main__":

    # Loads station info mapping as base DataFrame.
    df_station_info = load_station_info(FILE_PATH_STATION_INFO)

    get_network_demand_files(
        S3_BUCKET_API_BASE_URL,
        TARGET_S3_PREFIX,
        TARGET_FILENAME_PREFIX,
        DOWNLOAD_FOLDER,
    )

    df_station_footfall = make_station_footfall_dataframe(DOWNLOAD_FOLDER)

    # plot_station_footfall(df_station_footfall, stations=["Barons Court"])

    df_stations = combine_station_ids_and_footfall(df_station_info, df_station_footfall)

# # --- Data Saving Phase ---

# Saves the final combined DataFrame to an Excel file (without DataFrame index).
print(f"Saving df_stations to '{FILE_PATH_FOOTFALL_BASELINE}'...")
df_stations.to_excel(FILE_PATH_FOOTFALL_BASELINE, index=False)

print("Data combination and saving process completed.")
print("\n--- Script Execution Finished ---")
