import pandas as pd
import json
import os

# --- Configuration ---
# Ensure these file names match your actual CSV files in the same directory
STATION_INFO_FILE = "data/station_info.xlsx"
BASELINE_FOOTFALL_FILE = "data/stations_baseline_footfall.xlsx"
LIVE_CROWDING_FILE = "data/stations_live_crowding.xlsx"
OUTPUT_HTML_FILE = "index.html"  # The name of the generated HTML file


def generate_heatmap_dashboard():
    """
    Loads station data from CSVs, processes it, and generates a
    self-contained HTML heatmap dashboard with the data embedded.
    """
    print("Starting data processing and HTML generation...")

    # --- 1. Data Loading and Processing ---
    # Check if files exist
    for f_name in [STATION_INFO_FILE, BASELINE_FOOTFALL_FILE, LIVE_CROWDING_FILE]:
        if not os.path.exists(f_name):
            print(f"Error: File not found: {f_name}.")
            print(
                "Please ensure all Excel files are in the correct 'data/' subdirectory relative to this script."
            )
            return

    try:
        # Changed to pd.read_excel as per user's request
        station_info_df = pd.read_excel(STATION_INFO_FILE)
        baseline_footfall_df = pd.read_excel(BASELINE_FOOTFALL_FILE)
        live_crowding_df = pd.read_excel(LIVE_CROWDING_FILE)
        print("Excel files loaded successfully.")

        # Convert timestamp to datetime objects and take the latest entry for each station
        if "timestamp" in live_crowding_df.columns:
            live_crowding_df["timestamp"] = pd.to_datetime(
                live_crowding_df["timestamp"]
            )
            live_crowding_df = live_crowding_df.sort_values(
                by="timestamp"
            ).drop_duplicates(subset="stop_id", keep="last")

        # Merge station_info and live_crowding first
        final_df = pd.merge(station_info_df, live_crowding_df, on="stop_id", how="left")

        # Explicitly convert lat and lon to numeric, coercing errors to NaN
        final_df["lat"] = pd.to_numeric(final_df["lat"], errors="coerce")
        final_df["lon"] = pd.to_numeric(final_df["lon"], errors="coerce")

        # Calculate crowding metric: live_footfall / max(footfall_baseline)
        # Using the maximum footfall baseline from the baseline_footfall_df
        max_baseline_footfall = baseline_footfall_df["footfall_baseline"].max()
        print(f"Maximum baseline footfall: {max_baseline_footfall}")

        # Handle case where max_baseline_footfall might be zero to avoid division by zero
        if max_baseline_footfall == 0:
            final_df["crowding_metric"] = 0
        else:
            final_df["crowding_metric"] = (
                final_df["live_footfall"] / max_baseline_footfall
            )

        # Replace infinite values (due to division by zero) and NaN with 0 for heatmap compatibility
        final_df["crowding_metric"] = (
            final_df["crowding_metric"]
            .replace([float("inf"), -float("inf")], pd.NA)
            .fillna(0)
        )

        # Prepare data for the frontend: filter out rows with missing lat/lon, crowding_metric or station name
        # .dropna() will now reliably remove rows where lat/lon became NaN due to coercion
        heatmap_data_for_js = (
            final_df[["lat", "lon", "crowding_metric", "station"]]
            .dropna()
            .values.tolist()
        )

        # Convert the processed data to a JSON string
        processed_data_json = json.dumps(heatmap_data_for_js, indent=2)
        print(
            f"Data processing complete. {len(heatmap_data_for_js)} station entries processed."
        )

    except Exception as e:
        print(f"An error occurred during data processing: {e}")
        print("Generating HTML with empty data array.")
        processed_data_json = "[]"  # Fallback to empty array if data processing fails

    # --- 2. HTML Template ---
    html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>London Tube Station Heatmap</title>
    <!-- Tailwind CSS CDN -->
    <script src="https://cdn.tailwindcss.com"></script>
    <!-- Leaflet CSS -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
        integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
        crossorigin=""/>
    <style>
        /* Custom CSS for map container to ensure it takes full height */
        #map {{
            height: 80vh; /* Set a responsive height for the map */
            width: 100%;
            border-radius: 0.5rem; /* Apply rounded corners */
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); /* Add a subtle shadow */
        }}
        body {{
            font-family: 'Inter', sans-serif; /* Use Inter font */
        }}
    </style>
</head>
<body class="bg-gray-100 p-4 sm:p-6 lg:p-8">
    <div class="container mx-auto p-4 bg-white rounded-lg shadow-xl">
        <h1 class="text-3xl font-bold text-center text-gray-800 mb-4 rounded-md p-2 bg-blue-100">
            London Tube Station Busyness Heatmap
        </h1>
        <p class="text-center text-gray-600 mb-6">
            This heatmap visualizes the relative busyness of London tube stations based on live footfall data compared to baseline.
            Higher intensity (redder areas) indicates more crowding.
        </p>

        <!-- Map container -->
        <div id="map" class="mb-6"></div>

        <div class="text-center text-gray-500 text-sm">
            <p>Data Source: Processed data from your provided CSV files.</p>
            <p>Map tiles by <a href="https://www.openstreetmap.org/copyright" target="_blank" class="text-blue-500 hover:underline">OpenStreetMap</a> contributors.</p>
        </div>
    </div>

    <!-- Leaflet JS -->
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
        integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
        crossorigin=""></script>
    <!-- Leaflet.heat JS for heatmap -->
    <script src="https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js"></script>

    <script>
        // Embed the processed station data directly into the HTML
        // This data was generated by the Python script from your CSV files.
        const stationData = JSON.parse(`{processed_data_json}`);

        // Define the URL for your GeoJSON tube lines file
        // IMPORTANT: Place your 'TubeLines.geojson' file in a 'data/' folder
        // in your GitHub repository alongside your 'index.html' file.
        const tubeLinesGeoJSONUrl = 'https://github.com/isi22/London-Crowds-Heatmap/blob/main/data/TubeLines.geojson';

        // Ensure all scripts and the DOM are loaded before initializing the map
        window.onload = function() {{
            // Filter out any entries that are not arrays, or don't have enough elements,
            // or have non-numeric/NaN lat/lon values at the very first step.
            const cleanStationData = stationData.filter(entry =>
                Array.isArray(entry) &&
                entry.length >= 4 &&
                typeof entry[0] === 'number' && !isNaN(entry[0]) && // Check lat
                typeof entry[1] === 'number' && !isNaN(entry[1])    // Check lon
            );

            console.log("Cleaned stationData for map:", cleanStationData); // For debugging

            // Initialize the map
            // Centered on London (approximate), zoom level adjusted to show most stations
            const map = L.map('map').setView([51.505, -0.09], 11);

            // Add CartoDB Positron tiles (light map)
            L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}.png', {{
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
            }}).addTo(map);

            // Fetch and add Tube Lines GeoJSON
            fetch(tubeLinesGeoJSONUrl)
                .then(response => {{
                    if (!response.ok) {{
                        throw new Error(`HTTP error! status: ${{response.status}}`);
                    }}
                    return response.json();
                }})
                .then(geojsonData => {{
                    L.geoJson(geojsonData, {{
                        style: function(feature) {{
                            // You can style lines based on properties in your GeoJSON
                            // For example, if your GeoJSON has a 'line_name' property:
                            switch (feature.properties.line_name) {{
                                case 'Central': return {{color: '#DC241F', weight: 3}};
                                case 'Victoria': return {{color: '#00A0E2', weight: 3}};
                                case 'Jubilee': return {{color: '#868F98', weight: 3}};
                                case 'Northern': return {{color: '#000000', weight: 3}};
                                case 'Piccadilly': return {{color: '#0019A8', weight: 3}};
                                case 'Bakerloo': return {{color: '#B36305', weight: 3}};
                                case 'District': return {{color: '#007229', weight: 3}};
                                case 'Hammersmith & City': return {{color: '#F4A9BE', weight: 3}};
                                case 'Metropolitan': return {{color: '#751056', weight: 3}};
                                case 'Circle': return {{color: '#FFD300', weight: 3}};
                                case 'Waterloo & City': return {{color: '#76D0BD', weight: 3}};
                                case 'DLR': return {{color: '#00BFB3', weight: 3}}; // DLR specific color
                                case 'Overground': return {{color: '#EE7800', weight: 3}}; // Overground specific color
                                case 'Elizabeth line': return {{color: '#6950A1', weight: 3}}; // Elizabeth line specific color
                                default: return {{color: '#888888', weight: 2}}; // Default for other lines
                            }}
                        }}
                    }}).addTo(map);
                    console.log("Tube lines GeoJSON loaded.");
                }})
                .catch(error => console.error('Error loading tube lines GeoJSON:', error));


            // Prepare data for heatmap: Leaflet.heat requires data in [lat, lon, intensity] format
            // The 'intensity' here is our 'crowding_metric'.
            const heatData = cleanStationData.map(station => {{
                const lat = station[0];
                const lon = station[1];
                let crowdingMetric = station[2];

                // Ensure crowdingMetric is a valid number, default to 0 if not
                if (typeof crowdingMetric !== 'number' || isNaN(crowdingMetric)) {{
                    crowdingMetric = 0;
                }}

                return [lat, lon, crowdingMetric]; // Leaflet.heat only needs [lat, lon, intensity]
            }});


            // Add heatmap layer
            // Adjust radius, blur, and gradient for better visualization.
            // Max intensity is set to the maximum crowding metric found in the data,
            // or a default if all are 0, to scale the heatmap colors effectively.
            const maxCrowding = Math.max(...heatData.map(d => d[2]));
            // To increase intensity, we make the 'max' value smaller,
            // so lower crowding metrics reach the 'red' part of the gradient sooner.
            // Adjust the factor (e.g., 0.75, 0.5) to control how much more intense it appears.
            const heatMax = maxCrowding > 0 ? maxCrowding * 0.75 : 1; // Adjusted factor for intensity

            L.heatLayer(heatData, {{
                radius: 25,   // Radius of the individual heat points
                blur: 15,     // Amount of blur to apply
                // maxZoom: 13,  // REMOVED: To allow intensity to scale with zoom
                max: heatMax, // Max intensity value for color scaling
                gradient: {{
                    0.0: '#FFFFCC', // Very light yellow/off-white
                    0.2: '#FFEDA0', // Light yellow
                    0.4: '#FED976', // Muted yellow-orange
                    0.6: '#FEB24C', // Orange
                    0.8: '#FD8D3C', // Darker orange
                    1.0: '#FC4E2A'  // Red-orange
                }}
            }}).addTo(map);

            // Add invisible circle markers for individual stations with popups for detailed info on click
            cleanStationData.forEach(station => {{
                const lat = station[0];
                const lon = station[1];
                const crowdingMetric = station[2];
                const stationName = station[3];

                L.circleMarker([lat, lon], {{
                    radius: 8,          // Increased radius to make them more clickable
                    fillOpacity: 0,     // Completely transparent fill
                    stroke: false,      // No border
                    interactive: true   // Crucial for popups to work on click
                }})
                .bindPopup(`<b>Station:</b> ${{stationName}}<br><b>Crowding Metric:</b> ${{crowdingMetric.toFixed(2)}}`)
                .addTo(map);
            }});

            // Adjust map view on window resize to ensure responsiveness
            window.addEventListener('resize', () => {{
                map.invalidateSize();
            }});
        }}; // End of window.onload
    </script>
</body>
</html>
    """

    # --- 3. Save the HTML file ---
    try:
        with open(OUTPUT_HTML_FILE, "w") as f:
            f.write(html_template)
        print(f"\nSuccessfully generated '{OUTPUT_HTML_FILE}'!")
        print(
            f"You can now open '{OUTPUT_HTML_FILE}' in your web browser or host it on GitHub Pages."
        )
    except Exception as e:
        print(f"Error saving HTML file: {e}")


if __name__ == "__main__":
    generate_heatmap_dashboard()
