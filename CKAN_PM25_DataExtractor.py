import requests
import pandas as pd
import os
from dotenv import load_dotenv
import geopandas as gpd
from shapely.geometry import shape, Point
import datetime
import traceback
import json

try:
    # Load environment variables from the .env file
    load_dotenv()

    # Define your CKAN API SQL URL, resource ID, and API token
    ckan_sql_url = "https://ckan.dataplatform.nl/api/3/action/datastore_search_sql"
    resource_id = os.getenv("CKAN_RESOURCE_ID")
    api_token = os.getenv("CKAN_API_TOKEN")

    if not resource_id or not api_token:
        print("CKAN_RESOURCE_ID or CKAN_API_TOKEN not found in environment variables.")
        exit(1)

    # Set date ranges for filtering and use in file names
    date_ranges = [
        ("2022-04-01", "2022-05-31"),
        ("2023-04-01", "2023-05-31"),
        ("2024-04-01", "2024-05-31"),
    ]

    # Name of the data collection or project
    name = "highfive_area"

    # Define paths to the GeoJSON files
    geojson_files = {
        "highfive_area": "data/highfive_area.geojson",
    }

    # Check if GeoJSON files exist
    for geojson_path in geojson_files.values():
        if not os.path.isfile(geojson_path):
            print(f"GeoJSON file not found: {geojson_path}")
            exit(1)

    # Set up the headers, including the API token
    headers = {"Authorization": api_token}

    # File paths for the output files
    output_folder = "data3"
    excel_file_name = f"{name}_data_multiple_date_ranges_within_geojson_areas.xlsx"
    excel_file_path = os.path.join(output_folder, excel_file_name)

    # Create the folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Initialize a list to store stats for each date range and area for summary tab
    all_stats_sections = []

    # Function to sanitize sheet names
    def sanitize_sheet_name(name):
        invalid_chars = ["\\", "/", "*", "[", "]", ":", "?", "'", " "]
        for char in invalid_chars:
            name = name.replace(char, "")
        return name[:31]  # Truncate to 31 characters

    # Write to a single Excel file
    with pd.ExcelWriter(excel_file_path, engine="xlsxwriter") as writer:
        # Iterate over each date range
        for start_date, end_date in date_ranges:
            try:
                # Define cache file path
                cache_file_name = f"api_cache_{start_date}_to_{end_date}.json"
                cache_file_path = os.path.join(output_folder, cache_file_name)

                # Check if cache file exists
                if os.path.isfile(cache_file_path):
                    print(f"Loading data from cache file {cache_file_path}")
                    with open(cache_file_path, "r") as f:
                        all_records = json.load(f)
                else:
                    # Reset pagination variables
                    limit = 10000
                    offset = 0
                    all_records = []

                    # Fetch data for the current date range
                    while True:
                        sql_query = f"""
                        SELECT entity_id, recording_timestamp, acc_max, error_code, 
                               horizontal_accuracy, humidity, latitude, longitude, 
                               pm2_5, pressure, temperature, 
                               vertical_accuracy, voc, voltage, version_major 
                        FROM "{resource_id}" 
                        WHERE recording_timestamp >= '{start_date}' AND recording_timestamp <= '{end_date}'
                        ORDER BY recording_timestamp DESC
                        LIMIT {limit} OFFSET {offset}
                        """

                        params = {"sql": sql_query}
                        try:
                            response = requests.get(
                                ckan_sql_url, params=params, headers=headers
                            )
                            response.raise_for_status()
                        except requests.exceptions.HTTPError as http_err:
                            print(f"HTTP error occurred: {http_err}")
                            print(f"Response content: {response.text}")
                            break
                        except Exception as err:
                            print(f"An error occurred during data fetching: {err}")
                            traceback.print_exc()
                            break

                        print(
                            f"Fetching data with offset {offset}... Status: {response.status_code}"
                        )

                        data = response.json()
                        records = data.get("result", {}).get("records", [])
                        if not records:
                            break
                        all_records.extend(records)
                        offset += limit

                    # Save the fetched data to cache file
                    with open(cache_file_path, "w") as f:
                        json.dump(all_records, f)
                    print(f"Data cached to {cache_file_path}")

                # Convert all records for the date range to a DataFrame
                df = pd.DataFrame(all_records) if all_records else pd.DataFrame()

                # Apply filtering conditions
                if not df.empty and "pm2_5" in df:
                    df = df[(df["pm2_5"] > 0) & (df["pm2_5"] <= 10000)]
                if not df.empty and "version_major" in df and "pm2_5" in df:
                    df.loc[df["version_major"] == "1", "pm2_5"] *= 100

                if df.empty:
                    print(f"No data found for date range {start_date} to {end_date}")
                    continue  # Skip to next date range if no data

                df["geometry"] = [
                    Point(xy) for xy in zip(df["longitude"], df["latitude"])
                ]
                gdf = gpd.GeoDataFrame(df, geometry="geometry")

                # Process each location within the date range
                for location_name, geojson_path in geojson_files.items():
                    try:
                        geojson_data = gpd.read_file(geojson_path)
                    except Exception as e:
                        print(f"Error reading GeoJSON file {geojson_path}: {e}")
                        traceback.print_exc()
                        continue  # Skip to the next location

                    polygon = shape(geojson_data["geometry"][0])

                    gdf_within_geojson = gdf[gdf.within(polygon)].copy()
                    gdf_within_geojson["recording_timestamp"] = pd.to_datetime(
                        gdf_within_geojson["recording_timestamp"]
                    )

                    if gdf_within_geojson.empty:
                        print(
                            f"No data for {location_name} in range {start_date} to {end_date}"
                        )
                        continue  # Skip to the next location

                    # Calculate stats using original pm2_5 values
                    try:
                        # Calculate daily stats
                        daily_stats = (
                            gdf_within_geojson.set_index("recording_timestamp")
                            .resample("D")
                            .agg({"pm2_5": ["mean", "median"]})
                            .reset_index()
                        )
                        daily_stats.columns = ["Date", "PM2.5_Average", "PM2.5_Median"]

                        # Add "missing" for days with no data
                        date_range = pd.date_range(
                            start=start_date, end=end_date, freq="D"
                        )
                        daily_stats = (
                            daily_stats.set_index("Date")
                            .reindex(date_range)
                            .reset_index()
                        )
                        daily_stats.columns = ["Date", "PM2.5_Average", "PM2.5_Median"]

                        # Divide pm2_5 stats by 100 and round
                        daily_stats[["PM2.5_Average", "PM2.5_Median"]] /= 100
                        daily_stats[["PM2.5_Average", "PM2.5_Median"]] = daily_stats[
                            ["PM2.5_Average", "PM2.5_Median"]
                        ].round(2)
                        daily_stats[["PM2.5_Average", "PM2.5_Median"]] = daily_stats[
                            ["PM2.5_Average", "PM2.5_Median"]
                        ].fillna("missing")
                    except Exception as e:
                        print(f"Error calculating daily stats for {location_name}: {e}")
                        traceback.print_exc()

                    # Calculate overall stats
                    try:
                        overall_stats = pd.DataFrame(
                            {
                                "Date": ["Overall"],
                                "PM2.5_Average": [
                                    gdf_within_geojson["pm2_5"].mean() / 100
                                ],
                                "PM2.5_Median": [
                                    gdf_within_geojson["pm2_5"].median() / 100
                                ],
                            }
                        )
                        overall_stats = overall_stats.round(2)
                    except Exception as e:
                        print(
                            f"Error calculating overall stats for {location_name}: {e}"
                        )
                        traceback.print_exc()

                    # Combine daily and overall stats
                    stats_df = pd.concat(
                        [daily_stats, overall_stats], ignore_index=True
                    )

                    # Add to all_stats_sections for the summary sheet
                    all_stats_sections.append(
                        (f"{location_name} ({start_date} to {end_date})", stats_df)
                    )

                    # Now, divide pm2_5 by 100 in the data before saving to CSV and Excel
                    try:
                        gdf_within_geojson_to_save = gdf_within_geojson.copy()
                        gdf_within_geojson_to_save["pm2_5"] = (
                            gdf_within_geojson_to_save["pm2_5"] / 100
                        )

                        # Save separate CSV for each location and date range
                        csv_file_name = (
                            f"{location_name}_data_{start_date}_to_{end_date}.csv"
                        )
                        csv_file_path = os.path.join(output_folder, csv_file_name)
                        gdf_within_geojson_to_save.to_csv(csv_file_path, index=False)
                        print(
                            f"All records for {location_name} in range {start_date} to {end_date} saved to {csv_file_path}"
                        )

                        # Write each location and date range as a separate sheet in the Excel file
                        sheet_name = sanitize_sheet_name(
                            f"{location_name}_{start_date}_{end_date}"
                        )
                        gdf_within_geojson_to_save.to_excel(
                            writer, sheet_name=sheet_name, index=False
                        )
                    except Exception as e:
                        print(f"Error saving data for {location_name}: {e}")
                        traceback.print_exc()

            except Exception as e:
                print(
                    f"An error occurred during processing of date range {start_date} to {end_date}: {e}"
                )
                traceback.print_exc()

        # Add a summary sheet with daily stats for all locations and date ranges
        try:
            worksheet = writer.book.add_worksheet("PM25_Stats")
            bold_format = writer.book.add_format({"bold": True})
            date_format = writer.book.add_format({"num_format": "yyyy-mm-dd"})

            start_row = 0
            for location_range, stats_df in all_stats_sections:
                worksheet.write(start_row, 0, location_range, bold_format)

                headers = ["Date", "PM2.5_Average", "PM2.5_Median"]
                for col, header in enumerate(headers):
                    worksheet.write(start_row + 1, col, header)

                for row_idx, data_row in stats_df.iterrows():
                    row = start_row + 2 + row_idx
                    for col, value in enumerate(data_row):
                        if col == 0 and isinstance(
                            value, (pd.Timestamp, datetime.datetime)
                        ):
                            worksheet.write_datetime(row, col, value, date_format)
                        else:
                            if value == "missing":
                                worksheet.write(row, col, value)
                            else:
                                worksheet.write(row, col, value)
                start_row += len(stats_df) + 3  # Space before next section
        except Exception as e:
            print(f"Error creating summary worksheet: {e}")
            traceback.print_exc()

    print(f"Data saved to {excel_file_path}")

except Exception as e:
    print(f"An unexpected error occurred: {e}")
    traceback.print_exc()
    exit(1)
