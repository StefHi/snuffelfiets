# CKAN_PM25_DataExtractor

This script, **CKAN_PM25_DataExtractor**, automates data extraction from a CKAN API, filtering records within specific date ranges and GeoJSON-defined geographical areas, and exporting the data in both CSV and Excel formats. Additionally, it calculates and records daily and overall PM2.5 statistics for each area and time period, saving them in an organized Excel summary.

## Features

- **API Data Extraction**: Retrieves large datasets from the CKAN platform, supports pagination, and caches results locally for efficiency.
- **GeoJSON Filtering**: Filters data based on geographic boundaries defined in GeoJSON files.
- **Date-Based Filtering**: Applies specific date ranges to narrow down data extraction.
- **PM2.5 Statistical Analysis**: Computes daily mean and median values for PM2.5 measurements, with a summary of statistics for each date range and geographical area.
- **Data Export**: Saves filtered data as separate CSV files for each date range and location. Combines all data into a single Excel file with separate sheets for each area and a summary sheet.

## Prerequisites

1. **Python 3.7 or higher**
2. **Required Libraries**:
   - `requests`
   - `pandas`
   - `geopandas`
   - `shapely`
   - `dotenv`
   - `xlsxwriter`

 ## Environment Variables

- **CKAN_RESOURCE_ID**: The resource ID for the CKAN dataset.
- **CKAN_API_TOKEN**: Your CKAN API token.

These variables should be saved in a `.env` file in the project root.