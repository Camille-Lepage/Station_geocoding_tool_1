import pandas as pd
import requests
import time
import os
import pycountry
from urllib.parse import quote

def get_coordinates_with_nominatim(input_df, output_file, country=None, name_column='remote_name', city_column='remote_city_name', progress_callback=None):
    """
    Get GPS coordinates for each location from the DataFrame using Nominatim (OpenStreetMap).

    Parameters:
    input_df (DataFrame): DataFrame containing locations
    output_file (str): Path to save the results
    country (str, optional): Country name to use for geocoding, can be None for global search
    name_column (str): Column name containing location names
    city_column (str): Column name containing city names
    progress_callback (function, optional): Callback function to update progress

    Returns:
    DataFrame: Original DataFrame with added coordinates
    """
    # Nominatim API endpoint
    nominatim_endpoint = "https://nominatim.openstreetmap.org/search"
    
    # Set user-agent for API request (required by Nominatim)
    headers = {
        'User-Agent': 'StationGeocodingTool/1.0',
    }

    # Create a copy of the input DataFrame
    result_df = input_df.copy()
    
    # Add new columns for coordinates and address
    if 'Lat' not in result_df.columns:
        result_df['Lat'] = None
    if 'Lng' not in result_df.columns:
        result_df['Lng'] = None
    if 'Address' not in result_df.columns:
        result_df['Address'] = None
    if 'OSM_ID' not in result_df.columns:
        result_df['OSM_ID'] = None
    if 'OSM_Type' not in result_df.columns:
        result_df['OSM_Type'] = None
    
    # Check if results file already exists to resume processing
    processed_indices = set()
    if os.path.exists(output_file):
        try:
            existing_results = pd.read_csv(output_file)
            # Check if the existing results have the required columns
            required_columns = ['Lat', 'Lng', 'Address', name_column]
            if all(col in existing_results.columns for col in required_columns):
                # Map the existing results to our result DataFrame
                for idx, row in existing_results.iterrows():
                    if pd.notna(row['Lat']) and pd.notna(row['Lng']):
                        # Find matching rows in our result_df
                        matching_rows = result_df[result_df[name_column] == row[name_column]]
                        if not matching_rows.empty:
                            for match_idx in matching_rows.index:
                                result_df.at[match_idx, 'Lat'] = row['Lat']
                                result_df.at[match_idx, 'Lng'] = row['Lng']
                                result_df.at[match_idx, 'Address'] = row['Address']
                                if 'OSM_ID' in existing_results.columns:
                                    result_df.at[match_idx, 'OSM_ID'] = row['OSM_ID']
                                if 'OSM_Type' in existing_results.columns:
                                    result_df.at[match_idx, 'OSM_Type'] = row['OSM_Type']
                                processed_indices.add(match_idx)
                
                print(f"Resuming process: {len(processed_indices)} locations already processed.")
        except Exception as e:
            print(f"Error reading existing file: {e}")

    # Filter to process only locations not yet processed
    rows_to_process = [idx for idx in result_df.index if idx not in processed_indices]
    
    print(f"Number of locations to process: {len(rows_to_process)}")

    # Process each location
    for i, idx in enumerate(rows_to_process):
        row = result_df.loc[idx]
        
        # Get location name and city
        location_name = row[name_column] if pd.notna(row[name_column]) else ""
        city_name = row[city_column] if city_column in result_df.columns and pd.notna(row[city_column]) else ""
        
        # Create query string by combining location name and city
        if city_name:
            query = f"{location_name}, {city_name}"
        else:
            query = location_name
            
        # Add country if specified
        if country:
            query += f", {country}"
            
        try:
            print(f"[{i+1}/{len(rows_to_process)}] Processing: {query}")
            
            # Prepare API parameters
            params = {
                'q': query,
                'format': 'json',
                'limit': 1,  # Get only the best match
                'addressdetails': 1  # Include address details
            }
            
            # Add country code if provided (Nominatim prefers country codes)
            if country:
                try:
                    # Try to get country code from name
                    countries = list(pycountry.countries.search_fuzzy(country))
                    if countries:
                        params['countrycodes'] = countries[0].alpha_2.lower()
                except:
                    # If country code can't be determined, use country name as is
                    pass
            
            # Make the API request
            response = requests.get(nominatim_endpoint, params=params, headers=headers)
            
            if response.status_code == 200:
                results = response.json()
                
                if results:
                    # Get the best match (first result)
                    best_match = results[0]
                    
                    # Extract coordinates and address
                    lat = float(best_match['lat'])
                    lng = float(best_match['lon'])
                    display_name = best_match['display_name']
                    
                    # Get OSM details
                    osm_id = best_match.get('osm_id')
                    osm_type = best_match.get('osm_type')
                    
                    # Update the result DataFrame
                    result_df.at[idx, 'Lat'] = lat
                    result_df.at[idx, 'Lng'] = lng
                    result_df.at[idx, 'Address'] = display_name
                    result_df.at[idx, 'OSM_ID'] = osm_id
                    result_df.at[idx, 'OSM_Type'] = osm_type
                else:
                    print(f"No results found for: {query}")
            else:
                print(f"API error ({response.status_code}) for: {query}")
                
            # Save progress periodically
            if (i + 1) % 10 == 0 or i == len(rows_to_process) - 1:
                result_df.to_csv(output_file, index=False)
                print(f"Intermediate save: {i+1} locations processed")

            # Pause to respect Nominatim's usage policy (max 1 request per second)
            time.sleep(1.1)  # Slightly more than 1 second to be safe

            # Update progress bar if callback is provided
            if progress_callback:
                progress_callback(i + 1, len(rows_to_process))

        except Exception as e:
            print(f"Error processing {query}: {e}")
            
            # Save in case of error
            result_df.to_csv(output_file, index=False)

            # Wait a bit longer in case of error
            time.sleep(2)

            # Update progress bar on error as well
            if progress_callback:
                progress_callback(i + 1, len(rows_to_process))

    # Save final result
    result_df.to_csv(output_file, index=False)
    print(f"Processing completed! Results saved to {output_file}")

    return result_df