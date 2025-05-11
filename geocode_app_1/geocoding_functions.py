import pandas as pd
import googlemaps
import time
import os
import pycountry

def get_coordinates_for_locations(input_df, output_file, api_key, country=None, name_column='remote_name', city_column='remote_city_name', progress_callback=None):
    """
    Get GPS coordinates for each location from the DataFrame.

    Parameters:
    input_df (DataFrame): DataFrame containing locations
    output_file (str): Path to save the results
    api_key (str): Google Maps API key
    country (str, optional): Country name to use for geocoding, can be None for global search
    name_column (str): Column name containing location names
    city_column (str): Column name containing city names
    progress_callback (function, optional): Callback function to update progress, should accept two arguments (current, total)

    Returns:
    DataFrame: Original DataFrame with added coordinates
    """
    # Initialize Google Maps client
    gmaps = googlemaps.Client(key=api_key)

    # Create a copy of the input DataFrame
    result_df = input_df.copy()
    
    # Add new columns for coordinates and address
    if 'Lat' not in result_df.columns:
        result_df['Lat'] = None
    if 'Lng' not in result_df.columns:
        result_df['Lng'] = None
    if 'Address' not in result_df.columns:
        result_df['Address'] = None
    
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
            
            # FIX: Use the Google Maps geocoding API correctly
            # First, try with component filtering for locality
            geocode_result = gmaps.geocode(
                query,
                components={"locality": location_name}
            )
            
            if geocode_result:
                location = geocode_result[0]['geometry']['location']
                lat, lng = location['lat'], location['lng']
                
                # Get the formatted address
                formatted_address = geocode_result[0]['formatted_address']
                
                # Update the result DataFrame
                result_df.at[idx, 'Lat'] = lat
                result_df.at[idx, 'Lng'] = lng
                result_df.at[idx, 'Address'] = formatted_address
            else:
                # If no results with locality component, try without filtering
                geocode_result = gmaps.geocode(query)
                
                if geocode_result:
                    location = geocode_result[0]['geometry']['location']
                    lat, lng = location['lat'], location['lng']
                    
                    # Get the formatted address
                    formatted_address = geocode_result[0]['formatted_address']
                    
                    # Update the result DataFrame
                    result_df.at[idx, 'Lat'] = lat
                    result_df.at[idx, 'Lng'] = lng
                    result_df.at[idx, 'Address'] = formatted_address
                else:
                    print(f"No results found for: {query}")
            
            # Save progressively
            if (i + 1) % 10 == 0 or i == len(rows_to_process) - 1:
                result_df.to_csv(output_file, index=False)
                print(f"Intermediate save completed: {i+1} locations processed")

            # Pause to respect API limits
            time.sleep(0.3)

            # Update progress bar if callback is provided
            if progress_callback:
                progress_callback(i + 1, len(rows_to_process))

        except Exception as e:
            print(f"Error processing {query}: {e}")
            
            # Save in case of error
            result_df.to_csv(output_file, index=False)

            # Wait a bit longer in case of error (might be API limit)
            time.sleep(2)

            # Update progress bar on error as well
            if progress_callback:
                progress_callback(i + 1, len(rows_to_process))

    # Save final result
    result_df.to_csv(output_file, index=False)
    print(f"Processing completed! Results saved to {output_file}")

    return result_df

def filter_invalid_results(df, country=None):
    """
    Filter out incorrect results where the address only contains the country name.
    
    Parameters:
    df (DataFrame): DataFrame containing geocoding results
    country (str, optional): Country name used for geocoding
    
    Returns:
    DataFrame: Filtered DataFrame without invalid results
    """
    filtered_df = df.copy()
    
    # Only process rows that have an address
    mask = filtered_df['Address'].notna()
    
    # Create a list of potential country-only matches to filter out
    country_terms = []
    if country:
        country_terms.append(country.strip().lower())
        
    # Add countries from pycountry
    for pycountry_country in pycountry.countries:
        country_terms.append(pycountry_country.name.lower())
        if hasattr(pycountry_country, 'official_name') and pycountry_country.official_name:
            country_terms.append(pycountry_country.official_name.lower())
        if pycountry_country.alpha_2:
            country_terms.append(pycountry_country.alpha_2.lower())
        if pycountry_country.alpha_3:
            country_terms.append(pycountry_country.alpha_3.lower())
    
    # Filter out results where the address is just the country
    for idx in filtered_df[mask].index:
        address = str(filtered_df.at[idx, 'Address']).strip().lower()
        
        # Check if address only contains country information
        if any(address == country_term for country_term in country_terms):
            print(f"Invalid result detected - Address is just country name: {filtered_df.at[idx, 'Address']}")
            filtered_df.at[idx, 'Lat'] = None
            filtered_df.at[idx, 'Lng'] = None
            filtered_df.at[idx, 'Address'] = None
    
    return filtered_df

def find_potential_errors(df, name_column='remote_name', threshold=5):
    """
    Find potential errors where multiple different stations have the same coordinates or address.
    
    Parameters:
    df (DataFrame): DataFrame containing geocoding results
    name_column (str): Column name containing location names
    threshold (int): Minimum number of occurrences to flag as potential error
    
    Returns:
    dict: Dictionary with duplicate coordinates and addresses
    """
    potential_errors = {
        'duplicate_coordinates': [],
        'duplicate_addresses': []
    }
    
    # Only consider rows with coordinates and addresses
    valid_df = df[(df['Lat'].notna()) & (df['Lng'].notna())].copy()
    
    if valid_df.empty:
        return potential_errors
    
    # Create a combined coordinate column for easier grouping
    valid_df['coord_key'] = valid_df['Lat'].astype(str) + ',' + valid_df['Lng'].astype(str)
    
    # Find duplicate coordinates
    coord_counts = valid_df.groupby('coord_key')[name_column].apply(list).reset_index()
    for _, row in coord_counts.iterrows():
        unique_names = set(row[name_column])
        if len(unique_names) >= threshold:
            # Get sample data for these coordinates
            sample_data = valid_df[valid_df['coord_key'] == row['coord_key']].drop_duplicates(subset=[name_column])
            sample_data = sample_data[[name_column, 'Lat', 'Lng', 'Address']].head(10).to_dict('records')
            
            potential_errors['duplicate_coordinates'].append({
                'coordinates': row['coord_key'],
                'count': len(unique_names),
                'sample_data': sample_data
            })
    
    # Find duplicate addresses (excluding nulls)
    valid_address_df = valid_df[valid_df['Address'].notna()]
    if not valid_address_df.empty:
        address_counts = valid_address_df.groupby('Address')[name_column].apply(list).reset_index()
        for _, row in address_counts.iterrows():
            unique_names = set(row[name_column])
            if len(unique_names) >= threshold:
                # Get sample data for this address
                sample_data = valid_address_df[valid_address_df['Address'] == row['Address']].drop_duplicates(subset=[name_column])
                sample_data = sample_data[[name_column, 'Lat', 'Lng', 'Address']].head(10).to_dict('records')
                
                potential_errors['duplicate_addresses'].append({
                    'address': row['Address'],
                    'count': len(unique_names),
                    'sample_data': sample_data
                })
    
    return potential_errors

def display_summary(result_df, country=None, name_column='remote_name'):
    """
    Display a summary of the geocoding results.
    
    Parameters:
    result_df (DataFrame): DataFrame containing the geocoding results
    country (str, optional): Country name used for geocoding
    name_column (str): Column name containing location names
    
    Returns:
    dict: Dictionary containing summary statistics and potential errors
    """
    # First, filter out invalid results (where address is just the country name)
    filtered_df = filter_invalid_results(result_df, country)
    
    # Find potential errors (duplicates)
    potential_errors = find_potential_errors(filtered_df, name_column)
    
    # Create a basic summary
    total_locations = len(filtered_df)
    locations_with_coordinates = filtered_df['Lat'].notna().sum()
    locations_without_coordinates = total_locations - locations_with_coordinates
    
    # Calculate the percentage of successful geocoding
    success_rate = (locations_with_coordinates / total_locations) * 100 if total_locations > 0 else 0
    
    # Get summary of filtered results
    filtered_count = len(result_df) - len(filtered_df[filtered_df['Lat'].notna()])
    
    summary = {
        "total_locations": total_locations,
        "locations_with_coordinates": locations_with_coordinates,
        "locations_without_coordinates": locations_without_coordinates,
        "success_rate": success_rate,
        "filtered_results": filtered_count,
        "potential_errors": potential_errors,
        "sample_results": filtered_df.head(5).to_dict('records')
    }
    
    return summary, filtered_df