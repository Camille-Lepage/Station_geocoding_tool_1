import streamlit as st
import pandas as pd
import os
import base64
from datetime import datetime
from geocoding_functions import get_coordinates_for_locations, display_summary
from nominatim_geocoding import get_coordinates_with_nominatim

# Set page config
st.set_page_config(
    page_title="Get Station Coordinates",
    page_icon="🌎",
    layout="wide"
)

# Custom CSS to improve the appearance
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1E88E5;
        margin-bottom: 1rem;
        text-align: center;
    }
    .section-header {
        font-size: 1.5rem;
        font-weight: 600;
        color: #0D47A1;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    .info-text {
        font-size: 1rem;
        color: #424242;
        margin-bottom: 1rem;
    }
    .success-box {
        background-color: #E8F5E9;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 0.5rem solid #43A047;
        margin: 1rem 0;
    }
    .warning-box {
        background-color: #FFF8E1;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 0.5rem solid #FFA000;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #FFEBEE;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 0.5rem solid #E53935;
        margin: 1rem 0;
    }
    .detail-box {
        background-color: #E3F2FD;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .provider-box {
        display: flex;
        align-items: center;
        gap: 1rem;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
        box-shadow: 0 1px 3px rgba(0,0,0,0.12), 0 1px 2px rgba(0,0,0,0.24);
    }
    .provider-box.selected {
        border: 2px solid #1E88E5;
        background-color: #E3F2FD;
    }
    .provider-logo {
        width: 50px;
        height: 50px;
        object-fit: contain;
    }
    .emoji {
        font-size: 1.2rem;
        margin-right: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

# App title and description
st.markdown('<div class="main-header">🌍 Get Station Coordinates Tool</div>', unsafe_allow_html=True)

st.markdown("<br><br>", unsafe_allow_html=True)  # Add even more vertical space

# Add attention message
st.markdown("""
<div class="warning-box">
    <strong>⚠️ Attention:</strong> This tool is not 100% reliable. It is intended to help map stations for integrations that use the "points" system (such as Redbus), or integrations that do not show coordinates and addresses to customers.<br>
    For detailed information on how the tool works, please refer to the <a href="https://one2go.atlassian.net/wiki/x/GwCE-g" target="_blank">Confluence documentation</a>.
</div>
""", unsafe_allow_html=True)

# Create a download link for the results
def get_download_link(df, filename, text):
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">📥 {text}</a>'
    return href

# Sidebar for inputs
with st.sidebar:
    st.markdown('<div class="section-header">⚙️ Configuration</div>', unsafe_allow_html=True)
    
    # Geocoding provider selection
    st.markdown("### Choose a Geocoding Provider")
    
    # Use a single radio button for provider selection
    provider = st.radio(
        "Select a provider:",
        ["Google Maps API", "Nominatim (Free)"],
        index=0 if "provider" not in st.session_state or st.session_state.provider == "google" else 1,
        key="provider_radio"
    )
    
    # Update session state based on selection
    if provider == "Google Maps API":
        st.session_state.provider = "google"
    else:
        st.session_state.provider = "nominatim"
    
    # Provider description
    if st.session_state.provider == "google":
        st.markdown("""
        <div class="info-text">
            <strong>Google Maps API</strong><br>
            ✓ High accuracy<
            ✓ Consistent results<br>
            ❌ Requires an API key<br>
            ❌ Only 3000 free requests per month
        </div>
        """, unsafe_allow_html=True)
        
        # API Key input (only for Google Maps)
        api_key = st.text_input("Google Maps API Key", help="Enter your Google Maps API key")
        
        # Nouvelle option : fallback sans filtre
        fallback_without_location_filter = st.checkbox(
            "If no result, retry search without location filter",
            value=False,
            help="If enabled, when the initial search fails, a new search will be attempted without locality filtering to maximize results, though this may lead to inaccurate matches. This option is particularly useful when your station names correspond to something other than a locality name."
        )
    else:
        st.markdown("""
        <div class="info-text">
            <strong>Nominatim / OpenStreetMap</strong><br>
            ✓ Completely free<br>
            ✓ No API key required<br>
            ❌ Accuracy is really not as good<br>
            ❌ Slower<br>
        </div>
        """, unsafe_allow_html=True)
        api_key = None  # Not needed for Nominatim
        fallback_without_location_filter = False  # Pas utilisé pour Nominatim

        # Option à cocher pour restreindre à settlement
        lock_to_settlement = st.checkbox(
            "Restrict search to locality only",
            value=False,
            help="If enabled, only results classified as settlements (cities, towns, villages, etc.) will be returned by Nominatim. May improve relevance for locality searches."
        )
    
    # Common settings
    st.markdown("### Common Settings")
    
    # Country input (optional)
    country = st.text_input("Country (optional but recommended)", 
                            help="Specify the country to narrow down geocoding results (e.g., France, Belgium, Canada)")
    
    # Output file name
    output_filename = st.text_input("Output File Name", 
                                   value="geocoding_results.csv", 
                                   help="Name of the output CSV file")
    
    # Error detection threshold
    error_threshold = st.number_input("Error Detection Threshold", 
                                     min_value=2, 
                                     value=5, 
                                     help="Number of identical coordinates/addresses with different station names to flag as a potential error")

# Main content


# File uploader
uploaded_file = st.file_uploader("Upload a CSV file with station names", type=["csv"])

if uploaded_file is not None:
    try:
        # Read the uploaded file
        df = pd.read_csv(uploaded_file)
        
        # Show dataframe preview
        st.markdown('<div class="section-header">👀 Data Preview</div>', unsafe_allow_html=True)
        st.dataframe(df, use_container_width=True)
        
        # Column selection
        st.markdown('<div class="section-header">🛠️ Column Selection</div>', unsafe_allow_html=True)
        
        available_columns = df.columns.tolist()
        
        # Default to 'remote_name' if available, otherwise let user select
        default_name_column = 'remote_name' if 'remote_name' in available_columns else available_columns[0]
        name_column = st.selectbox("Select the column for location names:", available_columns, index=available_columns.index(default_name_column) if default_name_column in available_columns else 0)
        
        # Default to 'remote_city_name' if available, otherwise let user select
        city_column_options = ["None"] + available_columns
        default_city_column = 'remote_city_name' if 'remote_city_name' in available_columns else "None"
        city_column_index = city_column_options.index(default_city_column) if default_city_column in city_column_options else 0
        city_column = st.selectbox("Select the column for city names (optional):", city_column_options, index=city_column_index)
        
        # Convert "None" to None
        city_column = None if city_column == "None" else city_column
        
        # Process button
        button_label = "📍 Get Coordinates"
        if st.session_state.provider == "google":
            button_label += " (Google Maps)"
            if not api_key:
                st.error("Please enter your Google Maps API key")
                can_process = False
            else:
                can_process = True
        else:
            button_label += " (Nominatim)"
            can_process = True
            
        if st.button(button_label) and can_process:
            # Create progress message
            progress_message = st.empty()
            progress_message.info("Processing started... This may take time for large datasets.")

            # Progress bar
            progress_bar = st.progress(0, text="Geocoding in progress...")

            # Define output file path (in the same directory)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            provider_name = "google" if st.session_state.provider == "google" else "nominatim"
            output_file = f"{output_filename.split('.')[0]}_{provider_name}_{timestamp}.csv"

            # Progress callback
            def update_progress(current, total):
                progress = current / total
                progress_bar.progress(progress, text=f"Geocoding: {current}/{total}")

            # Process the data based on selected provider
            if st.session_state.provider == "google":
                result_df = get_coordinates_for_locations(
                    df, 
                    output_file, 
                    api_key, 
                    country=country, 
                    name_column=name_column, 
                    city_column=city_column,
                    progress_callback=update_progress,
                    fallback_without_location_filter=fallback_without_location_filter
                )
            else:  # Nominatim / OSM
                result_df = get_coordinates_with_nominatim(
                    df,
                    output_file,
                    country=country,
                    name_column=name_column,
                    city_column=city_column,
                    progress_callback=update_progress,
                    lock_to_settlement=lock_to_settlement  # <-- Passe l'option ici
                )

            progress_bar.progress(1.0, text="Geocoding completed!")

            # Get summary and filtered results
            summary, filtered_df = display_summary(result_df, country=country, name_column=name_column)
            
            # Update progress message
            progress_message.success("Processing completed!")
            
            # Show summary
            st.markdown('<div class="section-header">📊 Results Summary</div>', unsafe_allow_html=True)
            st.markdown(f"""
            <div class="success-box">
                <p>✅ <strong>Processing completed!</strong></p>
                <p>Total locations: {summary['total_locations']}</p>
                <p>Locations with coordinates: {summary['locations_with_coordinates']} ({summary['success_rate']:.1f}%)</p>
                <p>Locations without coordinates: {summary['locations_without_coordinates']} ({100-summary['success_rate']:.1f}%)</p>
                <p>Invalid results filtered (country name only): {summary['filtered_results']}</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Check for potential errors
            potential_errors = summary['potential_errors']
            
            # Display potential errors if any
            if potential_errors['duplicate_coordinates'] or potential_errors['duplicate_addresses']:
                st.markdown('<div class="section-header">⚠️ Potential Errors Detected</div>', unsafe_allow_html=True)
                
                st.markdown("""
                <div class="warning-box">
                    <p>⚠️ <strong>Warning:</strong> The following potential errors were detected:</p>
                </div>
                """, unsafe_allow_html=True)
                
                # Show duplicate coordinates
                if potential_errors['duplicate_coordinates']:
                    st.markdown('<div class="error-box"><strong>Multiple stations with identical coordinates</strong></div>', unsafe_allow_html=True)
                    
                    for error in potential_errors['duplicate_coordinates']:
                        st.markdown(f"""
                        <div class="detail-box">
                            <p><strong>Coordinates:</strong> {error['coordinates']}<br>
                            <strong>Number of different stations:</strong> {error['count']}</p>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Create a DataFrame for display
                        error_df = pd.DataFrame(error['sample_data'])
                        st.dataframe(error_df, use_container_width=True)
                
                # Show duplicate addresses
                if potential_errors['duplicate_addresses']:
                    st.markdown('<div class="error-box"><strong>Multiple stations with identical addresses</strong></div>', unsafe_allow_html=True)
                    
                    for error in potential_errors['duplicate_addresses']:
                        st.markdown(f"""
                        <div class="detail-box">
                            <p><strong>Address:</strong> {error['address']}<br>
                            <strong>Number of different stations:</strong> {error['count']}</p>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Create a DataFrame for display
                        error_df = pd.DataFrame(error['sample_data'])
                        st.dataframe(error_df, use_container_width=True)
            
            # Show results preview
            st.markdown('<div class="section-header">🔍 Results Preview</div>', unsafe_allow_html=True)
            st.dataframe(filtered_df, use_container_width=True)

            # Download links
            st.markdown('<div class="section-header">📥 Download Results</div>', unsafe_allow_html=True)
            st.markdown(get_download_link(filtered_df, output_file, "Download Filtered CSV Results"), unsafe_allow_html=True)
            st.markdown(get_download_link(result_df, f"raw_{output_file}", "Download Raw CSV Results (including potentially invalid entries)"), unsafe_allow_html=True)
            
            # Show a map of geocoded points
            st.markdown('<div class="section-header">🗺️ Map Visualization</div>', unsafe_allow_html=True)
            
            # Filter only points with coordinates
            map_data = filtered_df[filtered_df['Lat'].notna()].copy()
            
            # Rename columns for compatibility with st.map
            map_data_for_display = map_data.copy()
            map_data_for_display['latitude'] = map_data_for_display['Lat']
            map_data_for_display['longitude'] = map_data_for_display['Lng']
            
            if not map_data.empty:
                st.map(map_data_for_display[['latitude', 'longitude']])
            else:
                st.info("No valid coordinates to display on the map.")

            # Show Maps links if available
            if 'Maps_Link' in filtered_df.columns:
                st.markdown('<div class="section-header">🔗 Maps Links</div>', unsafe_allow_html=True)
                st.markdown('<div class="info-text">Click on any link below to open the location in Google Maps:</div>', unsafe_allow_html=True)
                # Display only the first 10 links to avoid cluttering the UI
                links_df = filtered_df[filtered_df['Maps_Link'].notna()].head(50)
                for idx, row in links_df.iterrows():
                    location_name = row[name_column] if pd.notna(row[name_column]) else "Unknown location"
                    maps_link = row['Maps_Link']
                    st.markdown(f'<a href="{maps_link}" target="_blank">🗺️ {location_name}</a>', unsafe_allow_html=True)
                if len(links_df) < len(filtered_df[filtered_df['Maps_Link'].notna()]):
                    st.info(f"Showing {len(links_df)} of {len(filtered_df[filtered_df['Maps_Link'].notna()])} available map links. Download the CSV to access all links.")
    
    except Exception as e:
        st.error(f"❌ Error processing the file: {str(e)}")
else:
    # Show instructions when no file is uploaded
    st.markdown("""
    <div class="info-text">
        <p>📝 <strong>Instructions:</strong></p>
        <ol>
            <li>🌐 Choose a geocoding provider in the sidebar</li>
            <li>🔑 If you choose Google Maps, enter your Google Maps API key</li>
            <li>🌍 Specify a country to narrow down geocoding results (recommended)</li>
            <li>📂 Upload a CSV file containing station names</li>
            <li>🛠️ Select the appropriate columns for station name and city (if available)</li>
            <li>📍 Click "Get Coordinates" to start processing</li>
        </ol>
        <p>✨ The tool will add latitude, longitude, address and Maps link columns to your data and save intermediate results every 10 locations.</p>
        <p>❌ Invalid results (where the address is just the country name) will be automatically filtered.</p>
        <p>⚠️ Potential errors (when multiple different stations have identical coordinates or addresses) will be flagged.</p>
    </div>
    """, unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown('<div style="text-align: center; color: #9E9E9E; font-size: 0.8rem;"> Сделано с ❤️ Камиллой | Built with ❤️ by Camille</div>', unsafe_allow_html=True)
