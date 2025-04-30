# --- START OF UPDATED FILE combined_extractor_app.py ---

import streamlit as st
from bs4 import BeautifulSoup
import pandas as pd
import re
import io
from thefuzz import process, fuzz
import unicodedata
import requests
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
import plotly.express as px
import numpy as np
import sqlite3
from datetime import datetime
import json # Required for the new Level Shoes processing
from collections import defaultdict

# --- App Configuration ---
APP_VERSION = "2.4.10" # Updated version: LevelShoes fix + KeyError fix + Localization to English
st.set_page_config(layout="wide", page_title="Ounass vs Level Shoes PLP Comparison")

# --- App Title and Info ---
st.title(f"Ounass vs Level Shoes PLP Designer Comparison (v{APP_VERSION})")
st.write("Enter Product Listing Page (PLP) URLs from Ounass and Level Shoes (Women's Shoes/Bags recommended) to extract and compare designer brand counts, or compare previously saved snapshots.")
st.info("Ensure the URLs point to the relevant listing pages. For Ounass, the tool will attempt to load all designers. Level Shoes extraction uses the new __NEXT_DATA__ method.")

# --- Session State Initialization ---
if 'ounass_data' not in st.session_state: st.session_state.ounass_data = []
if 'levelshoes_data' not in st.session_state: st.session_state.levelshoes_data = []
if 'df_ounass' not in st.session_state: st.session_state.df_ounass = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned'])
if 'df_levelshoes' not in st.session_state: st.session_state.df_levelshoes = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned'])
if 'df_comparison_sorted' not in st.session_state: st.session_state.df_comparison_sorted = pd.DataFrame()
if 'df_time_comparison' not in st.session_state: st.session_state.df_time_comparison = pd.DataFrame()
if 'ounass_url_input' not in st.session_state: st.session_state.ounass_url_input = ''
if 'levelshoes_url_input' not in st.session_state: st.session_state.levelshoes_url_input = ''
if 'processed_ounass_url' not in st.session_state: st.session_state.processed_ounass_url = ''
if 'confirm_delete_id' not in st.session_state: st.session_state.confirm_delete_id = None
if 'time_comp_id1' not in st.session_state: st.session_state.time_comp_id1 = None
if 'time_comp_id2' not in st.session_state: st.session_state.time_comp_id2 = None
if 'selected_url_key_for_time_comp' not in st.session_state: st.session_state.selected_url_key_for_time_comp = None
if 'time_comp_meta1' not in st.session_state: st.session_state.time_comp_meta1 = {}
if 'time_comp_meta2' not in st.session_state: st.session_state.time_comp_meta2 = {}

# --- Database Setup & Functions ---
DB_NAME = "comparison_history.db"
def get_db_connection(): conn = sqlite3.connect(DB_NAME); conn.row_factory = sqlite3.Row; return conn
def init_db():
    try:
        conn = get_db_connection()
        conn.execute('CREATE TABLE IF NOT EXISTS comparisons (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL, ounass_url TEXT NOT NULL, levelshoes_url TEXT NOT NULL, comparison_data TEXT NOT NULL, comparison_name TEXT)')
        conn.commit()
        conn.close()
    except Exception as e: st.error(f"Fatal DB Init Error: {e}")
def save_comparison(ounass_url, levelshoes_url, df_comparison):
    if df_comparison is None or df_comparison.empty:
        st.error("Cannot save empty comparison data.")
        return False
    try:
        conn = get_db_connection()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df_to_save = df_comparison.copy()
        cols_to_save = ['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference', 'Brand_Cleaned', 'Brand_Ounass', 'Brand_LevelShoes']
        # Ensure all expected columns exist for saving
        for col in cols_to_save:
            if col not in df_to_save.columns:
                df_to_save[col] = np.nan
        data_json = df_to_save[cols_to_save].to_json(orient="records", date_format="iso")
        conn.execute("INSERT INTO comparisons (timestamp, ounass_url, levelshoes_url, comparison_data, comparison_name) VALUES (?, ?, ?, ?, ?)",
                       (timestamp, ounass_url, levelshoes_url, data_json, None)) # Comparison name is not used currently
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Database Error: Could not save comparison - {e}")
        return False
def load_saved_comparisons_meta():
    try:
        conn = get_db_connection()
        comparisons = conn.execute("SELECT id, timestamp, ounass_url, levelshoes_url, comparison_name FROM comparisons ORDER BY timestamp DESC").fetchall()
        conn.close()
        return [dict(row) for row in comparisons] if comparisons else []
    except Exception as e:
        st.error(f"Database Error: Could not load saved comparisons - {e}")
        return []
def load_specific_comparison(comp_id):
    try:
        conn = get_db_connection()
        comp = conn.execute("SELECT timestamp, ounass_url, levelshoes_url, comparison_data, comparison_name FROM comparisons WHERE id = ?", (comp_id,)).fetchone()
        conn.close()
        if comp:
            # Create a default name if none was saved
            fallback_name = f"ID {comp_id} ({comp['timestamp']})"
            meta = {"timestamp": comp["timestamp"], "ounass_url": comp["ounass_url"], "levelshoes_url": comp["levelshoes_url"], "name": comp["comparison_name"] or fallback_name, "id": comp_id} # Add ID to meta
            df = pd.read_json(comp["comparison_data"], orient="records")
            # Re-calculate Difference if missing (older saves)
            if 'Difference' not in df.columns and 'Ounass_Count' in df.columns and 'LevelShoes_Count' in df.columns:
                df['Difference'] = df['Ounass_Count'] - df['LevelShoes_Count']
            # Re-create Display_Brand if missing (older saves)
            if 'Display_Brand' not in df.columns:
                brand_ounass_col = 'Brand_Ounass' if 'Brand_Ounass' in df.columns else None
                brand_ls_col = 'Brand_LevelShoes' if 'Brand_LevelShoes' in df.columns else None
                brand_cleaned_col = 'Brand_Cleaned' if 'Brand_Cleaned' in df.columns else None
                # Prioritize Ounass brand name if Ounass has count, else Level Shoes, fallback to Cleaned
                df['Display_Brand'] = np.where(df.get('Ounass_Count', 0) > 0,
                                               df[brand_ounass_col] if brand_ounass_col else df.get(brand_cleaned_col),
                                               df[brand_ls_col] if brand_ls_col else df.get(brand_cleaned_col))
                df['Display_Brand'].fillna("Unknown", inplace=True) # Final fallback

            return meta, df
        else:
            st.warning(f"Saved comparison with ID {comp_id} not found.")
            return None, None
    except Exception as e:
        st.error(f"Database Error: Could not load comparison ID {comp_id} - {e}")
        return None, None
def delete_comparison(comp_id):
    try:
        conn = get_db_connection()
        conn.execute("DELETE FROM comparisons WHERE id = ?", (comp_id,))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Database Error: Could not delete comparison ID {comp_id} - {e}")
        return False

# --- Helper Functions ---
def clean_brand_name(brand_name):
    if not isinstance(brand_name, str): return ""
    # Convert to uppercase, remove common symbols and spaces
    cleaned = brand_name.upper().replace('-', '').replace('&', '').replace('.', '').replace("'", '').replace(" ", '')
    # Normalize unicode characters (e.g., accented letters) to basic ASCII
    cleaned = unicodedata.normalize('NFKD', cleaned).encode('ascii', 'ignore').decode('utf-8')
    # Keep only alphanumeric characters
    cleaned = ''.join(c for c in cleaned if c.isalnum())
    return cleaned
def custom_scorer(s1, s2):
    # Use a combination of fuzzy matching scores for better results
    scores = [fuzz.ratio(s1, s2), fuzz.partial_ratio(s1, s2), fuzz.token_set_ratio(s1, s2), fuzz.token_sort_ratio(s1, s2)]
    return max(scores)

# --- HTML Processing Functions ---
def process_ounass_html(html_content):
    """ Parses Ounass HTML to find the designer facet and extract brand names and counts. """
    soup = BeautifulSoup(html_content, 'html.parser')
    data = [] # List to hold {'Brand': name, 'Count': count} dicts
    try:
        # Find the header containing "Designer" within a facet section
        designer_header = soup.find(lambda tag: tag.name == 'header' and
                                       'Designer' in tag.get_text(strip=True) and
                                       tag.find_parent('section', class_='Facet'))
        facet_section = designer_header.find_parent('section', class_='Facet') if designer_header else None

        if facet_section:
            # Find all designer links within the section
            # Prioritize links directly under ul > li, fallback to any FacetLink
            items = facet_section.select('ul > li > a.FacetLink') or \
                    facet_section.find_all('a', href=True, class_=lambda x: x and 'FacetLink' in x)

            if not items:
                st.warning("Ounass: Could not find brand list elements (FacetLink).")
            else:
                for item in items:
                    try:
                        name_span = item.find('span', class_='FacetLink-name')
                        if name_span:
                            # Find the count span within the name span
                            count_span = name_span.find('span', class_='FacetLink-count')
                            count_text = count_span.text.strip() if count_span else "(0)"

                            # To get the name without the count, clone the span and remove the count element
                            temp_name_span = BeautifulSoup(str(name_span), 'html.parser').find(class_='FacetLink-name')
                            temp_count_span = temp_name_span.find(class_='FacetLink-count')
                            if temp_count_span:
                                temp_count_span.decompose() # Remove count span from the temporary copy
                            designer_name = temp_name_span.text.strip()

                            # Extract count using regex
                            match = re.search(r'\((\d+)\)', count_text)
                            count = int(match.group(1)) if match else 0

                            # Add if name is valid and not a control element
                            if designer_name and "SHOW" not in designer_name.upper():
                                data.append({'Brand': designer_name, 'Count': count})
                    except Exception as item_e:
                         # Log individual item processing error if needed, but continue loop
                         # st.warning(f"Ounass: Error processing item: {item_e}")
                         pass # Continue to next item
        else:
            st.warning("Ounass: Could not find the 'Designer' facet section structure.")
    except Exception as e:
        st.error(f"Ounass: HTML parsing error: {e}")
        return [] # Return empty on major error
    if not data and html_content: # Check if data is empty despite having HTML
        st.warning("Ounass: No brand data extracted, though HTML was received.")
    return data

def process_levelshoes_html(html_content):
    """
    Parses HTML content from Level Shoes PLP to extract designer brands
    and counts by finding and processing the __NEXT_DATA__ JSON blob.
    """
    data_extracted = [] # Use the format expected by the main app
    if not html_content:
        st.warning("Level Shoes: Received empty HTML content.")
        return data_extracted

    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

        if not script_tag:
            st.error("Level Shoes Error: Page structure changed, '__NEXT_DATA__' script tag not found.")
            return data_extracted

        json_data_str = script_tag.string
        if not json_data_str:
            st.error("Level Shoes Error: __NEXT_DATA__ script tag content is empty.")
            return data_extracted

        # Parse the JSON data
        data = json.loads(json_data_str)

        # Navigate the JSON structure safely
        apollo_state = data.get('props', {}).get('pageProps', {}).get('__APOLLO_STATE__', {})
        if not apollo_state:
             st.error("Level Shoes Error: '__APOLLO_STATE__' not found within __NEXT_DATA__.")
             return data_extracted

        root_query = apollo_state.get('ROOT_QUERY', {})
        if not root_query:
            st.error("Level Shoes Error: 'ROOT_QUERY' not found within __APOLLO_STATE__.")
            return data_extracted

        # Find the key for product list data (often contains '_productList')
        product_list_key = next((key for key in root_query if key.startswith('_productList')), None)
        if not product_list_key:
             # Try alternative key structure often seen: _productList:({...})
             product_list_key = next((key for key in root_query if '_productList:({' in key), None)
             if not product_list_key:
                 st.error("Level Shoes Error: Could not find product list data key in ROOT_QUERY.")
                 # For debugging: # st.json(root_query.keys())
                 return data_extracted

        product_list_data = root_query.get(product_list_key, {})
        facets = product_list_data.get('facets', [])
        if not facets:
            st.warning("Level Shoes Warning: No 'facets' (filters) found in product list data.")
            return data_extracted # No filters available

        # Find the 'brand' or 'designer' facet
        designer_facet = None
        for facet in facets:
            # Check both 'key' and 'label' for flexibility
            facet_key = facet.get('key', '').lower()
            facet_label = facet.get('label', '').lower()
            if facet_key == 'brand' or facet_label == 'designer':
                designer_facet = facet
                break

        if not designer_facet:
            available_facets = [f.get('key') or f.get('label') for f in facets]
            st.error(f"Level Shoes Error: 'brand' or 'Designer' facet not found. Available facets: {available_facets}")
            return data_extracted

        # Extract options from the designer facet
        designer_options = designer_facet.get('options', [])
        if not designer_options:
            st.warning("Level Shoes Warning: 'Designer' facet found, but it contains no options.")
            return data_extracted

        # Process the options into the desired format
        for option in designer_options:
            name = option.get('name')
            count = option.get('count')
            # Ensure we have valid data and skip generic/control options
            if name is not None and count is not None:
                 upper_name = name.upper()
                 # Skip common filter control text
                 if "VIEW ALL" not in upper_name and "SHOW M" not in upper_name and "SHOW L" not in upper_name:
                     data_extracted.append({'Brand': name.strip(), 'Count': int(count)})

        if not data_extracted:
             st.warning("Level Shoes: Designer options were processed, but no valid brand data was extracted (list is empty).")

        return data_extracted

    except json.JSONDecodeError:
        st.error("Level Shoes Error: Failed to decode JSON data from __NEXT_DATA__.")
        return []
    except (AttributeError, KeyError, TypeError, IndexError) as e:
        st.error(f"Level Shoes Error: Problem navigating the JSON structure - {e}. Site structure might have changed again.")
        return []
    except Exception as e:
        st.error(f"Level Shoes Error: An unexpected error occurred during processing - {e}")
        # For debugging: # st.exception(e)
        return []

# --- Function to fetch HTML content from URL ---
def fetch_html_content(url):
    if not url:
        st.error("Fetch error: URL cannot be empty.")
        return None
    try:
        # Use a common User-Agent string
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive'
        }
        response = requests.get(url, headers=headers, timeout=30) # 30-second timeout
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        return response.text
    except requests.exceptions.Timeout:
        st.error(f"Error: Timeout occurred while fetching {url}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching {url}: {e}")
        return None
    except Exception as e: # Catch any other unexpected errors during fetch
        st.error(f"An unexpected error occurred during fetch: {e}")
        return None


# --- Function to ensure Ounass URL has the correct parameter ---
def ensure_ounass_full_list_parameter(url):
    """Adds or verifies the parameter to show all designers on Ounass."""
    param_key, param_value = 'fh_maxdisplaynrvalues_designer', '-1'
    try:
        if not url or 'ounass' not in urlparse(url).netloc.lower():
            return url # Return original URL if empty or not Ounass
    except Exception:
        return url # Return original on parsing error

    try:
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query, keep_blank_values=True)

        # Check if update is needed
        needs_update = param_key not in query_params or not query_params[param_key] or query_params[param_key][0] != param_value

        if needs_update:
            query_params[param_key] = [param_value]
            new_query_string = urlencode(query_params, doseq=True)
            # Reconstruct URL using list slicing for compatibility
            url_components = list(parsed_url)
            url_components[4] = new_query_string
            return urlunparse(url_components)
        else:
            return url # Parameter already correct
    except Exception as e:
        st.warning(f"Error processing Ounass URL parameters: {e}")
        return url # Return original URL on error

# --- URL Info Extraction Function ---
def extract_info_from_url(url):
    """Attempts to extract gender and category/subcategory from Ounass/LevelShoes URL paths."""
    try:
        if not url: return None, None
        parsed = urlparse(url)
        # Filter out empty segments and ignore common noise immediately
        ignore_segments = ['ae', 'com', 'en', 'shop', 'category', 'all', 'view-all', 'plp', 'sale']
        path_segments = [s for s in parsed.path.lower().split('/') if s and s not in ignore_segments]

        if not path_segments:
            return None, None

        gender_keywords = ["women", "men", "kids", "unisex"]
        gender = None
        category_parts_raw = []

        # Check first *remaining* segment for gender
        if path_segments and path_segments[0] in gender_keywords:
            gender = path_segments[0].title()
            category_parts_raw = path_segments[1:] # The rest are potential category parts
        else:
            # Assume all remaining segments are category parts if first isn't gender
            category_parts_raw = path_segments

        # Clean and join category parts
        cleaned_category_parts = []
        for part in category_parts_raw:
            # Further check within category parts - skip if it's a gender keyword misplaced
            if gender and part in gender_keywords:
                continue
            # Clean segment: remove .html, replace hyphens, strip whitespace
            cleaned = part.replace('.html', '').replace('-', ' ').strip()
            if cleaned:
                # Capitalize each word in the segment
                cleaned_category_parts.append(' '.join(word.capitalize() for word in cleaned.split()))

        category = " > ".join(cleaned_category_parts) if cleaned_category_parts else None

        return gender, category

    except Exception as e:
        # Optional: Log error for debugging
        # print(f"URL Info Extraction Error: {e} for URL: {url}")
        return None, None

# Initialize Database
init_db()

# --- Sidebar ---
st.sidebar.image("https://1000logos.net/wp-content/uploads/2021/05/Ounass-logo.png", width=150)
st.sidebar.caption(f"App Version: {APP_VERSION}")
st.sidebar.header("Enter URLs")
st.session_state.ounass_url_input = st.sidebar.text_input("Ounass URL", key="ounass_url_widget", value=st.session_state.ounass_url_input, placeholder="https://www.ounass.ae/...")
st.session_state.levelshoes_url_input = st.sidebar.text_input("Level Shoes URL", key="levelshoes_url_widget", value=st.session_state.levelshoes_url_input, placeholder="https://www.levelshoes.com/...")
process_button = st.sidebar.button("Process URLs", use_container_width=True)

# --- Saved Comparisons Sidebar ---
st.sidebar.markdown("---")
st.sidebar.subheader("Saved Comparisons")
saved_comps_meta = load_saved_comparisons_meta()
query_params = st.query_params.to_dict() # Get query parameters from URL
viewing_saved_id = query_params.get("view_id", [None])[0] # Check if 'view_id' is in URL

# Button to go back to live processing if viewing a saved comparison
if viewing_saved_id and st.session_state.get('confirm_delete_id') != viewing_saved_id :
     if st.sidebar.button("<< Back to Live Processing", key="back_live", use_container_width=True):
         st.query_params.clear() # Clear URL parameters
         st.session_state.confirm_delete_id = None
         st.rerun() # Rerun the app

if not saved_comps_meta:
    st.sidebar.caption("No comparisons saved yet.")
else:
    # Group comparisons by the pair of URLs used
    grouped_comps = defaultdict(list);
    for comp_meta in saved_comps_meta:
        url_key = (comp_meta.get('ounass_url',''), comp_meta.get('levelshoes_url',''))
        grouped_comps[url_key].append(comp_meta)

    st.sidebar.caption("Select two snapshots from the same group to compare over time.")
    url_group_keys = list(grouped_comps.keys())

    if 'selected_url_key_for_time_comp' not in st.session_state: st.session_state.selected_url_key_for_time_comp = None

    # Display each group in an expander
    for idx, url_key in enumerate(url_group_keys):
        comps_list = grouped_comps[url_key]
        # Try to generate a label from URL info
        g, c = extract_info_from_url(url_key[0] or url_key[1]) # Use Ounass URL first, fallback to LS
        expander_label = f"{g or '?'} / {c or '?'} ({len(comps_list)} snapshots)"
        # Fallback label if info extraction fails
        if not g and not c:
             oun_path_part = urlparse(url_key[0]).path.split('/')[-1].replace('.html','') or "Ounass"
             ls_path_part = urlparse(url_key[1]).path.split('/')[-1].replace('.html','') or "Level"
             expander_label = f"{oun_path_part} vs {ls_path_part} ({len(comps_list)} snapshots)"

        # Keep the expander open if this group is selected for time comparison
        is_expanded = st.session_state.selected_url_key_for_time_comp == url_key

        with st.sidebar.expander(expander_label, expanded=is_expanded):
            # Create options for selectbox: "Timestamp (ID: id)" -> id
            comp_options = {f"{datetime.fromisoformat(comp['timestamp']).strftime('%Y-%m-%d %H:%M')} (ID: {comp['id']})": comp['id']
                            for comp in sorted(comps_list, key=lambda x: x['timestamp'])} # Sort by timestamp
            options_list = list(comp_options.keys())
            ids_list = list(comp_options.values())

            # Button to select this group for time comparison
            if st.button("Select for Time Comparison", key=f"select_group_{idx}", use_container_width=True):
                 st.session_state.selected_url_key_for_time_comp = url_key
                 # Reset selections when choosing a new group
                 st.session_state.time_comp_id1 = None
                 st.session_state.time_comp_id2 = None
                 st.session_state.df_time_comparison = pd.DataFrame()
                 st.rerun()

            # If this group is selected, show snapshot selection
            if st.session_state.selected_url_key_for_time_comp == url_key:
                st.caption("Select two snapshots:")
                # Try to keep previous selections, default to first two if possible
                current_idx1 = ids_list.index(st.session_state.time_comp_id1) if st.session_state.time_comp_id1 in ids_list else 0
                current_idx2 = ids_list.index(st.session_state.time_comp_id2) if st.session_state.time_comp_id2 in ids_list else min(1, len(ids_list)-1) if len(ids_list) > 1 else 0

                selected_option1 = st.selectbox("Snapshot 1 (Older/Base):", options=options_list, index=current_idx1, key=f"time_sel1_{idx}", label_visibility="collapsed")
                selected_option2 = st.selectbox("Snapshot 2 (Newer):", options=options_list, index=current_idx2, key=f"time_sel2_{idx}", label_visibility="collapsed")
                st.session_state.time_comp_id1 = comp_options.get(selected_option1)
                st.session_state.time_comp_id2 = comp_options.get(selected_option2)

                if st.button("Compare Snapshots", key=f"compare_time_{idx}", use_container_width=True, disabled=(len(ids_list)<2)):
                    if st.session_state.time_comp_id1 and st.session_state.time_comp_id2 and st.session_state.time_comp_id1 != st.session_state.time_comp_id2:
                        # Load data for the two selected snapshots
                        meta1, df1 = load_specific_comparison(st.session_state.time_comp_id1)
                        meta2, df2 = load_specific_comparison(st.session_state.time_comp_id2)

                        if meta1 and df1 is not None and meta2 and df2 is not None:
                            # Ensure T1 is the older snapshot
                            ts1 = datetime.fromisoformat(meta1['timestamp'])
                            ts2 = datetime.fromisoformat(meta2['timestamp'])
                            if ts1 > ts2:
                                meta1, df1, meta2, df2 = meta2, df2, meta1, df1 # Swap if T1 is newer

                            # Ensure required columns exist before merge
                            for df_check in [df1, df2]:
                                if 'Display_Brand' not in df_check.columns:
                                     df_check['Display_Brand'] = df_check['Brand_Ounass'].fillna(df_check['Brand_LevelShoes']).fillna(df_check.get('Brand_Cleaned', "Unknown"))
                                     df_check['Display_Brand'].fillna("Unknown", inplace=True)
                                if 'Ounass_Count' not in df_check.columns: df_check['Ounass_Count'] = 0
                                if 'LevelShoes_Count' not in df_check.columns: df_check['LevelShoes_Count'] = 0


                            # Merge the dataframes on Display_Brand
                            df_time = pd.merge(
                                df1[['Display_Brand','Ounass_Count','LevelShoes_Count']],
                                df2[['Display_Brand','Ounass_Count','LevelShoes_Count']],
                                on='Display_Brand', how='outer', suffixes=('_T1','_T2')
                            )
                            df_time.fillna(0, inplace=True) # Fill NaN counts with 0

                            # Calculate changes
                            df_time['Ounass_Change'] = (df_time['Ounass_Count_T2'] - df_time['Ounass_Count_T1']).astype(int)
                            df_time['LevelShoes_Change'] = (df_time['LevelShoes_Count_T2'] - df_time['LevelShoes_Count_T1']).astype(int)

                            # Store results in session state and clear URL params
                            st.session_state.df_time_comparison = df_time
                            st.session_state.time_comp_meta1 = meta1
                            st.session_state.time_comp_meta2 = meta2
                            st.query_params.clear()
                            st.rerun()
                        else:
                            st.error("Failed to load data for one or both snapshots.")
                            st.session_state.df_time_comparison = pd.DataFrame()
                    else:
                        st.warning("Please select two different snapshots for comparison.")
                        st.session_state.df_time_comparison = pd.DataFrame()

            st.markdown("---")
            st.caption("View/Delete individual snapshots:")
            # Buttons to view or delete individual snapshots within the group
            for comp_meta in comps_list:
                 comp_id = comp_meta['id']
                 comp_ts_str = comp_meta['timestamp']
                 try:
                     display_ts = datetime.fromisoformat(comp_ts_str).strftime('%Y-%m-%d %H:%M')
                 except:
                     display_ts = comp_ts_str # Fallback if timestamp format is wrong

                 display_label = f"{display_ts} (ID: {comp_id})"
                 is_selected = str(comp_id) == viewing_saved_id # Check if this snapshot is being viewed
                 t_col1, t_col2 = st.columns([0.85, 0.15])

                 with t_col1:
                    button_type = "primary" if is_selected else "secondary"
                    # Button to view this snapshot - sets query parameter
                    if st.button(display_label, key=f"view_detail_{comp_id}", type=button_type, use_container_width=True):
                         st.query_params["view_id"] = str(comp_id)
                         st.session_state.confirm_delete_id = None # Clear delete confirmation
                         st.session_state.df_time_comparison = pd.DataFrame() # Clear time comparison view
                         st.rerun()
                 with t_col2:
                    # Button to initiate deletion - sets confirmation state
                    if st.button("🗑️", key=f"del_detail_{comp_id}", help=f"Delete snapshot from {display_ts}", use_container_width=True):
                         st.session_state.confirm_delete_id = comp_id
                         st.query_params.clear() # Clear view ID if deleting
                         st.rerun()

# --- Unified Display Function ---
def display_all_results(df_ounass, df_levelshoes, df_comparison_sorted, stats_title_prefix="Overall Statistics", is_saved_view=False, saved_meta=None):
    st.markdown("---")
    stats_title = stats_title_prefix
    detected_gender, detected_category = None, None

    # Extract info for titles/filenames if available
    if is_saved_view and saved_meta:
         # Use URLs from saved metadata
         oun_g, oun_c = extract_info_from_url(saved_meta.get('ounass_url', ''))
         ls_g, ls_c = extract_info_from_url(saved_meta.get('levelshoes_url', ''))
         if oun_g or ls_g: detected_gender = oun_g or ls_g # Prefer Ounass, take either
         if oun_c or ls_c: detected_category = oun_c or ls_c # Prefer Ounass, take either
         st.subheader(f"Viewing Saved Comparison ({saved_meta.get('timestamp', 'N/A')})")
         st.caption(f"Ounass URL: `{saved_meta.get('ounass_url', 'N/A')}`")
         st.caption(f"Level Shoes URL: `{saved_meta.get('levelshoes_url', 'N/A')}`")
         st.markdown("---")
    else: # Live view
        # Use URLs from session state
        url_for_stats = st.session_state.get('processed_ounass_url') or st.session_state.get('ounass_url_input')
        if not url_for_stats: url_for_stats = st.session_state.get('levelshoes_url_input') # Fallback to LS URL
        if url_for_stats:
            g_live, c_live = extract_info_from_url(url_for_stats)
            if g_live is not None: detected_gender = g_live
            if c_live is not None: detected_category = c_live

    # Construct the stats title
    if detected_gender and detected_category: stats_title = f"{stats_title_prefix} - {detected_gender} / {detected_category}"
    elif detected_gender: stats_title = f"{stats_title_prefix} - {detected_gender}"
    elif detected_category: stats_title = f"{stats_title_prefix} - {detected_category}"

    # Save Button Area (only in live view with results)
    if not is_saved_view and df_comparison_sorted is not None and not df_comparison_sorted.empty:
        stat_title_col, stat_save_col = st.columns([0.8, 0.2])
        with stat_title_col: st.subheader(stats_title)
        with stat_save_col:
            st.write("") # Spacer
            if st.button("💾 Save", key="save_live_comp_confirm", help="Save current comparison results", use_container_width=True):
                oun_url = st.session_state.get('processed_ounass_url', st.session_state.get('ounass_url_input','')) # Prioritize processed URL
                ls_url = st.session_state.get('levelshoes_url_input', '')
                df_save = st.session_state.df_comparison_sorted
                if save_comparison(oun_url, ls_url, df_save):
                    st.success(f"Comparison saved! (Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
                    st.session_state.confirm_delete_id = None
                    st.rerun() # Rerun to refresh saved list in sidebar
                # Error message handled within save_comparison
    else: # Saved view or no comparison data yet
        st.subheader(stats_title) # Display title even if no stats yet

    # --- UPDATED Statistics Calculation ---
    # Calculate stats safely, checking for column existence

    df_o_safe = df_ounass if df_ounass is not None and not df_ounass.empty else pd.DataFrame()
    df_l_safe = df_levelshoes if df_levelshoes is not None and not df_levelshoes.empty else pd.DataFrame()
    # df_c_safe represents the comparison dataframe, might be empty or lack columns initially
    df_c_safe = df_comparison_sorted if df_comparison_sorted is not None and not df_comparison_sorted.empty else pd.DataFrame()

    # Initialize counts to 0
    total_ounass_brands = 0
    total_levelshoes_brands = 0
    total_ounass_products = 0
    total_levelshoes_products = 0
    common_brands_count = 0
    ounass_only_count = 0
    levelshoes_only_count = 0

    # Calculate from individual dataframes first if available
    if not df_o_safe.empty:
        total_ounass_brands = len(df_o_safe)
        if 'Count' in df_o_safe.columns:
            total_ounass_products = int(df_o_safe['Count'].sum())

    if not df_l_safe.empty:
        total_levelshoes_brands = len(df_l_safe)
        if 'Count' in df_l_safe.columns:
            total_levelshoes_products = int(df_l_safe['Count'].sum())

    # Calculate comparison stats only if comparison dataframe exists and has necessary columns
    if not df_c_safe.empty and 'Ounass_Count' in df_c_safe.columns and 'LevelShoes_Count' in df_c_safe.columns:
        # Use comparison df as fallback or primary source if individual dfs were missing
        if total_ounass_products == 0: total_ounass_products = int(df_c_safe['Ounass_Count'].sum())
        if total_levelshoes_products == 0: total_levelshoes_products = int(df_c_safe['LevelShoes_Count'].sum())
        if total_ounass_brands == 0: total_ounass_brands = len(df_c_safe[df_c_safe['Ounass_Count'] > 0])
        if total_levelshoes_brands == 0: total_levelshoes_brands = len(df_c_safe[df_c_safe['LevelShoes_Count'] > 0])

        # Now calculate overlap stats safely
        common_brands_count = len(df_c_safe[(df_c_safe['Ounass_Count'] > 0) & (df_c_safe['LevelShoes_Count'] > 0)])
        ounass_only_count = len(df_c_safe[(df_c_safe['Ounass_Count'] > 0) & (df_c_safe['LevelShoes_Count'] == 0)])
        levelshoes_only_count = len(df_c_safe[(df_c_safe['Ounass_Count'] == 0) & (df_c_safe['LevelShoes_Count'] > 0)])
    # --- End of UPDATED Statistics Calculation ---


    # Display Stats
    stat_col1, stat_col2, stat_col3 = st.columns(3)
    with stat_col1:
        st.metric("Ounass Brands", f"{total_ounass_brands:,}")
        st.metric("Ounass Products", f"{total_ounass_products:,}")
    with stat_col2:
        st.metric("Level Shoes Brands", f"{total_levelshoes_brands:,}")
        st.metric("Level Shoes Products", f"{total_levelshoes_products:,}")
    with stat_col3:
        # Show comparison stats only if they were calculated
        if not df_c_safe.empty and 'Ounass_Count' in df_c_safe.columns and 'LevelShoes_Count' in df_c_safe.columns:
            st.metric("Common Brands", f"{common_brands_count:,}")
            st.metric("Ounass Only", f"{ounass_only_count:,}")
            st.metric("Level Shoes Only", f"{levelshoes_only_count:,}")
        else:
             # Display N/A if comparison data is unavailable
             st.metric("Common Brands", "N/A")
             st.metric("Ounass Only", "N/A")
             st.metric("Level Shoes Only", "N/A")
             if not is_saved_view and (st.session_state.get('ounass_url_input') or st.session_state.get('levelshoes_url_input')):
                 # Show caption only in live view if URLs were entered but comparison failed
                 st.caption("Comparison requires data from both sites.")

    st.write("") # Add vertical space
    st.markdown("---")

    # Individual Results Display (Only in Live View)
    if not is_saved_view:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Ounass Results")
            if df_ounass is not None and not df_ounass.empty and 'Brand' in df_ounass.columns and 'Count' in df_ounass.columns:
                 st.write(f"Brands Found: {len(df_ounass)}")
                 df_display = df_ounass.sort_values(by='Count', ascending=False).reset_index(drop=True)
                 df_display.index += 1 # Start index from 1
                 st.dataframe(df_display[['Brand', 'Count']], height=400, use_container_width=True)
                 # Download Button for Ounass List
                 csv_buffer = io.StringIO()
                 df_display[['Brand', 'Count']].to_csv(csv_buffer, index=False, encoding='utf-8')
                 csv_buffer.seek(0)
                 st.download_button("Download Ounass List (CSV)", csv_buffer.getvalue(), 'ounass_brands.csv', 'text/csv', key='ounass_dl_disp')
            elif process_button and st.session_state.ounass_url_input: # Process attempted but failed
                st.warning("No data extracted from Ounass.")
            elif not process_button and st.session_state.ounass_url_input: # URL entered but not processed
                st.info("Click 'Process URLs' to fetch Ounass data.")
            else: # No URL entered
                st.info("Enter Ounass URL in the sidebar.")
        with col2:
            st.subheader("Level Shoes Results")
            if df_levelshoes is not None and not df_levelshoes.empty and 'Brand' in df_levelshoes.columns and 'Count' in df_levelshoes.columns:
                 st.write(f"Brands Found: {len(df_levelshoes)}")
                 df_display = df_levelshoes.sort_values(by='Count', ascending=False).reset_index(drop=True)
                 df_display.index += 1 # Start index from 1
                 st.dataframe(df_display[['Brand', 'Count']], height=400, use_container_width=True)
                 # Download Button for Level Shoes List
                 csv_buffer = io.StringIO()
                 df_display[['Brand', 'Count']].to_csv(csv_buffer, index=False, encoding='utf-8')
                 csv_buffer.seek(0)
                 st.download_button("Download Level Shoes List (CSV)", csv_buffer.getvalue(), 'levelshoes_brands.csv', 'text/csv', key='ls_dl_disp')
            elif process_button and st.session_state.levelshoes_url_input: # Process attempted but failed
                st.warning("No data extracted from Level Shoes.")
            elif not process_button and st.session_state.levelshoes_url_input: # URL entered but not processed
                st.info("Click 'Process URLs' to fetch Level Shoes data.")
            else: # No URL entered
                st.info("Enter Level Shoes URL in the sidebar.")

    # Comparison Section (Show if comparison data exists)
    if df_comparison_sorted is not None and not df_comparison_sorted.empty:
        if not is_saved_view: st.markdown("---") # Separator only needed in live view before comparison
        st.subheader("Ounass vs Level Shoes Brand Comparison")

        df_display = df_comparison_sorted.copy()
        df_display.index += 1 # Start index from 1
        # Ensure necessary columns exist for display
        display_cols = ['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference']
        missing_cols = [col for col in display_cols if col not in df_display.columns]
        if missing_cols:
            st.warning(f"Comparison table is missing expected columns: {', '.join(missing_cols)}")
            # Display available columns as fallback
            st.dataframe(df_display, height=500, use_container_width=True)
        else:
            st.dataframe(df_display[display_cols], height=500, use_container_width=True)

        # Visualizations
        st.markdown("---")
        st.subheader("Visual Comparison")
        viz_col1, viz_col2 = st.columns(2)
        with viz_col1:
            st.write("**Brand Overlap**")
            pie_data = pd.DataFrame({'Category': ['Common Brands', 'Ounass Only', 'Level Shoes Only'],
                                     'Count': [common_brands_count, ounass_only_count, levelshoes_only_count]})
            pie_data = pie_data[pie_data['Count'] > 0] # Remove categories with zero count

            if not pie_data.empty:
                fig_pie = px.pie(pie_data, names='Category', values='Count', title="Brand Presence",
                                 color_discrete_sequence=px.colors.qualitative.Pastel)
                fig_pie.update_traces(textposition='inside', textinfo='percent+label+value')
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("No data available for overlap chart.")
        with viz_col2:
            st.write("**Top 10 Largest Differences (Count)**")
            # Check if 'Difference' column exists
            if 'Difference' in df_comparison_sorted.columns and 'Display_Brand' in df_comparison_sorted.columns:
                # Get top 5 positive and top 5 negative differences
                top_pos = df_comparison_sorted[df_comparison_sorted['Difference'] > 0].nlargest(5, 'Difference')
                top_neg = df_comparison_sorted[df_comparison_sorted['Difference'] < 0].nsmallest(5, 'Difference')
                top_diff = pd.concat([top_pos, top_neg]).sort_values('Difference', ascending=False)

                if not top_diff.empty:
                    fig_diff = px.bar(top_diff, x='Display_Brand', y='Difference',
                                      title="Largest Differences (Ounass - Level Shoes)",
                                      labels={'Display_Brand': 'Brand', 'Difference': 'Product Count Difference'},
                                      color='Difference', color_continuous_scale=px.colors.diverging.RdBu)
                    fig_diff.update_layout(xaxis_title=None) # Hide x-axis title
                    st.plotly_chart(fig_diff, use_container_width=True)
                else:
                    st.info("No significant differences found for the chart.")
            else:
                 st.info("Difference data unavailable for chart.")

        # Top Brands Bar Chart
        st.markdown("---")
        st.subheader("Top 15 Brands Comparison (Total Products)")
        # Check required columns exist
        if not df_comparison_sorted.empty and all(c in df_comparison_sorted.columns for c in ['Display_Brand', 'Ounass_Count', 'LevelShoes_Count']):
            df_comp_copy = df_comparison_sorted.copy()
            df_comp_copy['Total_Count'] = df_comp_copy['Ounass_Count'] + df_comp_copy['LevelShoes_Count']
            top_n = 15
            top_brands = df_comp_copy.nlargest(top_n, 'Total_Count')

            if not top_brands.empty:
                # Melt dataframe for grouped bar chart
                melted = top_brands.melt(id_vars='Display_Brand',
                                         value_vars=['Ounass_Count', 'LevelShoes_Count'],
                                         var_name='Website', value_name='Product Count')
                # Clean up website names for legend
                melted['Website'] = melted['Website'].str.replace('_Count', '').str.replace('LevelShoes','Level Shoes')

                fig_top = px.bar(melted, x='Display_Brand', y='Product Count', color='Website',
                                 barmode='group', title=f"Top {top_n} Brands by Total Products",
                                 labels={'Display_Brand': 'Brand'},
                                 category_orders={"Display_Brand": top_brands['Display_Brand'].tolist()}) # Keep order
                fig_top.update_layout(xaxis_title=None)
                st.plotly_chart(fig_top, use_container_width=True)
            else:
                st.info(f"Not enough data for Top {top_n} brands chart.")
        else:
             st.info(f"Comparison data unavailable for Top {top_n} chart.")

        # Tables for Unique / Common Brands
        st.markdown("---")
        col_comp1, col_comp2 = st.columns(2)
        # Check required columns before filtering/displaying
        req_cols_exist = all(c in df_comparison_sorted.columns for c in ['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference'])

        with col_comp1:
            st.subheader("Brands in Ounass Only")
            if req_cols_exist:
                df_f = df_comparison_sorted[(df_comparison_sorted['LevelShoes_Count'] == 0) & (df_comparison_sorted['Ounass_Count'] > 0)]
                if not df_f.empty:
                    df_d = df_f[['Display_Brand', 'Ounass_Count']].sort_values('Ounass_Count', ascending=False).reset_index(drop=True)
                    df_d.index += 1
                    st.dataframe(df_d, height=400, use_container_width=True)
                else: st.info("No unique Ounass brands found in this comparison.")
            else: st.info("Data unavailable.")

        with col_comp2:
            st.subheader("Brands in Level Shoes Only")
            if req_cols_exist:
                df_f = df_comparison_sorted[(df_comparison_sorted['Ounass_Count'] == 0) & (df_comparison_sorted['LevelShoes_Count'] > 0)]
                if not df_f.empty:
                    df_d = df_f[['Display_Brand', 'LevelShoes_Count']].sort_values('LevelShoes_Count', ascending=False).reset_index(drop=True)
                    df_d.index += 1
                    st.dataframe(df_d, height=400, use_container_width=True)
                else: st.info("No unique Level Shoes brands found in this comparison.")
            else: st.info("Data unavailable.")

        st.markdown("---")
        col_comp3, col_comp4 = st.columns(2)

        with col_comp3:
            st.subheader("Common Brands: Ounass > Level Shoes")
            if req_cols_exist:
                df_f = df_comparison_sorted[(df_comparison_sorted['Ounass_Count'] > 0) & (df_comparison_sorted['LevelShoes_Count'] > 0) & (df_comparison_sorted['Difference'] > 0)].sort_values('Difference', ascending=False)
                if not df_f.empty:
                    df_d = df_f[['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference']].reset_index(drop=True)
                    df_d.index += 1
                    st.dataframe(df_d, height=400, use_container_width=True)
                else: st.info("No common brands found where Ounass has more products.")
            else: st.info("Data unavailable.")

        with col_comp4:
            st.subheader("Common Brands: Level Shoes > Ounass")
            if req_cols_exist:
                df_f = df_comparison_sorted[(df_comparison_sorted['Ounass_Count'] > 0) & (df_comparison_sorted['LevelShoes_Count'] > 0) & (df_comparison_sorted['Difference'] < 0)].sort_values('Difference', ascending=True)
                if not df_f.empty:
                    df_d = df_f[['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference']].reset_index(drop=True)
                    df_d.index += 1
                    st.dataframe(df_d, height=400, use_container_width=True)
                else: st.info("No common brands found where Level Shoes has more products.")
            else: st.info("Data unavailable.")

        # Download Button for Comparison
        st.markdown("---")
        csv_buffer_comparison = io.StringIO()
        dl_cols = ['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference']
        if req_cols_exist: # Only enable download if columns exist
            df_comparison_sorted[dl_cols].to_csv(csv_buffer_comparison, index=False, encoding='utf-8')
            csv_buffer_comparison.seek(0)
            download_label = f"Download {'Saved' if is_saved_view else 'Current'} Comparison (CSV)"
            view_id_part = saved_meta['id'] if is_saved_view and saved_meta else 'live'
            download_key = f"comp_dl_button_{'saved' if is_saved_view else 'live'}_{view_id_part}"
            # Create a filename using extracted info or fallback
            filename_desc = f"{detected_gender or 'All'}_{detected_category or 'All'}".replace(' > ','-').replace(' ','_').lower()
            download_filename = f"brand_comparison_{filename_desc}_{view_id_part}.csv".replace('?_?', 'all_all') # Clean up filename

            st.download_button(download_label, csv_buffer_comparison.getvalue(), download_filename, 'text/csv', key=download_key)
        else:
             st.warning("Could not generate download file due to missing comparison columns.")

    elif process_button and not is_saved_view: # Process clicked, but no comparison generated
        st.markdown("---")
        st.warning("Comparison could not be generated. Check if data was successfully extracted from both URLs in the sections above.")
    elif not process_button and not is_saved_view and df_o_safe.empty and df_l_safe.empty :
         # Initial state or after clearing, show info message
         st.info("Enter URLs in the sidebar and click 'Process URLs' or select a saved comparison from the sidebar.")

# --- Time Comparison Display Function ---
def display_time_comparison_results(df_time_comp, meta1, meta2):
    st.markdown("---")
    st.subheader("Snapshot Comparison Over Time")
    ts_format = '%Y-%m-%d %H:%M'
    ts1_str, ts2_str = "N/A", "N/A"
    id1, id2 = meta1.get('id','N/A'), meta2.get('id','N/A') # Get IDs from meta if available
    try:
        if meta1 and 'timestamp' in meta1: ts1_str = datetime.fromisoformat(meta1['timestamp']).strftime(ts_format)
        if meta2 and 'timestamp' in meta2: ts2_str = datetime.fromisoformat(meta2['timestamp']).strftime(ts_format)
        st.markdown(f"Comparing **Snapshot 1** (`{ts1_str}`, ID: {id1}) **vs** **Snapshot 2** (`{ts2_str}`, ID: {id2})")
    except Exception as e:
        st.warning(f"Error formatting timestamps: {e}")
        st.markdown(f"Comparing Snapshot 1 (ID: {id1}) vs Snapshot 2 (ID: {id2})")

    with st.expander("Show URLs for Compared Snapshots"):
        st.caption(f"**Snap 1 ({ts1_str}):** O: `{meta1.get('ounass_url', 'N/A')}` | LS: `{meta1.get('levelshoes_url', 'N/A')}`")
        st.caption(f"**Snap 2 ({ts2_str}):** O: `{meta2.get('ounass_url', 'N/A')}` | LS: `{meta2.get('levelshoes_url', 'N/A')}`")
    st.markdown("---")

    # Ensure required columns exist in the time comparison dataframe
    req_time_cols = ['Display_Brand','Ounass_Count_T1','Ounass_Count_T2','Ounass_Change','LevelShoes_Count_T1','LevelShoes_Count_T2','LevelShoes_Change']
    if not all(col in df_time_comp.columns for col in req_time_cols):
        st.error("Time comparison data is missing required columns. Cannot display detailed changes.")
        return # Stop execution of this function if data is malformed

    # Calculate changes safely
    new_o=df_time_comp[(df_time_comp['Ounass_Count_T1']==0)&(df_time_comp['Ounass_Count_T2']>0)]
    drop_o=df_time_comp[(df_time_comp['Ounass_Count_T1']>0)&(df_time_comp['Ounass_Count_T2']==0)]
    inc_o=df_time_comp[(df_time_comp['Ounass_Change']>0) & (df_time_comp['Ounass_Count_T1'] > 0)] # Exclude 'New'
    dec_o=df_time_comp[(df_time_comp['Ounass_Change']<0)] # Includes those that went to 0 (dropped) if needed, handled by separate 'Dropped' category display

    new_l=df_time_comp[(df_time_comp['LevelShoes_Count_T1']==0)&(df_time_comp['LevelShoes_Count_T2']>0)]
    drop_l=df_time_comp[(df_time_comp['LevelShoes_Count_T1']>0)&(df_time_comp['LevelShoes_Count_T2']==0)]
    inc_l=df_time_comp[(df_time_comp['LevelShoes_Change']>0) & (df_time_comp['LevelShoes_Count_T1'] > 0)] # Exclude 'New'
    dec_l=df_time_comp[(df_time_comp['LevelShoes_Change']<0)] # Includes those that went to 0 (dropped)

    st.subheader("Summary of Changes")
    t_stat_col1, t_stat_col2 = st.columns(2)
    with t_stat_col1:
        st.metric("New Brands (Ounass)", len(new_o))
        st.metric("Dropped Brands (Ounass)", len(drop_o))
        st.metric("Increased Brands (Ounass)", len(inc_o)) # Brands with count > 0 in both T1 and T2, and change > 0
        st.metric("Decreased Brands (Ounass)", len(dec_o[dec_o['Ounass_Count_T2'] > 0])) # Brands with count > 0 in T2 and change < 0
        st.metric("Net Product Change (Ounass)", f"{df_time_comp['Ounass_Change'].sum():+,}")
    with t_stat_col2:
        st.metric("New Brands (Level Shoes)", len(new_l))
        st.metric("Dropped Brands (Level Shoes)", len(drop_l))
        st.metric("Increased Brands (Level Shoes)", len(inc_l))
        st.metric("Decreased Brands (Level Shoes)", len(dec_l[dec_l['LevelShoes_Count_T2'] > 0]))
        st.metric("Net Product Change (Level Shoes)", f"{df_time_comp['LevelShoes_Change'].sum():+,}")

    st.markdown("---")
    st.subheader("Detailed Brand Changes")
    tc_col1, tc_col2 = st.columns(2)
    height=250 # Height for dataframes

    # Helper function to display change category dataframe
    def display_change_df(df_change, category_name, count_col_t1, count_col_t2, change_col, sort_col, sort_ascending, rename_map):
        if not df_change.empty:
            st.write(f"{category_name} ({len(df_change)}):")
            display_cols = ['Display_Brand']
            if count_col_t1: display_cols.append(count_col_t1)
            if count_col_t2: display_cols.append(count_col_t2)
            if change_col: display_cols.append(change_col)

            df = df_change[display_cols].rename(columns=rename_map).sort_values(sort_col, ascending=sort_ascending).reset_index(drop=True)
            df.index += 1
            st.dataframe(df, height=height, use_container_width=True)
            return True
        return False

    # Ounass Changes Display
    with tc_col1:
        st.write("**Ounass Changes**")
        displayed_any_o = False
        rename_new_drop = {'Ounass_Count_T1':'Was', 'Ounass_Count_T2':'Now'}
        rename_inc_dec = {'Ounass_Count_T1':'Was', 'Ounass_Count_T2':'Now', 'Ounass_Change':'Change'}
        if display_change_df(new_o, "New", None, 'Ounass_Count_T2', None, 'Now', False, rename_new_drop): displayed_any_o = True
        if display_change_df(drop_o, "Dropped", 'Ounass_Count_T1', None, None, 'Was', False, rename_new_drop): displayed_any_o = True
        if display_change_df(inc_o, "Increased", 'Ounass_Count_T1', 'Ounass_Count_T2', 'Ounass_Change', 'Change', False, rename_inc_dec): displayed_any_o = True
        # Only show decreased if count > 0 in T2
        dec_o_display = dec_o[dec_o['Ounass_Count_T2'] > 0]
        if display_change_df(dec_o_display, "Decreased", 'Ounass_Count_T1', 'Ounass_Count_T2', 'Ounass_Change', 'Change', True, rename_inc_dec): displayed_any_o = True

        if not displayed_any_o:
            st.info("No significant changes detected for Ounass between these snapshots.")

    # Level Shoes Changes Display
    with tc_col2:
        st.write("**Level Shoes Changes**")
        displayed_any_l = False
        rename_new_drop = {'LevelShoes_Count_T1':'Was', 'LevelShoes_Count_T2':'Now'}
        rename_inc_dec = {'LevelShoes_Count_T1':'Was', 'LevelShoes_Count_T2':'Now', 'LevelShoes_Change':'Change'}
        if display_change_df(new_l, "New", None, 'LevelShoes_Count_T2', None, 'Now', False, rename_new_drop): displayed_any_l = True
        if display_change_df(drop_l, "Dropped", 'LevelShoes_Count_T1', None, None, 'Was', False, rename_new_drop): displayed_any_l = True
        if display_change_df(inc_l, "Increased", 'LevelShoes_Count_T1', 'LevelShoes_Count_T2', 'LevelShoes_Change', 'Change', False, rename_inc_dec): displayed_any_l = True
        # Only show decreased if count > 0 in T2
        dec_l_display = dec_l[dec_l['LevelShoes_Count_T2'] > 0]
        if display_change_df(dec_l_display, "Decreased", 'LevelShoes_Count_T1', 'LevelShoes_Count_T2', 'LevelShoes_Change', 'Change', True, rename_inc_dec): displayed_any_l = True

        if not displayed_any_l:
            st.info("No significant changes detected for Level Shoes between these snapshots.")

    # Download Button for Time Comparison
    st.markdown("---")
    csv_buffer = io.StringIO()
    # Ensure columns exist before attempting download
    if all(col in df_time_comp.columns for col in req_time_cols):
        df_time_comp[req_time_cols].to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_buffer.seek(0)
        st.download_button(
            label=f"Download Time Comparison ({ts1_str} vs {ts2_str})",
            data=csv_buffer.getvalue(),
            file_name=f"time_comparison_{id1}_vs_{id2}.csv",
            mime='text/csv',
            key='time_comp_dl_button'
        )
    else:
        st.warning("Could not generate download file for time comparison due to missing data.")


# --- Main Application Flow ---
confirm_id = st.session_state.get('confirm_delete_id')

# 1. Handle Delete Confirmation First
if confirm_id:
    st.warning(f"Are you sure you want to delete comparison ID {confirm_id}?")
    col_confirm, col_cancel, _ = st.columns([1,1,3])
    with col_confirm:
        if st.button("Yes, Delete", type="primary", key=f"confirm_delete_{confirm_id}"):
            if delete_comparison(confirm_id):
                st.success(f"Comparison ID {confirm_id} deleted.")
            else:
                 st.error("Deletion failed.") # Error message handled by delete_comparison
            # Clear confirmation state regardless of success/failure and rerun
            st.session_state.confirm_delete_id = None
            st.query_params.clear()
            st.rerun()
    with col_cancel:
        if st.button("Cancel", key=f"cancel_delete_{confirm_id}"):
            st.session_state.confirm_delete_id = None
            st.rerun()

# 2. Display Time Comparison Results if available
elif 'df_time_comparison' in st.session_state and not st.session_state.df_time_comparison.empty:
    display_time_comparison_results(
        st.session_state.df_time_comparison,
        st.session_state.get('time_comp_meta1',{}),
        st.session_state.get('time_comp_meta2',{})
    )

# 3. Display Saved Comparison if view_id is set
elif viewing_saved_id:
    saved_meta, saved_df = load_specific_comparison(viewing_saved_id)
    if saved_meta and saved_df is not None:
         # Display the loaded saved comparison
         display_all_results(None, None, saved_df, stats_title_prefix="Saved Comparison Details", is_saved_view=True, saved_meta=saved_meta)
    else:
         # Handle case where loading failed (e.g., ID deleted)
         st.error(f"Could not load comparison ID: {viewing_saved_id}. It might have been deleted or there was a load error.")
         if st.button("Clear Invalid Saved View URL"):
             st.query_params.clear()
             st.rerun()

# 4. Handle Live Processing or Initial State
else:
    if process_button:
        # Clear previous live results and time comparison state before processing
        st.session_state.df_ounass = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned'])
        st.session_state.df_levelshoes = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned'])
        st.session_state.ounass_data = []
        st.session_state.levelshoes_data = []
        st.session_state.df_comparison_sorted = pd.DataFrame()
        st.session_state.processed_ounass_url = ''
        st.session_state.df_time_comparison = pd.DataFrame() # Clear time comparison view
        st.session_state.time_comp_id1 = None
        st.session_state.time_comp_id2 = None
        st.session_state.selected_url_key_for_time_comp = None
        st.session_state.time_comp_meta1 = {}
        st.session_state.time_comp_meta2 = {}

        # Process Ounass URL
        if st.session_state.ounass_url_input:
            with st.spinner("Processing Ounass URL..."):
                st.session_state.processed_ounass_url = ensure_ounass_full_list_parameter(st.session_state.ounass_url_input)
                ounass_html_content = fetch_html_content(st.session_state.processed_ounass_url)
                if ounass_html_content:
                    st.session_state.ounass_data = process_ounass_html(ounass_html_content)
                    if st.session_state.ounass_data:
                         try:
                             st.session_state.df_ounass = pd.DataFrame(st.session_state.ounass_data)
                             if not st.session_state.df_ounass.empty:
                                 st.session_state.df_ounass['Brand_Cleaned'] = st.session_state.df_ounass['Brand'].apply(clean_brand_name)
                             else: st.warning("Ounass data extracted but resulted in an empty list.")
                         except Exception as e:
                             st.error(f"Error creating Ounass DataFrame: {e}")
                             st.session_state.df_ounass = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned'])
                    # else: Warnings/errors handled by process_ounass_html
                # else: Errors handled by fetch_html_content

        # Process Level Shoes URL
        if st.session_state.levelshoes_url_input:
             with st.spinner("Processing Level Shoes URL (using __NEXT_DATA__)..."):
                levelshoes_html_content = fetch_html_content(st.session_state.levelshoes_url_input)
                if levelshoes_html_content:
                    st.session_state.levelshoes_data = process_levelshoes_html(levelshoes_html_content) # Use the updated function
                    if st.session_state.levelshoes_data:
                        try:
                            st.session_state.df_levelshoes = pd.DataFrame(st.session_state.levelshoes_data)
                            if not st.session_state.df_levelshoes.empty:
                                st.session_state.df_levelshoes['Brand_Cleaned'] = st.session_state.df_levelshoes['Brand'].apply(clean_brand_name)
                            else: st.warning("Level Shoes data extracted but resulted in an empty list.")
                        except Exception as e:
                            st.error(f"Error creating Level Shoes DataFrame: {e}")
                            st.session_state.df_levelshoes = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned'])
                    # else: Warnings/errors handled by process_levelshoes_html
                # else: Errors handled by fetch_html_content

        # Create Comparison DataFrame if both were successful
        if ('df_ounass' in st.session_state and not st.session_state.df_ounass.empty and
            'df_levelshoes' in st.session_state and not st.session_state.df_levelshoes.empty):
            with st.spinner("Generating comparison..."):
                try:
                    df_o = st.session_state.df_ounass[['Brand','Count','Brand_Cleaned']].copy()
                    df_l = st.session_state.df_levelshoes[['Brand','Count','Brand_Cleaned']].copy()
                    # Merge based on the cleaned brand name
                    df_comp = pd.merge(df_o, df_l, on='Brand_Cleaned', how='outer', suffixes=('_Ounass', '_LevelShoes'))

                    # Fill NaN counts with 0 and ensure integer type
                    df_comp['Ounass_Count'] = df_comp['Count_Ounass'].fillna(0).astype(int)
                    df_comp['LevelShoes_Count'] = df_comp['Count_LevelShoes'].fillna(0).astype(int)
                    df_comp['Difference'] = df_comp['Ounass_Count'] - df_comp['LevelShoes_Count']

                    # Determine the display brand (prefer Ounass if present, else LevelShoes, fallback to Cleaned)
                    df_comp['Display_Brand'] = np.where(df_comp['Ounass_Count'] > 0, df_comp['Brand_Ounass'], df_comp['Brand_LevelShoes'])
                    df_comp['Display_Brand'].fillna(df_comp['Brand_Cleaned'], inplace=True) # Fallback if only one side had it and name was NaN
                    df_comp['Display_Brand'].fillna("Unknown", inplace=True) # Final fallback

                    # Select and order final columns for storage/potential internal use
                    final_cols = ['Display_Brand','Brand_Cleaned','Ounass_Count','LevelShoes_Count','Difference','Brand_Ounass','Brand_LevelShoes']
                    for col in final_cols:
                        if col not in df_comp.columns: df_comp[col] = np.nan # Ensure columns exist

                    # Sort the comparison results (e.g., by total count descending)
                    df_comp['Total_Count'] = df_comp['Ounass_Count'] + df_comp['LevelShoes_Count']
                    st.session_state.df_comparison_sorted = df_comp.sort_values(
                        by=['Total_Count', 'Ounass_Count', 'Display_Brand'],
                        ascending=[False, False, True] # Sort descending by total, then Ounass, then alphabetically
                    ).reset_index(drop=True)[final_cols + ['Total_Count']] # Select columns at the end
                except Exception as merge_e:
                     st.error(f"Error during comparison merge: {merge_e}")
                     st.session_state.df_comparison_sorted = pd.DataFrame()
        else:
             # If one or both failed, ensure comparison is empty
             st.session_state.df_comparison_sorted = pd.DataFrame()
             # Show warnings if process was clicked but data is missing
             if process_button:
                 if st.session_state.ounass_url_input and ('df_ounass' not in st.session_state or st.session_state.df_ounass.empty):
                     st.warning("Could not process Ounass data. Comparison not generated.")
                 if st.session_state.levelshoes_url_input and ('df_levelshoes' not in st.session_state or st.session_state.df_levelshoes.empty):
                      st.warning("Could not process Level Shoes data. Comparison not generated.")

        # After processing, rerun to display results or updated warnings
        st.rerun()


    # Always display whatever is currently in session state for live view
    # Get current state dataframes (could be populated by processing or empty)
    df_ounass_live = st.session_state.get('df_ounass', pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned']))
    df_levelshoes_live = st.session_state.get('df_levelshoes', pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned']))
    df_comparison_sorted_live = st.session_state.get('df_comparison_sorted', pd.DataFrame())

    # Call display function for live data (or lack thereof)
    display_all_results(df_ounass_live, df_levelshoes_live, df_comparison_sorted_live, stats_title_prefix="Current Comparison")

    # Show initial message if nothing has happened yet and no processing was attempted
    if not process_button and df_ounass_live.empty and df_levelshoes_live.empty and df_comparison_sorted_live.empty:
        # This state means app just loaded, no buttons clicked, no saved view selected
        st.info("Enter URLs in the sidebar and click 'Process URLs' or select a saved comparison.")


# --- END OF UPDATED FILE ---
