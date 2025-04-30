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
APP_VERSION = "2.4.11" # Updated version: UI Change - Inputs moved to main area
st.set_page_config(layout="wide", page_title="Ounass vs Level Shoes PLP Comparison")

# --- App Title and Info ---
st.title(f"Ounass vs Level Shoes PLP Designer Comparison (v{APP_VERSION})")
st.write("Enter Product Listing Page (PLP) URLs from Ounass and Level Shoes (Women's Shoes/Bags recommended) to extract and compare designer brand counts, or compare previously saved snapshots.")
st.info("Ensure the URLs point to the relevant listing pages. For Ounass, the tool will attempt to load all designers. Level Shoes extraction uses the new __NEXT_DATA__ method.")

# --- Session State Initialization ---
# (Keep this section as is - it initializes variables regardless of UI location)
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


# --- !!! MOVED URL Input Section (Main Area) !!! ---
st.markdown("---") # Separator
st.subheader("Enter URLs to Compare")

col1, col2 = st.columns(2)
with col1:
    st.session_state.ounass_url_input = st.text_input( # Removed .sidebar
        "Ounass URL",
        key="ounass_url_widget_main", # Use a distinct key
        value=st.session_state.ounass_url_input,
        placeholder="https://www.ounass.ae/..."
    )
with col2:
    st.session_state.levelshoes_url_input = st.text_input( # Removed .sidebar
        "Level Shoes URL",
        key="levelshoes_url_widget_main", # Use a distinct key
        value=st.session_state.levelshoes_url_input,
        placeholder="https://www.levelshoes.com/..."
    )

# Place button below inputs, perhaps centered
# _, col_btn, _ = st.columns([1, 2, 1]) # Centering column approach
# with col_btn:
process_button = st.button( # Removed .sidebar
        "Process URLs",
        key="process_button_main", # Use a distinct key
        # use_container_width=True # Optional: Makes button wider
    )

st.markdown("---") # Separator before results


# --- Database Setup & Functions ---
# (Keep this section as is)
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
        for col in cols_to_save:
            if col not in df_to_save.columns: df_to_save[col] = np.nan
        data_json = df_to_save[cols_to_save].to_json(orient="records", date_format="iso")
        conn.execute("INSERT INTO comparisons (timestamp, ounass_url, levelshoes_url, comparison_data, comparison_name) VALUES (?, ?, ?, ?, ?)",
                       (timestamp, ounass_url, levelshoes_url, data_json, None))
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
            fallback_name = f"ID {comp_id} ({comp['timestamp']})"
            meta = {"timestamp": comp["timestamp"], "ounass_url": comp["ounass_url"], "levelshoes_url": comp["levelshoes_url"], "name": comp["comparison_name"] or fallback_name, "id": comp_id}
            df = pd.read_json(comp["comparison_data"], orient="records")
            if 'Difference' not in df.columns and 'Ounass_Count' in df.columns and 'LevelShoes_Count' in df.columns:
                df['Difference'] = df['Ounass_Count'] - df['LevelShoes_Count']
            if 'Display_Brand' not in df.columns:
                brand_ounass_col = 'Brand_Ounass' if 'Brand_Ounass' in df.columns else None
                brand_ls_col = 'Brand_LevelShoes' if 'Brand_LevelShoes' in df.columns else None
                brand_cleaned_col = 'Brand_Cleaned' if 'Brand_Cleaned' in df.columns else None
                df['Display_Brand'] = np.where(df.get('Ounass_Count', 0) > 0,
                                               df[brand_ounass_col] if brand_ounass_col else df.get(brand_cleaned_col),
                                               df[brand_ls_col] if brand_ls_col else df.get(brand_cleaned_col))
                df['Display_Brand'].fillna("Unknown", inplace=True)
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
# (Keep this section as is)
def clean_brand_name(brand_name):
    if not isinstance(brand_name, str): return ""
    cleaned = brand_name.upper().replace('-', '').replace('&', '').replace('.', '').replace("'", '').replace(" ", '')
    cleaned = unicodedata.normalize('NFKD', cleaned).encode('ascii', 'ignore').decode('utf-8')
    cleaned = ''.join(c for c in cleaned if c.isalnum())
    return cleaned
def custom_scorer(s1, s2):
    scores = [fuzz.ratio(s1, s2), fuzz.partial_ratio(s1, s2), fuzz.token_set_ratio(s1, s2), fuzz.token_sort_ratio(s1, s2)]
    return max(scores)

# --- HTML Processing Functions ---
# (Keep these functions as is)
def process_ounass_html(html_content):
    """ Parses Ounass HTML to find the designer facet and extract brand names and counts. """
    soup = BeautifulSoup(html_content, 'html.parser'); data = []
    try:
        designer_header = soup.find(lambda tag: tag.name == 'header' and 'Designer' in tag.get_text(strip=True) and tag.find_parent('section', class_='Facet'))
        facet_section = designer_header.find_parent('section', class_='Facet') if designer_header else None
        if facet_section:
            items = facet_section.select('ul > li > a.FacetLink') or facet_section.find_all('a', href=True, class_=lambda x: x and 'FacetLink' in x)
            if not items: st.warning("Ounass: Could not find brand list elements (FacetLink).")
            else:
                for item in items:
                    try:
                        name_span = item.find('span', class_='FacetLink-name')
                        if name_span:
                            count_span = name_span.find('span', class_='FacetLink-count'); count_text = count_span.text.strip() if count_span else "(0)"
                            temp_name_span = BeautifulSoup(str(name_span), 'html.parser').find(class_='FacetLink-name')
                            temp_count_span = temp_name_span.find(class_='FacetLink-count')
                            if temp_count_span: temp_count_span.decompose()
                            designer_name = temp_name_span.text.strip()
                            match = re.search(r'\((\d+)\)', count_text); count = int(match.group(1)) if match else 0
                            if designer_name and "SHOW" not in designer_name.upper(): data.append({'Brand': designer_name, 'Count': count})
                    except Exception as item_e: pass
        else: st.warning("Ounass: Could not find the 'Designer' facet section structure.")
    except Exception as e: st.error(f"Ounass: HTML parsing error: {e}"); return []
    if not data and html_content: st.warning("Ounass: No brand data extracted, though HTML was received.")
    return data

def process_levelshoes_html(html_content):
    """ Parses HTML content from Level Shoes PLP to extract designer brands and counts by finding and processing the __NEXT_DATA__ JSON blob. """
    data_extracted = [];
    if not html_content: st.warning("Level Shoes: Received empty HTML content."); return data_extracted
    try:
        soup = BeautifulSoup(html_content, 'html.parser'); script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
        if not script_tag: st.error("Level Shoes Error: Page structure changed, '__NEXT_DATA__' script tag not found."); return data_extracted
        json_data_str = script_tag.string
        if not json_data_str: st.error("Level Shoes Error: __NEXT_DATA__ script tag content is empty."); return data_extracted
        data = json.loads(json_data_str)
        apollo_state = data.get('props', {}).get('pageProps', {}).get('__APOLLO_STATE__', {})
        if not apollo_state: st.error("Level Shoes Error: '__APOLLO_STATE__' not found within __NEXT_DATA__."); return data_extracted
        root_query = apollo_state.get('ROOT_QUERY', {})
        if not root_query: st.error("Level Shoes Error: 'ROOT_QUERY' not found within __APOLLO_STATE__."); return data_extracted
        product_list_key = next((key for key in root_query if key.startswith('_productList')), None)
        if not product_list_key: product_list_key = next((key for key in root_query if '_productList:({' in key), None)
        if not product_list_key: st.error("Level Shoes Error: Could not find product list data key in ROOT_QUERY."); return data_extracted
        product_list_data = root_query.get(product_list_key, {}); facets = product_list_data.get('facets', [])
        if not facets: st.warning("Level Shoes Warning: No 'facets' (filters) found in product list data."); return data_extracted
        designer_facet = None
        for facet in facets:
            facet_key = facet.get('key', '').lower(); facet_label = facet.get('label', '').lower()
            if facet_key == 'brand' or facet_label == 'designer': designer_facet = facet; break
        if not designer_facet: available_facets = [f.get('key') or f.get('label') for f in facets]; st.error(f"Level Shoes Error: 'brand' or 'Designer' facet not found. Available facets: {available_facets}"); return data_extracted
        designer_options = designer_facet.get('options', [])
        if not designer_options: st.warning("Level Shoes Warning: 'Designer' facet found, but it contains no options."); return data_extracted
        for option in designer_options:
            name = option.get('name'); count = option.get('count')
            if name is not None and count is not None:
                 upper_name = name.upper()
                 if "VIEW ALL" not in upper_name and "SHOW M" not in upper_name and "SHOW L" not in upper_name:
                     data_extracted.append({'Brand': name.strip(), 'Count': int(count)})
        if not data_extracted: st.warning("Level Shoes: Designer options were processed, but no valid brand data was extracted (list is empty).")
        return data_extracted
    except json.JSONDecodeError: st.error("Level Shoes Error: Failed to decode JSON data from __NEXT_DATA__."); return []
    except (AttributeError, KeyError, TypeError, IndexError) as e: st.error(f"Level Shoes Error: Problem navigating the JSON structure - {e}. Site structure might have changed again."); return []
    except Exception as e: st.error(f"Level Shoes Error: An unexpected error occurred during processing - {e}"); return []

# --- Function to fetch HTML content from URL ---
# (Keep this function as is)
def fetch_html_content(url):
    if not url: st.error("Fetch error: URL cannot be empty."); return None
    try:
        headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8', 'Accept-Language': 'en-US,en;q=0.9', 'Connection': 'keep-alive' }
        response = requests.get(url, headers=headers, timeout=30); response.raise_for_status(); return response.text
    except requests.exceptions.Timeout: st.error(f"Error: Timeout occurred while fetching {url}"); return None
    except requests.exceptions.RequestException as e: st.error(f"Error fetching {url}: {e}"); return None
    except Exception as e: st.error(f"An unexpected error occurred during fetch: {e}"); return None

# --- Function to ensure Ounass URL has the correct parameter ---
# (Keep this function as is)
def ensure_ounass_full_list_parameter(url):
    param_key, param_value = 'fh_maxdisplaynrvalues_designer', '-1'
    try:
        if not url or 'ounass' not in urlparse(url).netloc.lower(): return url
    except Exception: return url
    try:
        parsed_url = urlparse(url); query_params = parse_qs(parsed_url.query, keep_blank_values=True)
        needs_update = param_key not in query_params or not query_params[param_key] or query_params[param_key][0] != param_value
        if needs_update:
            query_params[param_key] = [param_value]; new_query_string = urlencode(query_params, doseq=True)
            url_components = list(parsed_url); url_components[4] = new_query_string; return urlunparse(url_components)
        else: return url
    except Exception as e: st.warning(f"Error processing Ounass URL parameters: {e}"); return url

# --- URL Info Extraction Function ---
# (Keep this function as is)
def extract_info_from_url(url):
    try:
        if not url: return None, None
        parsed = urlparse(url)
        ignore_segments = ['ae', 'com', 'en', 'shop', 'category', 'all', 'view-all', 'plp', 'sale']
        path_segments = [s for s in parsed.path.lower().split('/') if s and s not in ignore_segments]
        if not path_segments: return None, None
        gender_keywords = ["women", "men", "kids", "unisex"]; gender = None; category_parts_raw = []
        if path_segments and path_segments[0] in gender_keywords:
            gender = path_segments[0].title(); category_parts_raw = path_segments[1:]
        else: category_parts_raw = path_segments
        cleaned_category_parts = []
        for part in category_parts_raw:
            if gender and part in gender_keywords: continue
            cleaned = part.replace('.html', '').replace('-', ' ').strip()
            if cleaned: cleaned_category_parts.append(' '.join(word.capitalize() for word in cleaned.split()))
        category = " > ".join(cleaned_category_parts) if cleaned_category_parts else None
        return gender, category
    except Exception as e: return None, None

# Initialize Database
init_db()

# --- Sidebar ---
# (Sidebar now only contains Logo, Version, and Saved Comparisons)
st.sidebar.image("https://1000logos.net/wp-content/uploads/2021/05/Ounass-logo.png", width=150)
st.sidebar.caption(f"App Version: {APP_VERSION}")
# Removed: st.sidebar.header("Enter URLs")
# Removed: URL input text boxes
# Removed: Process URLs button

# --- Saved Comparisons Sidebar Section ---
st.sidebar.markdown("---")
st.sidebar.subheader("Saved Comparisons")
saved_comps_meta = load_saved_comparisons_meta()
query_params = st.query_params.to_dict()
viewing_saved_id = query_params.get("view_id", [None])[0]

if viewing_saved_id and st.session_state.get('confirm_delete_id') != viewing_saved_id :
     if st.sidebar.button("<< Back to Live Processing", key="back_live", use_container_width=True):
         st.query_params.clear(); st.session_state.confirm_delete_id = None; st.rerun()

if not saved_comps_meta:
    st.sidebar.caption("No comparisons saved yet.")
else:
    grouped_comps = defaultdict(list);
    for comp_meta in saved_comps_meta:
        url_key = (comp_meta.get('ounass_url',''), comp_meta.get('levelshoes_url',''))
        grouped_comps[url_key].append(comp_meta)
    st.sidebar.caption("Select two snapshots from the same group to compare over time.")
    url_group_keys = list(grouped_comps.keys())
    if 'selected_url_key_for_time_comp' not in st.session_state: st.session_state.selected_url_key_for_time_comp = None

    for idx, url_key in enumerate(url_group_keys):
        comps_list = grouped_comps[url_key]
        g, c = extract_info_from_url(url_key[0] or url_key[1])
        expander_label = f"{g or '?'} / {c or '?'} ({len(comps_list)} snapshots)"
        if not g and not c:
             oun_path_part = urlparse(url_key[0]).path.split('/')[-1].replace('.html','') or "Ounass"
             ls_path_part = urlparse(url_key[1]).path.split('/')[-1].replace('.html','') or "Level"
             expander_label = f"{oun_path_part} vs {ls_path_part} ({len(comps_list)} snapshots)"
        is_expanded = st.session_state.selected_url_key_for_time_comp == url_key

        with st.sidebar.expander(expander_label, expanded=is_expanded):
            comp_options = {f"{datetime.fromisoformat(comp['timestamp']).strftime('%Y-%m-%d %H:%M')} (ID: {comp['id']})": comp['id']
                            for comp in sorted(comps_list, key=lambda x: x['timestamp'])}
            options_list = list(comp_options.keys()); ids_list = list(comp_options.values())
            if st.button("Select for Time Comparison", key=f"select_group_{idx}", use_container_width=True):
                 st.session_state.selected_url_key_for_time_comp = url_key
                 st.session_state.time_comp_id1 = None; st.session_state.time_comp_id2 = None
                 st.session_state.df_time_comparison = pd.DataFrame(); st.rerun()
            if st.session_state.selected_url_key_for_time_comp == url_key:
                st.caption("Select two snapshots:")
                current_idx1 = ids_list.index(st.session_state.time_comp_id1) if st.session_state.time_comp_id1 in ids_list else 0
                current_idx2 = ids_list.index(st.session_state.time_comp_id2) if st.session_state.time_comp_id2 in ids_list else min(1, len(ids_list)-1) if len(ids_list) > 1 else 0
                selected_option1 = st.selectbox("Snapshot 1 (Older/Base):", options=options_list, index=current_idx1, key=f"time_sel1_{idx}", label_visibility="collapsed")
                selected_option2 = st.selectbox("Snapshot 2 (Newer):", options=options_list, index=current_idx2, key=f"time_sel2_{idx}", label_visibility="collapsed")
                st.session_state.time_comp_id1 = comp_options.get(selected_option1); st.session_state.time_comp_id2 = comp_options.get(selected_option2)
                if st.button("Compare Snapshots", key=f"compare_time_{idx}", use_container_width=True, disabled=(len(ids_list)<2)):
                    if st.session_state.time_comp_id1 and st.session_state.time_comp_id2 and st.session_state.time_comp_id1 != st.session_state.time_comp_id2:
                        meta1, df1 = load_specific_comparison(st.session_state.time_comp_id1); meta2, df2 = load_specific_comparison(st.session_state.time_comp_id2)
                        if meta1 and df1 is not None and meta2 and df2 is not None:
                            ts1 = datetime.fromisoformat(meta1['timestamp']); ts2 = datetime.fromisoformat(meta2['timestamp'])
                            if ts1 > ts2: meta1, df1, meta2, df2 = meta2, df2, meta1, df1
                            for df_check in [df1, df2]:
                                if 'Display_Brand' not in df_check.columns: df_check['Display_Brand'] = df_check['Brand_Ounass'].fillna(df_check['Brand_LevelShoes']).fillna(df_check.get('Brand_Cleaned', "Unknown")); df_check['Display_Brand'].fillna("Unknown", inplace=True)
                                if 'Ounass_Count' not in df_check.columns: df_check['Ounass_Count'] = 0
                                if 'LevelShoes_Count' not in df_check.columns: df_check['LevelShoes_Count'] = 0
                            df_time = pd.merge(df1[['Display_Brand','Ounass_Count','LevelShoes_Count']], df2[['Display_Brand','Ounass_Count','LevelShoes_Count']], on='Display_Brand', how='outer', suffixes=('_T1','_T2'))
                            df_time.fillna(0, inplace=True)
                            df_time['Ounass_Change'] = (df_time['Ounass_Count_T2'] - df_time['Ounass_Count_T1']).astype(int)
                            df_time['LevelShoes_Change'] = (df_time['LevelShoes_Count_T2'] - df_time['LevelShoes_Count_T1']).astype(int)
                            st.session_state.df_time_comparison = df_time; st.session_state.time_comp_meta1 = meta1; st.session_state.time_comp_meta2 = meta2
                            st.query_params.clear(); st.rerun()
                        else: st.error("Failed to load data for one or both snapshots."); st.session_state.df_time_comparison = pd.DataFrame()
                    else: st.warning("Please select two different snapshots for comparison."); st.session_state.df_time_comparison = pd.DataFrame()
            st.markdown("---"); st.caption("View/Delete individual snapshots:")
            for comp_meta in comps_list:
                 comp_id = comp_meta['id']; comp_ts_str = comp_meta['timestamp']
                 try: display_ts = datetime.fromisoformat(comp_ts_str).strftime('%Y-%m-%d %H:%M')
                 except: display_ts = comp_ts_str
                 display_label = f"{display_ts} (ID: {comp_id})"; is_selected = str(comp_id) == viewing_saved_id; t_col1, t_col2 = st.columns([0.85, 0.15])
                 with t_col1:
                    button_type = "primary" if is_selected else "secondary"
                    if st.button(display_label, key=f"view_detail_{comp_id}", type=button_type, use_container_width=True):
                         st.query_params["view_id"] = str(comp_id); st.session_state.confirm_delete_id = None
                         st.session_state.df_time_comparison = pd.DataFrame(); st.rerun()
                 with t_col2:
                    if st.button("🗑️", key=f"del_detail_{comp_id}", help=f"Delete snapshot from {display_ts}", use_container_width=True):
                         st.session_state.confirm_delete_id = comp_id; st.query_params.clear(); st.rerun()

# --- Unified Display Function ---
# (Keep this function as is, including the KeyError fix)
def display_all_results(df_ounass, df_levelshoes, df_comparison_sorted, stats_title_prefix="Overall Statistics", is_saved_view=False, saved_meta=None):
    # This separator is now below the input area
    # st.markdown("---") # Removed from here, placed earlier
    stats_title = stats_title_prefix; detected_gender, detected_category = None, None
    if is_saved_view and saved_meta:
         oun_g, oun_c = extract_info_from_url(saved_meta.get('ounass_url', '')); ls_g, ls_c = extract_info_from_url(saved_meta.get('levelshoes_url', ''))
         if oun_g or ls_g: detected_gender = oun_g or ls_g
         if oun_c or ls_c: detected_category = oun_c or ls_c
         st.subheader(f"Viewing Saved Comparison ({saved_meta.get('timestamp', 'N/A')})")
         st.caption(f"Ounass URL: `{saved_meta.get('ounass_url', 'N/A')}`")
         st.caption(f"Level Shoes URL: `{saved_meta.get('levelshoes_url', 'N/A')}`")
         # Removed st.markdown("---") here, stats follow directly
    else:
        url_for_stats = st.session_state.get('processed_ounass_url') or st.session_state.get('ounass_url_input')
        if not url_for_stats: url_for_stats = st.session_state.get('levelshoes_url_input')
        if url_for_stats: g_live, c_live = extract_info_from_url(url_for_stats); detected_gender = g_live; detected_category = c_live
        # No extra markdown before stats in live view either

    if detected_gender and detected_category: stats_title = f"{stats_title_prefix} - {detected_gender} / {detected_category}"
    elif detected_gender: stats_title = f"{stats_title_prefix} - {detected_gender}"
    elif detected_category: stats_title = f"{stats_title_prefix} - {detected_category}"

    if not is_saved_view and df_comparison_sorted is not None and not df_comparison_sorted.empty:
        stat_title_col, stat_save_col = st.columns([0.8, 0.2])
        with stat_title_col: st.subheader(stats_title)
        with stat_save_col:
            st.write("");
            if st.button("💾 Save", key="save_live_comp_confirm", help="Save current comparison results", use_container_width=True):
                oun_url = st.session_state.get('processed_ounass_url', st.session_state.get('ounass_url_input',''))
                ls_url = st.session_state.get('levelshoes_url_input', ''); df_save = st.session_state.df_comparison_sorted
                if save_comparison(oun_url, ls_url, df_save):
                    st.success(f"Comparison saved! (Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
                    st.session_state.confirm_delete_id = None; st.rerun()
    else: st.subheader(stats_title)

    # --- UPDATED Statistics Calculation ---
    df_o_safe = df_ounass if df_ounass is not None and not df_ounass.empty else pd.DataFrame()
    df_l_safe = df_levelshoes if df_levelshoes is not None and not df_levelshoes.empty else pd.DataFrame()
    df_c_safe = df_comparison_sorted if df_comparison_sorted is not None and not df_comparison_sorted.empty else pd.DataFrame()
    total_ounass_brands = 0; total_levelshoes_brands = 0; total_ounass_products = 0; total_levelshoes_products = 0
    common_brands_count = 0; ounass_only_count = 0; levelshoes_only_count = 0
    if not df_o_safe.empty:
        total_ounass_brands = len(df_o_safe)
        if 'Count' in df_o_safe.columns: total_ounass_products = int(df_o_safe['Count'].sum())
    if not df_l_safe.empty:
        total_levelshoes_brands = len(df_l_safe)
        if 'Count' in df_l_safe.columns: total_levelshoes_products = int(df_l_safe['Count'].sum())
    if not df_c_safe.empty and 'Ounass_Count' in df_c_safe.columns and 'LevelShoes_Count' in df_c_safe.columns:
        if total_ounass_products == 0: total_ounass_products = int(df_c_safe['Ounass_Count'].sum())
        if total_levelshoes_products == 0: total_levelshoes_products = int(df_c_safe['LevelShoes_Count'].sum())
        if total_ounass_brands == 0: total_ounass_brands = len(df_c_safe[df_c_safe['Ounass_Count'] > 0])
        if total_levelshoes_brands == 0: total_levelshoes_brands = len(df_c_safe[df_c_safe['LevelShoes_Count'] > 0])
        common_brands_count = len(df_c_safe[(df_c_safe['Ounass_Count'] > 0) & (df_c_safe['LevelShoes_Count'] > 0)])
        ounass_only_count = len(df_c_safe[(df_c_safe['Ounass_Count'] > 0) & (df_c_safe['LevelShoes_Count'] == 0)])
        levelshoes_only_count = len(df_c_safe[(df_c_safe['Ounass_Count'] == 0) & (df_c_safe['LevelShoes_Count'] > 0)])
    # --- End of UPDATED Statistics Calculation ---

    stat_col1, stat_col2, stat_col3 = st.columns(3)
    with stat_col1: st.metric("Ounass Brands", f"{total_ounass_brands:,}"); st.metric("Ounass Products", f"{total_ounass_products:,}")
    with stat_col2: st.metric("Level Shoes Brands", f"{total_levelshoes_brands:,}"); st.metric("Level Shoes Products", f"{total_levelshoes_products:,}")
    with stat_col3:
        if not df_c_safe.empty and 'Ounass_Count' in df_c_safe.columns and 'LevelShoes_Count' in df_c_safe.columns:
            st.metric("Common Brands", f"{common_brands_count:,}")
            st.metric("Ounass Only", f"{ounass_only_count:,}")
            st.metric("Level Shoes Only", f"{levelshoes_only_count:,}")
        else:
             st.metric("Common Brands", "N/A"); st.metric("Ounass Only", "N/A"); st.metric("Level Shoes Only", "N/A")
             if not is_saved_view and (st.session_state.get('ounass_url_input') or st.session_state.get('levelshoes_url_input')):
                 st.caption("Comparison requires data from both sites.")
    st.write(""); st.markdown("---") # Separator after stats

    # Individual Results Display (Only in Live View)
    if not is_saved_view:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Ounass Results")
            if df_ounass is not None and not df_ounass.empty and 'Brand' in df_ounass.columns and 'Count' in df_ounass.columns:
                 st.write(f"Brands Found: {len(df_ounass)}"); df_display = df_ounass.sort_values(by='Count', ascending=False).reset_index(drop=True); df_display.index += 1
                 st.dataframe(df_display[['Brand', 'Count']], height=400, use_container_width=True)
                 csv_buffer = io.StringIO(); df_display[['Brand', 'Count']].to_csv(csv_buffer, index=False, encoding='utf-8'); csv_buffer.seek(0)
                 st.download_button("Download Ounass List (CSV)", csv_buffer.getvalue(), 'ounass_brands.csv', 'text/csv', key='ounass_dl_disp')
            elif process_button and st.session_state.ounass_url_input: st.warning("No data extracted from Ounass.")
            elif not process_button and st.session_state.ounass_url_input: st.info("Click 'Process URLs' to fetch Ounass data.")
            else: st.info("Enter Ounass URL in the input field above.") # Updated text
        with col2:
            st.subheader("Level Shoes Results")
            if df_levelshoes is not None and not df_levelshoes.empty and 'Brand' in df_levelshoes.columns and 'Count' in df_levelshoes.columns:
                 st.write(f"Brands Found: {len(df_levelshoes)}"); df_display = df_levelshoes.sort_values(by='Count', ascending=False).reset_index(drop=True); df_display.index += 1
                 st.dataframe(df_display[['Brand', 'Count']], height=400, use_container_width=True)
                 csv_buffer = io.StringIO(); df_display[['Brand', 'Count']].to_csv(csv_buffer, index=False, encoding='utf-8'); csv_buffer.seek(0)
                 st.download_button("Download Level Shoes List (CSV)", csv_buffer.getvalue(), 'levelshoes_brands.csv', 'text/csv', key='ls_dl_disp')
            elif process_button and st.session_state.levelshoes_url_input: st.warning("No data extracted from Level Shoes.")
            elif not process_button and st.session_state.levelshoes_url_input: st.info("Click 'Process URLs' to fetch Level Shoes data.")
            else: st.info("Enter Level Shoes URL in the input field above.") # Updated text

    # Comparison Section (Show if comparison data exists)
    # (Keep comparison section logic as is)
    if df_comparison_sorted is not None and not df_comparison_sorted.empty:
        if not is_saved_view: st.markdown("---")
        st.subheader("Ounass vs Level Shoes Brand Comparison")
        df_display = df_comparison_sorted.copy(); df_display.index += 1
        display_cols = ['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference']
        missing_cols = [col for col in display_cols if col not in df_display.columns]
        if missing_cols: st.warning(f"Comparison table is missing expected columns: {', '.join(missing_cols)}"); st.dataframe(df_display, height=500, use_container_width=True)
        else: st.dataframe(df_display[display_cols], height=500, use_container_width=True)
        st.markdown("---"); st.subheader("Visual Comparison"); viz_col1, viz_col2 = st.columns(2)
        with viz_col1:
            st.write("**Brand Overlap**"); pie_data = pd.DataFrame({'Category': ['Common Brands', 'Ounass Only', 'Level Shoes Only'],'Count': [common_brands_count, ounass_only_count, levelshoes_only_count]}); pie_data = pie_data[pie_data['Count'] > 0]
            if not pie_data.empty: fig_pie = px.pie(pie_data, names='Category', values='Count', title="Brand Presence", color_discrete_sequence=px.colors.qualitative.Pastel); fig_pie.update_traces(textposition='inside', textinfo='percent+label+value'); st.plotly_chart(fig_pie, use_container_width=True)
            else: st.info("No data available for overlap chart.")
        with viz_col2:
            st.write("**Top 10 Largest Differences (Count)**")
            if 'Difference' in df_comparison_sorted.columns and 'Display_Brand' in df_comparison_sorted.columns:
                top_pos = df_comparison_sorted[df_comparison_sorted['Difference'] > 0].nlargest(5, 'Difference'); top_neg = df_comparison_sorted[df_comparison_sorted['Difference'] < 0].nsmallest(5, 'Difference')
                top_diff = pd.concat([top_pos, top_neg]).sort_values('Difference', ascending=False)
                if not top_diff.empty: fig_diff = px.bar(top_diff, x='Display_Brand', y='Difference', title="Largest Differences (Ounass - Level Shoes)", labels={'Display_Brand': 'Brand', 'Difference': 'Product Count Difference'}, color='Difference', color_continuous_scale=px.colors.diverging.RdBu); fig_diff.update_layout(xaxis_title=None); st.plotly_chart(fig_diff, use_container_width=True)
                else: st.info("No significant differences found for the chart.")
            else: st.info("Difference data unavailable for chart.")
        st.markdown("---"); st.subheader("Top 15 Brands Comparison (Total Products)")
        if not df_comparison_sorted.empty and all(c in df_comparison_sorted.columns for c in ['Display_Brand', 'Ounass_Count', 'LevelShoes_Count']):
            df_comp_copy = df_comparison_sorted.copy(); df_comp_copy['Total_Count'] = df_comp_copy['Ounass_Count'] + df_comp_copy['LevelShoes_Count']; top_n = 15
            top_brands = df_comp_copy.nlargest(top_n, 'Total_Count')
            if not top_brands.empty:
                melted = top_brands.melt(id_vars='Display_Brand', value_vars=['Ounass_Count', 'LevelShoes_Count'], var_name='Website', value_name='Product Count'); melted['Website'] = melted['Website'].str.replace('_Count', '').str.replace('LevelShoes','Level Shoes');
                fig_top = px.bar(melted, x='Display_Brand', y='Product Count', color='Website', barmode='group', title=f"Top {top_n} Brands by Total Products", labels={'Display_Brand': 'Brand'}, category_orders={"Display_Brand": top_brands['Display_Brand'].tolist()}); fig_top.update_layout(xaxis_title=None); st.plotly_chart(fig_top, use_container_width=True)
            else: st.info(f"Not enough data for Top {top_n} brands chart.")
        else: st.info(f"Comparison data unavailable for Top {top_n} chart.")
        st.markdown("---"); col_comp1, col_comp2 = st.columns(2)
        req_cols_exist = all(c in df_comparison_sorted.columns for c in ['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference'])
        with col_comp1:
            st.subheader("Brands in Ounass Only")
            if req_cols_exist:
                df_f = df_comparison_sorted[(df_comparison_sorted['LevelShoes_Count'] == 0) & (df_comparison_sorted['Ounass_Count'] > 0)]
                if not df_f.empty: df_d = df_f[['Display_Brand', 'Ounass_Count']].sort_values('Ounass_Count', ascending=False).reset_index(drop=True); df_d.index += 1; st.dataframe(df_d, height=400, use_container_width=True)
                else: st.info("No unique Ounass brands found in this comparison.")
            else: st.info("Data unavailable.")
        with col_comp2:
            st.subheader("Brands in Level Shoes Only")
            if req_cols_exist:
                df_f = df_comparison_sorted[(df_comparison_sorted['Ounass_Count'] == 0) & (df_comparison_sorted['LevelShoes_Count'] > 0)]
                if not df_f.empty: df_d = df_f[['Display_Brand', 'LevelShoes_Count']].sort_values('LevelShoes_Count', ascending=False).reset_index(drop=True); df_d.index += 1; st.dataframe(df_d, height=400, use_container_width=True)
                else: st.info("No unique Level Shoes brands found in this comparison.")
            else: st.info("Data unavailable.")
        st.markdown("---"); col_comp3, col_comp4 = st.columns(2)
        with col_comp3:
            st.subheader("Common Brands: Ounass > Level Shoes")
            if req_cols_exist:
                df_f = df_comparison_sorted[(df_comparison_sorted['Ounass_Count'] > 0) & (df_comparison_sorted['LevelShoes_Count'] > 0) & (df_comparison_sorted['Difference'] > 0)].sort_values('Difference', ascending=False)
                if not df_f.empty: df_d = df_f[['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference']].reset_index(drop=True); df_d.index += 1; st.dataframe(df_d, height=400, use_container_width=True)
                else: st.info("No common brands found where Ounass has more products.")
            else: st.info("Data unavailable.")
        with col_comp4:
            st.subheader("Common Brands: Level Shoes > Ounass")
            if req_cols_exist:
                df_f = df_comparison_sorted[(df_comparison_sorted['Ounass_Count'] > 0) & (df_comparison_sorted['LevelShoes_Count'] > 0) & (df_comparison_sorted['Difference'] < 0)].sort_values('Difference', ascending=True)
                if not df_f.empty: df_d = df_f[['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference']].reset_index(drop=True); df_d.index += 1; st.dataframe(df_d, height=400, use_container_width=True)
                else: st.info("No common brands found where Level Shoes has more products.")
            else: st.info("Data unavailable.")
        st.markdown("---")
        csv_buffer_comparison = io.StringIO(); dl_cols = ['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference']
        if req_cols_exist:
            df_comparison_sorted[dl_cols].to_csv(csv_buffer_comparison, index=False, encoding='utf-8'); csv_buffer_comparison.seek(0)
            download_label = f"Download {'Saved' if is_saved_view else 'Current'} Comparison (CSV)"
            view_id_part = saved_meta['id'] if is_saved_view and saved_meta else 'live'
            download_key = f"comp_dl_button_{'saved' if is_saved_view else 'live'}_{view_id_part}"
            filename_desc = f"{detected_gender or 'All'}_{detected_category or 'All'}".replace(' > ','-').replace(' ','_').lower()
            download_filename = f"brand_comparison_{filename_desc}_{view_id_part}.csv".replace('?_?', 'all_all')
            st.download_button(download_label, csv_buffer_comparison.getvalue(), download_filename, 'text/csv', key=download_key)
        else: st.warning("Could not generate download file due to missing comparison columns.")
    elif process_button and not is_saved_view:
        st.markdown("---")
        st.warning("Comparison could not be generated. Check if data was successfully extracted from both URLs in the sections above.")
    # elif not process_button and not is_saved_view and df_o_safe.empty and df_l_safe.empty :
        # Initial state message handled in the main flow now

# --- Time Comparison Display Function ---
# (Keep this function as is)
def display_time_comparison_results(df_time_comp, meta1, meta2):
    st.markdown("---"); st.subheader("Snapshot Comparison Over Time")
    ts_format = '%Y-%m-%d %H:%M'; ts1_str, ts2_str = "N/A", "N/A"; id1, id2 = meta1.get('id','N/A'), meta2.get('id','N/A')
    try:
        if meta1 and 'timestamp' in meta1: ts1_str = datetime.fromisoformat(meta1['timestamp']).strftime(ts_format)
        if meta2 and 'timestamp' in meta2: ts2_str = datetime.fromisoformat(meta2['timestamp']).strftime(ts_format)
        st.markdown(f"Comparing **Snapshot 1** (`{ts1_str}`, ID: {id1}) **vs** **Snapshot 2** (`{ts2_str}`, ID: {id2})")
    except Exception as e: st.warning(f"Error formatting timestamps: {e}"); st.markdown(f"Comparing Snapshot 1 (ID: {id1}) vs Snapshot 2 (ID: {id2})")
    with st.expander("Show URLs for Compared Snapshots"):
        st.caption(f"**Snap 1 ({ts1_str}):** O: `{meta1.get('ounass_url', 'N/A')}` | LS: `{meta1.get('levelshoes_url', 'N/A')}`")
        st.caption(f"**Snap 2 ({ts2_str}):** O: `{meta2.get('ounass_url', 'N/A')}` | LS: `{meta2.get('levelshoes_url', 'N/A')}`")
    st.markdown("---")
    req_time_cols = ['Display_Brand','Ounass_Count_T1','Ounass_Count_T2','Ounass_Change','LevelShoes_Count_T1','LevelShoes_Count_T2','LevelShoes_Change']
    if not all(col in df_time_comp.columns for col in req_time_cols): st.error("Time comparison data is missing required columns. Cannot display detailed changes."); return
    new_o=df_time_comp[(df_time_comp['Ounass_Count_T1']==0)&(df_time_comp['Ounass_Count_T2']>0)]; drop_o=df_time_comp[(df_time_comp['Ounass_Count_T1']>0)&(df_time_comp['Ounass_Count_T2']==0)]; inc_o=df_time_comp[(df_time_comp['Ounass_Change']>0) & (df_time_comp['Ounass_Count_T1'] > 0)]; dec_o=df_time_comp[(df_time_comp['Ounass_Change']<0)]
    new_l=df_time_comp[(df_time_comp['LevelShoes_Count_T1']==0)&(df_time_comp['LevelShoes_Count_T2']>0)]; drop_l=df_time_comp[(df_time_comp['LevelShoes_Count_T1']>0)&(df_time_comp['LevelShoes_Count_T2']==0)]; inc_l=df_time_comp[(df_time_comp['LevelShoes_Change']>0) & (df_time_comp['LevelShoes_Count_T1'] > 0)]; dec_l=df_time_comp[(df_time_comp['LevelShoes_Change']<0)]
    st.subheader("Summary of Changes"); t_stat_col1, t_stat_col2 = st.columns(2)
    with t_stat_col1: st.metric("New Brands (Ounass)", len(new_o)); st.metric("Dropped Brands (Ounass)", len(drop_o)); st.metric("Increased Brands (Ounass)", len(inc_o)); st.metric("Decreased Brands (Ounass)", len(dec_o[dec_o['Ounass_Count_T2'] > 0])); st.metric("Net Product Change (Ounass)", f"{df_time_comp['Ounass_Change'].sum():+,}")
    with t_stat_col2: st.metric("New Brands (Level Shoes)", len(new_l)); st.metric("Dropped Brands (Level Shoes)", len(drop_l)); st.metric("Increased Brands (Level Shoes)", len(inc_l)); st.metric("Decreased Brands (Level Shoes)", len(dec_l[dec_l['LevelShoes_Count_T2'] > 0])); st.metric("Net Product Change (Level Shoes)", f"{df_time_comp['LevelShoes_Change'].sum():+,}")
    st.markdown("---"); st.subheader("Detailed Brand Changes"); tc_col1, tc_col2 = st.columns(2); height=250
    def display_change_df(df_change, category_name, count_col_t1, count_col_t2, change_col, sort_col, sort_ascending, rename_map):
        if not df_change.empty:
            st.write(f"{category_name} ({len(df_change)}):"); display_cols = ['Display_Brand'];
            if count_col_t1: display_cols.append(count_col_t1);
            if count_col_t2: display_cols.append(count_col_t2);
            if change_col: display_cols.append(change_col);
            df = df_change[display_cols].rename(columns=rename_map).sort_values(sort_col, ascending=sort_ascending).reset_index(drop=True); df.index += 1
            st.dataframe(df, height=height, use_container_width=True); return True
        return False
    with tc_col1:
        st.write("**Ounass Changes**"); displayed_any_o = False; rename_new_drop = {'Ounass_Count_T1':'Was', 'Ounass_Count_T2':'Now'}; rename_inc_dec = {'Ounass_Count_T1':'Was', 'Ounass_Count_T2':'Now', 'Ounass_Change':'Change'}
        if display_change_df(new_o, "New", None, 'Ounass_Count_T2', None, 'Now', False, rename_new_drop): displayed_any_o = True
        if display_change_df(drop_o, "Dropped", 'Ounass_Count_T1', None, None, 'Was', False, rename_new_drop): displayed_any_o = True
        if display_change_df(inc_o, "Increased", 'Ounass_Count_T1', 'Ounass_Count_T2', 'Ounass_Change', 'Change', False, rename_inc_dec): displayed_any_o = True
        dec_o_display = dec_o[dec_o['Ounass_Count_T2'] > 0]
        if display_change_df(dec_o_display, "Decreased", 'Ounass_Count_T1', 'Ounass_Count_T2', 'Ounass_Change', 'Change', True, rename_inc_dec): displayed_any_o = True
        if not displayed_any_o: st.info("No significant changes detected for Ounass between these snapshots.")
    with tc_col2:
        st.write("**Level Shoes Changes**"); displayed_any_l = False; rename_new_drop = {'LevelShoes_Count_T1':'Was', 'LevelShoes_Count_T2':'Now'}; rename_inc_dec = {'LevelShoes_Count_T1':'Was', 'LevelShoes_Count_T2':'Now', 'LevelShoes_Change':'Change'}
        if display_change_df(new_l, "New", None, 'LevelShoes_Count_T2', None, 'Now', False, rename_new_drop): displayed_any_l = True
        if display_change_df(drop_l, "Dropped", 'LevelShoes_Count_T1', None, None, 'Was', False, rename_new_drop): displayed_any_l = True
        if display_change_df(inc_l, "Increased", 'LevelShoes_ArtT1', 'LevelShoes_Count_T2', 'LevelShoes_Change', 'Change', False, rename_inc_dec): displayed_any_l = True # Typo corrected
        dec_l_display = dec_l[dec_l['LevelShoes_Count_T2'] > 0]
        if display_change_df(dec_l_display, "Decreased", 'LevelShoes_Count_T1', 'LevelShoes_Count_T2', 'LevelShoes_Change', 'Change', True, rename_inc_dec): displayed_any_l = True
        if not displayed_any_l: st.info("No significant changes detected for Level Shoes between these snapshots.")
    st.markdown("---"); csv_buffer = io.StringIO()
    if all(col in df_time_comp.columns for col in req_time_cols):
        df_time_comp[req_time_cols].to_csv(csv_buffer, index=False, encoding='utf-8'); csv_buffer.seek(0)
        st.download_button(label=f"Download Time Comparison ({ts1_str} vs {ts2_str})", data=csv_buffer.getvalue(), file_name=f"time_comparison_{id1}_vs_{id2}.csv", mime='text/csv', key='time_comp_dl_button')
    else: st.warning("Could not generate download file for time comparison due to missing data.")

# --- Main Application Flow ---
# (Keep this logic flow as is)
confirm_id = st.session_state.get('confirm_delete_id')
if confirm_id:
    st.warning(f"Are you sure you want to delete comparison ID {confirm_id}?"); col_confirm, col_cancel, _ = st.columns([1,1,3])
    with col_confirm:
        if st.button("Yes, Delete", type="primary", key=f"confirm_delete_{confirm_id}"):
            if delete_comparison(confirm_id): st.success(f"Comparison ID {confirm_id} deleted.")
            else: st.error("Deletion failed.")
            st.session_state.confirm_delete_id = None; st.query_params.clear(); st.rerun()
    with col_cancel:
        if st.button("Cancel", key=f"cancel_delete_{confirm_id}"): st.session_state.confirm_delete_id = None; st.rerun()
elif 'df_time_comparison' in st.session_state and not st.session_state.df_time_comparison.empty:
    display_time_comparison_results(st.session_state.df_time_comparison, st.session_state.get('time_comp_meta1',{}), st.session_state.get('time_comp_meta2',{}))
elif viewing_saved_id:
    saved_meta, saved_df = load_specific_comparison(viewing_saved_id)
    if saved_meta and saved_df is not None: display_all_results(None, None, saved_df, stats_title_prefix="Saved Comparison Details", is_saved_view=True, saved_meta=saved_meta)
    else: st.error(f"Could not load comparison ID: {viewing_saved_id}. It might have been deleted or there was a load error.");
    if st.button("Clear Invalid Saved View URL"): st.query_params.clear(); st.rerun()
else:
    if process_button: # Check if the button in the main area was clicked
        st.session_state.df_ounass = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned']); st.session_state.df_levelshoes = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned'])
        st.session_state.ounass_data = []; st.session_state.levelshoes_data = []; st.session_state.df_comparison_sorted = pd.DataFrame(); st.session_state.processed_ounass_url = ''
        st.session_state.df_time_comparison = pd.DataFrame(); st.session_state.time_comp_id1 = None; st.session_state.time_comp_id2 = None; st.session_state.selected_url_key_for_time_comp = None; st.session_state.time_comp_meta1 = {}; st.session_state.time_comp_meta2 = {}
        if st.session_state.ounass_url_input:
            with st.spinner("Processing Ounass URL..."):
                st.session_state.processed_ounass_url = ensure_ounass_full_list_parameter(st.session_state.ounass_url_input)
                ounass_html_content = fetch_html_content(st.session_state.processed_ounass_url)
                if ounass_html_content:
                    st.session_state.ounass_data = process_ounass_html(ounass_html_content)
                    if st.session_state.ounass_data:
                         try:
                             st.session_state.df_ounass = pd.DataFrame(st.session_state.ounass_data)
                             if not st.session_state.df_ounass.empty: st.session_state.df_ounass['Brand_Cleaned'] = st.session_state.df_ounass['Brand'].apply(clean_brand_name)
                             else: st.warning("Ounass data extracted but resulted in an empty list.")
                         except Exception as e: st.error(f"Error creating Ounass DataFrame: {e}"); st.session_state.df_ounass = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned'])
        if st.session_state.levelshoes_url_input:
             with st.spinner("Processing Level Shoes URL (using __NEXT_DATA__)..."):
                levelshoes_html_content = fetch_html_content(st.session_state.levelshoes_url_input)
                if levelshoes_html_content:
                    st.session_state.levelshoes_data = process_levelshoes_html(levelshoes_html_content)
                    if st.session_state.levelshoes_data:
                        try:
                            st.session_state.df_levelshoes = pd.DataFrame(st.session_state.levelshoes_data)
                            if not st.session_state.df_levelshoes.empty: st.session_state.df_levelshoes['Brand_Cleaned'] = st.session_state.df_levelshoes['Brand'].apply(clean_brand_name)
                            else: st.warning("Level Shoes data extracted but resulted in an empty list.")
                        except Exception as e: st.error(f"Error creating Level Shoes DataFrame: {e}"); st.session_state.df_levelshoes = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned'])
        if ('df_ounass' in st.session_state and not st.session_state.df_ounass.empty and 'df_levelshoes' in st.session_state and not st.session_state.df_levelshoes.empty):
            with st.spinner("Generating comparison..."):
                try:
                    df_o = st.session_state.df_ounass[['Brand','Count','Brand_Cleaned']].copy(); df_l = st.session_state.df_levelshoes[['Brand','Count','Brand_Cleaned']].copy()
                    df_comp = pd.merge(df_o, df_l, on='Brand_Cleaned', how='outer', suffixes=('_Ounass', '_LevelShoes'))
                    df_comp['Ounass_Count'] = df_comp['Count_Ounass'].fillna(0).astype(int); df_comp['LevelShoes_Count'] = df_comp['Count_LevelShoes'].fillna(0).astype(int); df_comp['Difference'] = df_comp['Ounass_Count'] - df_comp['LevelShoes_Count']
                    df_comp['Display_Brand'] = np.where(df_comp['Ounass_Count'] > 0, df_comp['Brand_Ounass'], df_comp['Brand_LevelShoes']); df_comp['Display_Brand'].fillna(df_comp['Brand_Cleaned'], inplace=True); df_comp['Display_Brand'].fillna("Unknown", inplace=True)
                    final_cols = ['Display_Brand','Brand_Cleaned','Ounass_Count','LevelShoes_Count','Difference','Brand_Ounass','Brand_LevelShoes']
                    for col in final_cols:
                        if col not in df_comp.columns: df_comp[col] = np.nan
                    df_comp['Total_Count'] = df_comp['Ounass_Count'] + df_comp['LevelShoes_Count']
                    st.session_state.df_comparison_sorted = df_comp.sort_values(by=['Total_Count', 'Ounass_Count', 'Display_Brand'], ascending=[False, False, True]).reset_index(drop=True)[final_cols + ['Total_Count']]
                except Exception as merge_e: st.error(f"Error during comparison merge: {merge_e}"); st.session_state.df_comparison_sorted = pd.DataFrame()
        else:
             st.session_state.df_comparison_sorted = pd.DataFrame()
             if process_button:
                 if st.session_state.ounass_url_input and ('df_ounass' not in st.session_state or st.session_state.df_ounass.empty): st.warning("Could not process Ounass data. Comparison not generated.")
                 if st.session_state.levelshoes_url_input and ('df_levelshoes' not in st.session_state or st.session_state.df_levelshoes.empty): st.warning("Could not process Level Shoes data. Comparison not generated.")
        st.rerun() # Rerun after processing to display results

    # Display current live data (or empty state)
    df_ounass_live = st.session_state.get('df_ounass', pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned']))
    df_levelshoes_live = st.session_state.get('df_levelshoes', pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned']))
    df_comparison_sorted_live = st.session_state.get('df_comparison_sorted', pd.DataFrame())
    display_all_results(df_ounass_live, df_levelshoes_live, df_comparison_sorted_live, stats_title_prefix="Current Comparison")

    # Show initial message only if app just loaded and process button wasn't clicked
    if not process_button and df_ounass_live.empty and df_levelshoes_live.empty and df_comparison_sorted_live.empty:
        # This message is implicitly handled now by the info messages under "Ounass Results" and "Level Shoes Results" when empty
        pass # st.info("Enter URLs above and click 'Process URLs' or select a saved comparison from the sidebar.")


# --- END OF UPDATED FILE ---
