# --- START OF REFACTORED combined_extractor_app.py ---

import streamlit as st
# Removed: from bs4 import BeautifulSoup # Now in extractors
# Removed: import re                   # Now in ounass_extractor
# Removed: import json                 # Now in levelshoes_extractor
import pandas as pd
import io
from thefuzz import process, fuzz
import unicodedata
import requests
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
import plotly.express as px
import numpy as np
import psycopg2 # For PostgreSQL connection
import psycopg2.extras # For dictionary cursor
from datetime import datetime
from collections import defaultdict
import os # Potentially useful for local testing with env vars

# --- NEW IMPORTS ---
import ounass_extractor
import levelshoes_extractor
# --- END NEW IMPORTS ---

# --- App Configuration ---
APP_VERSION = "2.6.0" # Updated version: Refactored extractors
st.set_page_config(layout="wide", page_title="Ounass vs Level Shoes PLP Comparison")

# --- App Title and Info ---
st.title(f"Ounass vs Level Shoes PLP Designer Comparison (v{APP_VERSION})")
st.write("Enter Product Listing Page (PLP) URLs from Ounass and Level Shoes (Women's Shoes/Bags recommended) to extract and compare designer brand counts, or compare previously saved snapshots.")
st.info("Ensure the URLs point to the relevant listing pages. For Ounass, the tool will attempt to load all designers. Level Shoes extraction uses the new __NEXT_DATA__ method. Comparison history is now stored externally.")

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
if 'time_comp_meta1' not in st.session_state: st.session_state.time_comp_meta1 = {}
if 'time_comp_meta2' not in st.session_state: st.session_state.time_comp_meta2 = {}
if 'df_ounass_processed' not in st.session_state: st.session_state.df_ounass_processed = False
if 'df_levelshoes_processed' not in st.session_state: st.session_state.df_levelshoes_processed = False
if 'selections_by_group' not in st.session_state: st.session_state.selections_by_group = {}
if 'show_saved_comparisons' not in st.session_state: st.session_state.show_saved_comparisons = False


# --- URL Input Section (Show only when not viewing saved and not comparing time) ---
viewing_saved_id_check = st.query_params.get("view_id", [None])[0]
process_button = False # Default value
if not viewing_saved_id_check and st.session_state.get('df_time_comparison', pd.DataFrame()).empty:
    st.markdown("---") # Separator
    st.subheader("Enter URLs to Compare")
    col1, col2 = st.columns(2)
    with col1:
        st.session_state.ounass_url_input = st.text_input("Ounass URL", key="ounass_url_widget_main", value=st.session_state.ounass_url_input, placeholder="https://www.ounass.ae/...")
    with col2:
        st.session_state.levelshoes_url_input = st.text_input("Level Shoes URL", key="levelshoes_url_widget_main", value=st.session_state.levelshoes_url_input, placeholder="https://www.levelshoes.com/...")
    process_button = st.button("Process URLs", key="process_button_main")
    st.markdown("---") # Separator before results
# --- End URL Input Section ---


# --- Database Setup & Functions (PostgreSQL Version) ---
@st.cache_resource
def get_connection_details():
    try:
        if hasattr(st, 'secrets') and "connections" in st.secrets and "postgres" in st.secrets["connections"]:
             return st.secrets["connections"]["postgres"]["url"]
        else:
             st.error("Database connection details not found in Streamlit Secrets.")
             return None
    except Exception as e: st.error(f"Error accessing connection details: {e}"); return None

def get_db_connection():
    db_url = get_connection_details()
    if not db_url: return None
    try:
        conn = psycopg2.connect(db_url, sslmode='require')
        return conn
    except psycopg2.OperationalError as e:
        st.error(f"Database Connection Error: Could not connect. Check secrets/credentials.")
        return None
    except Exception as e:
        st.error(f"Unexpected Database Connection Error.")
        return None

def init_db():
    conn = get_db_connection()
    if conn is None: return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS comparisons (
                    id SERIAL PRIMARY KEY, timestamp TIMESTAMPTZ NOT NULL, ounass_url TEXT NOT NULL,
                    levelshoes_url TEXT NOT NULL, comparison_data JSONB NOT NULL, comparison_name TEXT
                );
            """)
        conn.commit()
    except Exception as e:
        st.error(f"Fatal DB Init Error: {e}")
        try: conn.rollback()
        except Exception as rb_e: st.error(f"Rollback failed after init error: {rb_e}")
    finally:
        if conn: conn.close()

def save_comparison(ounass_url, levelshoes_url, df_comparison):
    if df_comparison is None or df_comparison.empty: st.error("Cannot save empty comparison data."); return False
    conn = get_db_connection()
    if conn is None: return False
    try:
        timestamp = datetime.now()
        df_to_save = df_comparison.copy()
        cols_to_save = ['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference', 'Brand_Cleaned', 'Brand_Ounass', 'Brand_LevelShoes']
        for col in cols_to_save:
            if col not in df_to_save.columns: df_to_save[col] = np.nan
        data_json = df_to_save[cols_to_save].to_json(orient="records", date_format="iso", default_handler=str)
        with conn.cursor() as cur:
            sql = "INSERT INTO comparisons (timestamp, ounass_url, levelshoes_url, comparison_data, comparison_name) VALUES (%s, %s, %s, %s, %s)"
            cur.execute(sql, (timestamp, ounass_url, levelshoes_url, data_json, None))
        conn.commit()
        load_saved_comparisons_meta.clear() # Clear cache after saving
        return True
    except Exception as e:
        st.error(f"Database Error: Could not save comparison - {e}")
        try: conn.rollback()
        except Exception as rb_e: st.error(f"Rollback failed after save error: {rb_e}")
        return False
    finally:
        if conn: conn.close()

@st.cache_data(ttl=300) # Cache the list for 5 minutes unless manually refreshed
def load_saved_comparisons_meta():
    conn = get_db_connection()
    if conn is None: return []
    comparisons_list = []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT id, timestamp, ounass_url, levelshoes_url, comparison_name FROM comparisons ORDER BY timestamp DESC")
            comparisons = cur.fetchall()
            comparisons_list = [dict(row) for row in comparisons] if comparisons else []
    except psycopg2.Error as e:
        st.error(f"Database Error loading comparisons: {e}")
        if "relation" in str(e) and "does not exist" in str(e):
            st.warning("The 'comparisons' table might not exist. Attempting to initialize...")
            init_db()
        comparisons_list = []
    except Exception as e:
        st.error(f"Unexpected Error loading comparisons: {e}")
        comparisons_list = []
    finally:
        if conn: conn.close()
    return comparisons_list

@st.cache_data(ttl=600)
def load_specific_comparison(comp_id):
    st.info(f"Loading details for saved comparison ID: {comp_id}")
    conn = get_db_connection()
    if conn is None: return None, None
    meta, df = None, None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            sql = "SELECT id, timestamp, ounass_url, levelshoes_url, comparison_data, comparison_name FROM comparisons WHERE id = %s"
            cur.execute(sql, (comp_id,))
            comp = cur.fetchone()
            if comp:
                comp_dict = dict(comp)
                fallback_name = f"ID {comp_dict['id']} ({comp_dict['timestamp']})"
                meta = {"timestamp": comp_dict["timestamp"], "ounass_url": comp_dict["ounass_url"], "levelshoes_url": comp_dict["levelshoes_url"], "name": comp_dict["comparison_name"] or fallback_name, "id": comp_dict["id"]}
                json_data = comp_dict["comparison_data"]
                if isinstance(json_data, str): df = pd.read_json(io.StringIO(json_data), orient="records")
                elif isinstance(json_data, (list, dict)): df = pd.DataFrame(json_data)
                else: st.error(f"Unexpected data type for comparison_data: {type(json_data)}"); df = pd.DataFrame()

                if not df.empty:
                    if 'Difference' not in df.columns and 'Ounass_Count' in df.columns and 'LevelShoes_Count' in df.columns: df['Difference'] = df['Ounass_Count'] - df['LevelShoes_Count']
                    if 'Display_Brand' not in df.columns:
                        brand_ounass_col = 'Brand_Ounass' if 'Brand_Ounass' in df.columns else None
                        brand_ls_col = 'Brand_LevelShoes' if 'Brand_LevelShoes' in df.columns else None
                        brand_cleaned_col = 'Brand_Cleaned' if 'Brand_Cleaned' in df.columns else None
                        df['Display_Brand'] = np.where(df.get('Ounass_Count', 0) > 0, df[brand_ounass_col] if brand_ounass_col else df.get(brand_cleaned_col), df[brand_ls_col] if brand_ls_col else df.get(brand_cleaned_col))
                        df['Display_Brand'].fillna("Unknown", inplace=True)
            else: st.warning(f"Saved comparison with ID {comp_id} not found.")
    except Exception as e: st.error(f"Database Error: Could not load comparison ID {comp_id} - {e}")
    finally:
        if conn: conn.close()
    return meta, df

def delete_comparison(comp_id):
    conn = get_db_connection()
    if conn is None: return False
    success = False
    try:
        with conn.cursor() as cur:
            sql = "DELETE FROM comparisons WHERE id = %s"
            cur.execute(sql, (comp_id,))
        conn.commit()
        load_saved_comparisons_meta.clear()
        load_specific_comparison.clear() # Clear all args for this cache
        success = True
    except Exception as e:
        st.error(f"Database Error: Could not delete comparison ID {comp_id} - {e}")
        try: conn.rollback()
        except Exception as rb_e: st.error(f"Rollback failed after delete error: {rb_e}")
    finally:
        if conn: conn.close()
    return success

# --- Helper Functions ---
def clean_brand_name(brand_name):
    if not isinstance(brand_name, str): return ""
    cleaned = brand_name.upper().replace('-', '').replace('&', '').replace('.', '').replace("'", '').replace(" ", '')
    cleaned = unicodedata.normalize('NFKD', cleaned).encode('ascii', 'ignore').decode('utf-8')
    cleaned = ''.join(c for c in cleaned if c.isalnum())
    return cleaned
def custom_scorer(s1, s2):
    scores = [fuzz.ratio(s1, s2), fuzz.partial_ratio(s1, s2), fuzz.token_set_ratio(s1, s2), fuzz.token_sort_ratio(s1, s2)]
    return max(scores)

def handle_checkbox_change(group_key, comp_id):
    checkbox_state_key = f"cb_{comp_id}"
    current_state = st.session_state.get(checkbox_state_key, False)
    st.session_state.selections_by_group.setdefault(group_key, set())
    selections = st.session_state.selections_by_group[group_key]
    if current_state:
        if len(selections) >= 2 and comp_id not in selections:
            st.warning("You can only select two snapshots for comparison.")
            st.session_state[checkbox_state_key] = False
        else:
            selections.add(comp_id)
    else:
        selections.discard(comp_id)

# --- REMOVED HTML Processing Function Definitions ---
# get_processed_ounass_data and get_processed_levelshoes_data are now imported

# --- Function to fetch HTML content from URL ---
@st.cache_data(ttl=600) # Cache for 10 minutes
def fetch_html_content(url):
    if not url: st.error("Fetch error: URL cannot be empty."); return None
    # st.info(f"Fetching fresh data for: {url}") # Can be noisy, removed for now
    try:
        headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8', 'Accept-Language': 'en-US,en;q=0.9', 'Connection': 'keep-alive' }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.exceptions.Timeout: st.error(f"Error: Timeout fetching {url}"); return None
    except requests.exceptions.RequestException as e: st.error(f"Error fetching {url}: {e}"); return None
    except Exception as e: st.error(f"Unexpected error during fetch: {e}"); return None

# --- Function to ensure Ounass URL has the correct parameter ---
def ensure_ounass_full_list_parameter(url):
    param_key = 'fh_maxdisplaynrvalues_designer'; param_value = '-1'
    try:
        if not url or 'ounass' not in urlparse(url).netloc.lower(): return url
    except Exception: return url
    try:
        parsed_url = urlparse(url); query_params = parse_qs(parsed_url.query, keep_blank_values=True)
        needs_update = (param_key not in query_params or not query_params[param_key] or query_params[param_key][0] != param_value)
        if needs_update:
            query_params[param_key] = [param_value]; new_query_string = urlencode(query_params, doseq=True)
            url_components = list(parsed_url); url_components[4] = new_query_string
            return urlunparse(url_components)
        else: return url
    except Exception as e: st.warning(f"Error processing Ounass URL parameters: {e}"); return url

# --- URL Info Extraction Function ---
def extract_info_from_url(url):
    try:
        if not url: return None, None
        parsed = urlparse(url); ignore_segments = ['ae', 'com', 'en', 'shop', 'category', 'all', 'view-all', 'plp', 'sale']; path_segments = [s for s in parsed.path.lower().split('/') if s and s not in ignore_segments]
        if not path_segments: return None, None
        gender_keywords = ["women", "men", "kids", "unisex"]; gender = None; category_parts_raw = []
        if path_segments and path_segments[0] in gender_keywords: gender = path_segments[0].title(); category_parts_raw = path_segments[1:]
        else: category_parts_raw = path_segments
        cleaned_category_parts = []
        for part in category_parts_raw:
            if gender and part in gender_keywords: continue
            cleaned = part.replace('.html', '').replace('-', ' ').strip()
            if cleaned: cleaned_category_parts.append(' '.join(word.capitalize() for word in cleaned.split()))
        category = " > ".join(cleaned_category_parts) if cleaned_category_parts else None
        return gender, category
    except Exception: return None, None

# --- Sidebar ---
st.sidebar.image("https://1000logos.net/wp-content/uploads/2021/05/Ounass-logo.png", width=150)
st.sidebar.caption(f"App Version: {APP_VERSION}")
# --- Back Button Logic ---
if viewing_saved_id_check or not st.session_state.get('df_time_comparison', pd.DataFrame()).empty:
    if st.sidebar.button("<< Back to Live Processing", key="back_live", use_container_width=True):
        st.query_params.clear(); st.session_state.confirm_delete_id = None
        st.session_state.df_time_comparison = pd.DataFrame(); st.session_state.time_comp_meta1 = {}; st.session_state.time_comp_meta2 = {}
        st.session_state.show_saved_comparisons = False; st.session_state.selections_by_group = {}
        st.rerun()
# --- Saved Comparisons Sidebar Section ---
st.sidebar.markdown("---")
st.sidebar.subheader("Saved Comparisons")
if not st.session_state.get('show_saved_comparisons', False):
    if st.sidebar.button("Load Saved Comparisons", key="load_saved_btn", use_container_width=True):
        st.session_state.show_saved_comparisons = True
        st.rerun()
else:
    if st.sidebar.button("Hide Saved Comparisons", key="hide_saved_btn", use_container_width=True):
        st.session_state.show_saved_comparisons = False
        st.session_state.selections_by_group = {} # Clear selections when hiding
        st.rerun()

    # Load data ONLY when this block is active
    saved_comps_meta = load_saved_comparisons_meta() # Cached function

    if not saved_comps_meta:
        st.sidebar.caption("No comparisons found in the database.")
    else:
        grouped_comps = defaultdict(list)
        for comp_meta in saved_comps_meta: url_key = (comp_meta.get('ounass_url',''), comp_meta.get('levelshoes_url','')); grouped_comps[url_key].append(comp_meta)
        if 'selections_by_group' not in st.session_state: st.session_state.selections_by_group = {}
        st.sidebar.caption("Select two snapshots below to compare.")
        url_group_keys = list(grouped_comps.keys())
        for idx, url_key in enumerate(url_group_keys):
            comps_list = grouped_comps[url_key]; g, c = extract_info_from_url(url_key[0] or url_key[1]); expander_label = f"{g or '?'} / {c or '?'} ({len(comps_list)} snapshots)"
            if not g and not c: oun_path_part = urlparse(url_key[0]).path.split('/')[-1].replace('.html','') or "Ounass"; ls_path_part = urlparse(url_key[1]).path.split('/')[-1].replace('.html','') or "Level"; expander_label = f"{oun_path_part} vs {ls_path_part} ({len(comps_list)} snapshots)"
            with st.sidebar.expander(expander_label, expanded=True):
                st.session_state.selections_by_group.setdefault(url_key, set())
                current_selections = st.session_state.selections_by_group[url_key]
                st.write("Select two snapshots:")
                for comp_meta in sorted(comps_list, key=lambda x: x['timestamp']):
                     comp_id = comp_meta['id']; ts = comp_meta['timestamp']; display_ts_str="Invalid Date"
                     try:
                         if isinstance(ts, datetime): display_ts_str = ts.strftime('%Y-%m-%d %H:%M')
                         else: display_ts_str = datetime.fromisoformat(str(ts)).strftime('%Y-%m-%d %H:%M')
                     except Exception: pass
                     display_label = f"{display_ts_str} (ID: {comp_id})"; is_currently_selected_in_state = comp_id in current_selections
                     col_cb, col_view, col_del = st.columns([0.15, 0.7, 0.15])
                     with col_cb:
                          st.checkbox(" ", key=f"cb_{comp_id}", value=is_currently_selected_in_state, on_change=handle_checkbox_change, args=(url_key, comp_id), label_visibility="collapsed")
                     with col_view:
                          is_being_viewed = str(comp_id) == viewing_saved_id_check; button_type = "primary" if is_being_viewed else "secondary"
                          if st.button(display_label, key=f"view_detail_{comp_id}", type=button_type, use_container_width=True):
                               st.query_params["view_id"] = str(comp_id); st.session_state.confirm_delete_id = None; st.session_state.df_time_comparison = pd.DataFrame(); st.rerun()
                     with col_del:
                          if st.button("🗑️", key=f"del_detail_{comp_id}", help=f"Delete snapshot from {display_ts_str}", use_container_width=True):
                               st.session_state.confirm_delete_id = comp_id; st.query_params.clear(); st.rerun()
                st.markdown("---")
                selected_ids_list = list(current_selections); compare_button_disabled = (len(selected_ids_list) != 2)
                if st.button("Compare Selected Snapshots", key=f"compare_chk_{idx}", disabled=compare_button_disabled, use_container_width=True):
                     if len(selected_ids_list) == 2:
                         id1, id2 = selected_ids_list[0], selected_ids_list[1]
                         meta1, df1 = load_specific_comparison(id1); meta2, df2 = load_specific_comparison(id2) # Cached
                         if meta1 and df1 is not None and meta2 and df2 is not None:
                             ts1 = meta1['timestamp']; ts2 = meta2['timestamp']
                             if isinstance(ts1, str): ts1 = datetime.fromisoformat(ts1)
                             if isinstance(ts2, str): ts2 = datetime.fromisoformat(ts2)
                             if ts1 > ts2: id1, id2, meta1, df1, meta2, df2 = id2, id1, meta2, df2, meta1, df1
                             for df_check in [df1, df2]:
                                 if 'Display_Brand' not in df_check.columns: df_check['Display_Brand'] = df_check['Brand_Ounass'].fillna(df_check['Brand_LevelShoes']).fillna(df_check.get('Brand_Cleaned', "Unknown")); df_check['Display_Brand'].fillna("Unknown", inplace=True)
                                 if 'Ounass_Count' not in df_check.columns: df_check['Ounass_Count'] = 0
                                 if 'LevelShoes_Count' not in df_check.columns: df_check['LevelShoes_Count'] = 0
                             df_time = pd.merge(df1[['Display_Brand','Ounass_Count','LevelShoes_Count']], df2[['Display_Brand','Ounass_Count','LevelShoes_Count']], on='Display_Brand', how='outer', suffixes=('_T1','_T2')); df_time.fillna(0, inplace=True)
                             df_time['Ounass_Change'] = (df_time['Ounass_Count_T2'] - df_time['Ounass_Count_T1']).astype(int); df_time['LevelShoes_Change'] = (df_time['LevelShoes_Count_T2'] - df_time['LevelShoes_Count_T1']).astype(int)
                             st.session_state.df_time_comparison = df_time; st.session_state.time_comp_meta1 = meta1; st.session_state.time_comp_meta2 = meta2
                             st.query_params.clear()
                             st.session_state.selections_by_group[url_key] = set() # Clear selections
                             st.rerun()
                         else: st.error("Failed to load data for one or both selected snapshots.")
                     else: st.warning("Please select exactly two snapshots to compare.")


# --- Unified Display Function ---
def display_all_results(df_ounass, df_levelshoes, df_comparison_sorted, stats_title_prefix="Overall Statistics", is_saved_view=False, saved_meta=None):
    stats_title = stats_title_prefix
    detected_gender, detected_category = None, None
    if is_saved_view and saved_meta:
         oun_g, oun_c = extract_info_from_url(saved_meta.get('ounass_url', ''))
         ls_g, ls_c = extract_info_from_url(saved_meta.get('levelshoes_url', ''))
         if oun_g or ls_g: detected_gender = oun_g or ls_g
         if oun_c or ls_c: detected_category = oun_c or ls_c
         ts = saved_meta.get('timestamp', 'N/A'); display_ts_str="N/A"
         try:
             if isinstance(ts, datetime): display_ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')
             else: display_ts_str = datetime.fromisoformat(str(ts)).strftime('%Y-%m-%d %H:%M:%S')
         except Exception: pass
         st.subheader(f"Viewing Saved Comparison ({display_ts_str})")
         st.caption(f"Ounass URL: `{saved_meta.get('ounass_url', 'N/A')}`")
         st.caption(f"Level Shoes URL: `{saved_meta.get('levelshoes_url', 'N/A')}`")
    else:
        url_for_stats = st.session_state.get('processed_ounass_url') or st.session_state.get('ounass_url_input')
        if not url_for_stats: url_for_stats = st.session_state.get('levelshoes_url_input')
        if url_for_stats: g_live, c_live = extract_info_from_url(url_for_stats); detected_gender = g_live; detected_category = c_live
    if detected_gender and detected_category: stats_title = f"{stats_title_prefix} - {detected_gender} / {detected_category}"
    elif detected_gender: stats_title = f"{stats_title_prefix} - {detected_gender}"
    elif detected_category: stats_title = f"{stats_title_prefix} - {detected_category}"
    if not is_saved_view and df_comparison_sorted is not None and not df_comparison_sorted.empty:
        stat_title_col, stat_save_col = st.columns([0.8, 0.2])
        with stat_title_col: st.subheader(stats_title)
        with stat_save_col:
            st.write("")
            if st.button("💾 Save", key=f"save_live_comp_confirm_{stats_title}", help="Save current comparison results", use_container_width=True):
                oun_url = st.session_state.get('processed_ounass_url', st.session_state.get('ounass_url_input',''))
                ls_url = st.session_state.get('levelshoes_url_input', ''); df_save = st.session_state.df_comparison_sorted
                if save_comparison(oun_url, ls_url, df_save):
                    st.success(f"Comparison saved! ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
                    load_saved_comparisons_meta.clear() # Clear cache after saving
                    st.session_state.confirm_delete_id = None; st.rerun()
    else: st.subheader(stats_title)
    df_o_safe = df_ounass if df_ounass is not None and not df_ounass.empty else pd.DataFrame()
    df_l_safe = df_levelshoes if df_levelshoes is not None and not df_levelshoes.empty else pd.DataFrame()
    df_c_safe = df_comparison_sorted if df_comparison_sorted is not None and not df_comparison_sorted.empty else pd.DataFrame()
    total_ounass_brands = 0; total_levelshoes_brands = 0; total_ounass_products = 0; total_levelshoes_products = 0; common_brands_count = 0; ounass_only_count = 0; levelshoes_only_count = 0
    if not df_o_safe.empty: total_ounass_brands = len(df_o_safe)
    if 'Count' in df_o_safe.columns: total_ounass_products = int(df_o_safe['Count'].sum())
    if not df_l_safe.empty: total_levelshoes_brands = len(df_l_safe)
    if 'Count' in df_l_safe.columns: total_levelshoes_products = int(df_l_safe['Count'].sum())
    if not df_c_safe.empty and 'Ounass_Count' in df_c_safe.columns and 'LevelShoes_Count' in df_c_safe.columns:
        if total_ounass_products == 0: total_ounass_products = int(df_c_safe['Ounass_Count'].sum())
        if total_levelshoes_products == 0: total_levelshoes_products = int(df_c_safe['LevelShoes_Count'].sum())
        if total_ounass_brands == 0: total_ounass_brands = len(df_c_safe[df_c_safe['Ounass_Count'] > 0])
        if total_levelshoes_brands == 0: total_levelshoes_brands = len(df_c_safe[df_c_safe['LevelShoes_Count'] > 0])
        common_brands_count = len(df_c_safe[(df_c_safe['Ounass_Count'] > 0) & (df_c_safe['LevelShoes_Count'] > 0)])
        ounass_only_count = len(df_c_safe[(df_c_safe['Ounass_Count'] > 0) & (df_c_safe['LevelShoes_Count'] == 0)])
        levelshoes_only_count = len(df_c_safe[(df_c_safe['Ounass_Count'] == 0) & (df_c_safe['LevelShoes_Count'] > 0)])
    stat_col1, stat_col2, stat_col3 = st.columns(3)
    with stat_col1: st.metric("Ounass Brands", f"{total_ounass_brands:,}"); st.metric("Ounass Products", f"{total_ounass_products:,}")
    with stat_col2: st.metric("Level Shoes Brands", f"{total_levelshoes_brands:,}"); st.metric("Level Shoes Products", f"{total_levelshoes_products:,}")
    with stat_col3:
        if not df_c_safe.empty and 'Ounass_Count' in df_c_safe.columns and 'LevelShoes_Count' in df_c_safe.columns: st.metric("Common Brands", f"{common_brands_count:,}"); st.metric("Ounass Only", f"{ounass_only_count:,}"); st.metric("Level Shoes Only", f"{levelshoes_only_count:,}")
        else: st.metric("Common Brands", "N/A"); st.metric("Ounass Only", "N/A"); st.metric("Level Shoes Only", "N/A")
        if not is_saved_view and (st.session_state.get('ounass_url_input') or st.session_state.get('levelshoes_url_input')): st.caption("Comparison requires data from both sites.")
    st.write(""); st.markdown("---")
    if not is_saved_view:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Ounass Results")
            if df_ounass is not None and not df_ounass.empty and 'Brand' in df_ounass.columns and 'Count' in df_ounass.columns: st.write(f"Brands Found: {len(df_ounass)}"); df_display = df_ounass.sort_values(by='Count', ascending=False).reset_index(drop=True); df_display.index += 1; st.dataframe(df_display[['Brand', 'Count']], height=400, use_container_width=True); csv_buffer = io.StringIO(); df_display[['Brand', 'Count']].to_csv(csv_buffer, index=False, encoding='utf-8'); csv_buffer.seek(0); st.download_button("Download Ounass List (CSV)", csv_buffer.getvalue(), 'ounass_brands.csv', 'text/csv', key='ounass_dl_disp')
            elif not st.session_state.get('df_ounass_processed', False): st.info("Enter Ounass URL and click 'Process URLs'.")
            elif process_button: st.warning("No data extracted from Ounass.")
        with col2:
            st.subheader("Level Shoes Results")
            if df_levelshoes is not None and not df_levelshoes.empty and 'Brand' in df_levelshoes.columns and 'Count' in df_levelshoes.columns: st.write(f"Brands Found: {len(df_levelshoes)}"); df_display = df_levelshoes.sort_values(by='Count', ascending=False).reset_index(drop=True); df_display.index += 1; st.dataframe(df_display[['Brand', 'Count']], height=400, use_container_width=True); csv_buffer = io.StringIO(); df_display[['Brand', 'Count']].to_csv(csv_buffer, index=False, encoding='utf-8'); csv_buffer.seek(0); st.download_button("Download Level Shoes List (CSV)", csv_buffer.getvalue(), 'levelshoes_brands.csv', 'text/csv', key='ls_dl_disp')
            elif not st.session_state.get('df_levelshoes_processed', False): st.info("Enter Level Shoes URL and click 'Process URLs'.")
            elif process_button: st.warning("No data extracted from Level Shoes.")

    if df_comparison_sorted is not None and not df_comparison_sorted.empty:
        if not is_saved_view: st.markdown("---")
        st.subheader("Ounass vs Level Shoes Brand Comparison")
        df_display = df_comparison_sorted.copy(); df_display.index += 1; display_cols = ['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference']; missing_cols = [col for col in display_cols if col not in df_display.columns]
        if missing_cols: st.warning(f"Comp table missing: {', '.join(missing_cols)}"); st.dataframe(df_display, height=500, use_container_width=True)
        else: st.dataframe(df_display[display_cols], height=500, use_container_width=True)
        st.markdown("---"); st.subheader("Visual Comparison"); viz_col1, viz_col2 = st.columns(2)
        with viz_col1:
            st.write("**Brand Overlap**"); pie_data = pd.DataFrame({'Category': ['Common Brands', 'Ounass Only', 'Level Shoes Only'],'Count': [common_brands_count, ounass_only_count, levelshoes_only_count]}); pie_data = pie_data[pie_data['Count'] > 0]
            if not pie_data.empty: fig_pie = px.pie(pie_data, names='Category', values='Count', title="Brand Presence", color_discrete_sequence=px.colors.qualitative.Pastel); fig_pie.update_traces(textposition='inside', textinfo='percent+label+value'); st.plotly_chart(fig_pie, use_container_width=True)
            else: st.info("No data for overlap chart.")
        with viz_col2:
            st.write("**Top 10 Largest Differences (Count)**")
            if 'Difference' in df_comparison_sorted.columns and 'Display_Brand' in df_comparison_sorted.columns:
                top_pos = df_comparison_sorted[df_comparison_sorted['Difference'] > 0].nlargest(5, 'Difference'); top_neg = df_comparison_sorted[df_comparison_sorted['Difference'] < 0].nsmallest(5, 'Difference'); top_diff = pd.concat([top_pos, top_neg]).sort_values('Difference', ascending=False)
                if not top_diff.empty: fig_diff = px.bar(top_diff, x='Display_Brand', y='Difference', title="Largest Differences (Ounass - LS)", labels={'Display_Brand': 'Brand', 'Difference': 'Product Count Diff'}, color='Difference', color_continuous_scale=px.colors.diverging.RdBu); fig_diff.update_layout(xaxis_title=None); st.plotly_chart(fig_diff, use_container_width=True)
                else: st.info("No significant differences for chart.")
            else: st.info("Difference data unavailable.")
        st.markdown("---"); st.subheader("Top 15 Brands Comparison (Total Products)")
        if not df_comparison_sorted.empty and all(c in df_comparison_sorted.columns for c in ['Display_Brand', 'Ounass_Count', 'LevelShoes_Count']):
            df_comp_copy = df_comparison_sorted.copy(); df_comp_copy['Total_Count'] = df_comp_copy['Ounass_Count'] + df_comp_copy['LevelShoes_Count']; top_n = 15; top_brands = df_comp_copy.nlargest(top_n, 'Total_Count')
            if not top_brands.empty:
                melted = top_brands.melt(id_vars='Display_Brand', value_vars=['Ounass_Count', 'LevelShoes_Count'], var_name='Website', value_name='Product Count'); melted['Website'] = melted['Website'].str.replace('_Count', '').str.replace('LevelShoes','Level Shoes'); fig_top = px.bar(melted, x='Display_Brand', y='Product Count', color='Website', barmode='group', title=f"Top {top_n} Brands by Total Products", labels={'Display_Brand': 'Brand'}, category_orders={"Display_Brand": top_brands['Display_Brand'].tolist()}); fig_top.update_layout(xaxis_title=None); st.plotly_chart(fig_top, use_container_width=True)
            else: st.info(f"Not enough data for Top {top_n} chart.")
        else: st.info(f"Comparison data unavailable for Top {top_n} chart.")
        st.markdown("---"); col_comp1, col_comp2 = st.columns(2); req_cols_exist = all(c in df_comparison_sorted.columns for c in ['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference'])
        with col_comp1:
            st.subheader("Brands in Ounass Only");
            if req_cols_exist: df_f = df_comparison_sorted[(df_comparison_sorted['LevelShoes_Count'] == 0) & (df_comparison_sorted['Ounass_Count'] > 0)]
            if req_cols_exist and not df_f.empty: df_d = df_f[['Display_Brand', 'Ounass_Count']].sort_values('Ounass_Count', ascending=False).reset_index(drop=True); df_d.index += 1; st.dataframe(df_d, height=400, use_container_width=True)
            elif req_cols_exist: st.info("No unique Ounass brands found.")
            else: st.info("Data unavailable.")
        with col_comp2:
            st.subheader("Brands in Level Shoes Only");
            if req_cols_exist: df_f = df_comparison_sorted[(df_comparison_sorted['Ounass_Count'] == 0) & (df_comparison_sorted['LevelShoes_Count'] > 0)]
            if req_cols_exist and not df_f.empty: df_d = df_f[['Display_Brand', 'LevelShoes_Count']].sort_values('LevelShoes_Count', ascending=False).reset_index(drop=True); df_d.index += 1; st.dataframe(df_d, height=400, use_container_width=True)
            elif req_cols_exist: st.info("No unique Level Shoes brands found.")
            else: st.info("Data unavailable.")
        st.markdown("---"); col_comp3, col_comp4 = st.columns(2)
        with col_comp3:
            st.subheader("Common Brands: Ounass > Level Shoes");
            if req_cols_exist: df_f = df_comparison_sorted[(df_comparison_sorted['Ounass_Count'] > 0) & (df_comparison_sorted['LevelShoes_Count'] > 0) & (df_comparison_sorted['Difference'] > 0)].sort_values('Difference', ascending=False)
            if req_cols_exist and not df_f.empty: df_d = df_f[['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference']].reset_index(drop=True); df_d.index += 1; st.dataframe(df_d, height=400, use_container_width=True)
            elif req_cols_exist: st.info("No common brands where Ounass > LS.")
            else: st.info("Data unavailable.")
        with col_comp4:
            st.subheader("Common Brands: Level Shoes > Ounass");
            if req_cols_exist: df_f = df_comparison_sorted[(df_comparison_sorted['Ounass_Count'] > 0) & (df_comparison_sorted['LevelShoes_Count'] > 0) & (df_comparison_sorted['Difference'] < 0)].sort_values('Difference', ascending=True)
            if req_cols_exist and not df_f.empty: df_d = df_f[['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference']].reset_index(drop=True); df_d.index += 1; st.dataframe(df_d, height=400, use_container_width=True)
            elif req_cols_exist: st.info("No common brands where LS > Ounass.")
            else: st.info("Data unavailable.")
        st.markdown("---"); csv_buffer_comparison = io.StringIO(); dl_cols = ['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference']
        if req_cols_exist:
            df_comparison_sorted[dl_cols].to_csv(csv_buffer_comparison, index=False, encoding='utf-8'); csv_buffer_comparison.seek(0); download_label = f"Download {'Saved' if is_saved_view else 'Current'} Comparison (CSV)"; view_id_part = saved_meta['id'] if is_saved_view and saved_meta else 'live'; download_key = f"comp_dl_button_{'saved' if is_saved_view else 'live'}_{view_id_part}"; filename_desc = f"{detected_gender or 'All'}_{detected_category or 'All'}".replace(' > ','-').replace(' ','_').lower(); download_filename = f"brand_comparison_{filename_desc}_{view_id_part}.csv".replace('?_?', 'all_all'); st.download_button(download_label, csv_buffer_comparison.getvalue(), download_filename, 'text/csv', key=download_key)
        else: st.warning("Could not generate download: missing columns.")
    elif process_button and not is_saved_view:
        st.markdown("---")
        st.warning("Comparison could not be generated. Check individual results.")


# --- Time Comparison Display Function ---
def display_time_comparison_results(df_time_comp, meta1, meta2):
    st.markdown("---"); st.subheader("Snapshot Comparison Over Time")
    ts_format = '%Y-%m-%d %H:%M'; ts1_str, ts2_str = "N/A", "N/A"; id1, id2 = meta1.get('id','N/A'), meta2.get('id','N/A')
    comparison_markdown = f"Comparing Snapshot 1 (ID: {id1}) vs Snapshot 2 (ID: {id2})" # Default
    try:
        ts1 = meta1.get('timestamp'); ts2 = meta2.get('timestamp')
        if ts1:
            if isinstance(ts1, datetime): ts1_str = ts1.strftime(ts_format)
            elif isinstance(ts1, str): ts1_str = datetime.fromisoformat(ts1).strftime(ts_format)
        if ts2:
            if isinstance(ts2, datetime): ts2_str = ts2.strftime(ts_format)
            elif isinstance(ts2, str): ts2_str = datetime.fromisoformat(ts2).strftime(ts_format)
        comparison_markdown = f"Comparing **Snapshot 1** (`{ts1_str}`, ID: {id1}) **vs** **Snapshot 2** (`{ts2_str}`, ID: {id2})"
    except Exception as e:
        st.warning(f"Error formatting timestamps: {e}")
    st.markdown(comparison_markdown)

    with st.expander("Show URLs for Compared Snapshots"): st.caption(f"**Snap 1 ({ts1_str}):** O: `{meta1.get('ounass_url', 'N/A')}` | LS: `{meta1.get('levelshoes_url', 'N/A')}`"); st.caption(f"**Snap 2 ({ts2_str}):** O: `{meta2.get('ounass_url', 'N/A')}` | LS: `{meta2.get('levelshoes_url', 'N/A')}`")
    st.markdown("---"); req_time_cols = ['Display_Brand','Ounass_Count_T1','Ounass_Count_T2','Ounass_Change','LevelShoes_Count_T1','LevelShoes_Count_T2','LevelShoes_Change']
    if not all(col in df_time_comp.columns for col in req_time_cols): st.error("Time comparison data missing required columns."); return
    new_o=df_time_comp[(df_time_comp['Ounass_Count_T1']==0)&(df_time_comp['Ounass_Count_T2']>0)]; drop_o=df_time_comp[(df_time_comp['Ounass_Count_T1']>0)&(df_time_comp['Ounass_Count_T2']==0)]; inc_o=df_time_comp[(df_time_comp['Ounass_Change']>0) & (df_time_comp['Ounass_Count_T1'] > 0)]; dec_o=df_time_comp[(df_time_comp['Ounass_Change']<0)]
    new_l=df_time_comp[(df_time_comp['LevelShoes_Count_T1']==0)&(df_time_comp['LevelShoes_Count_T2']>0)]; drop_l=df_time_comp[(df_time_comp['LevelShoes_Count_T1']>0)&(df_time_comp['LevelShoes_Count_T2']==0)]; inc_l=df_time_comp[(df_time_comp['LevelShoes_Change']>0) & (df_time_comp['LevelShoes_Count_T1'] > 0)]; dec_l=df_time_comp[(df_time_comp['LevelShoes_Change']<0)]
    st.subheader("Summary of Changes"); t_stat_col1, t_stat_col2 = st.columns(2)
    with t_stat_col1: st.metric("New Brands (Ounass)", len(new_o)); st.metric("Dropped Brands (Ounass)", len(drop_o)); st.metric("Increased Brands (Ounass)", len(inc_o)); st.metric("Decreased Brands (Ounass)", len(dec_o[dec_o['Ounass_Count_T2'] > 0])); st.metric("Net Product Change (Ounass)", f"{df_time_comp['Ounass_Change'].sum():+,}")
    with t_stat_col2: st.metric("New Brands (Level Shoes)", len(new_l)); st.metric("Dropped Brands (Level Shoes)", len(drop_l)); st.metric("Increased Brands (Level Shoes)", len(inc_l)); st.metric("Decreased Brands (Level Shoes)", len(dec_l[dec_l['LevelShoes_Count_T2'] > 0])); st.metric("Net Product Change (Level Shoes)", f"{df_time_comp['LevelShoes_Change'].sum():+,}")
    st.markdown("---"); st.subheader("Detailed Brand Changes"); tc_col1, tc_col2 = st.columns(2); height=250

    def display_change_df(df_change, category_name, cols_to_select, rename_map, sort_col, sort_ascending):
        if not df_change.empty:
            valid_cols_to_select = [col for col in cols_to_select if col in df_change.columns]
            if not valid_cols_to_select: st.error(f"Internal Error: No valid columns for '{category_name}'."); return False
            st.write(f"{category_name} ({len(df_change)}):")
            try:
                df_display = df_change[valid_cols_to_select].rename(columns=rename_map)
                final_sort_col = rename_map.get(sort_col, sort_col)
                if final_sort_col in df_display.columns:
                    df_display = df_display.sort_values(final_sort_col, ascending=sort_ascending).reset_index(drop=True); df_display.index += 1
                else: st.warning(f"Could not sort '{category_name}' by '{final_sort_col}'."); df_display = df_display.reset_index(drop=True); df_display.index += 1
                st.dataframe(df_display, height=height, use_container_width=True)
                return True
            except KeyError as e: st.error(f"Internal Error processing '{category_name}': Column {e} not found."); return False
            except Exception as e: st.error(f"Unexpected error displaying '{category_name}' details: {e}"); return False
        return False

    with tc_col1:
        st.write("**Ounass Changes**"); displayed_any_o = False
        cols_new_o = ['Display_Brand', 'Ounass_Count_T2']; map_new_o = {'Ounass_Count_T2': 'Now'}
        cols_drop_o = ['Display_Brand', 'Ounass_Count_T1']; map_drop_o = {'Ounass_Count_T1': 'Was'}
        cols_inc_dec_o = ['Display_Brand', 'Ounass_Count_T1', 'Ounass_Count_T2', 'Ounass_Change']; map_inc_dec_o = {'Ounass_Count_T1':'Was', 'Ounass_Count_T2':'Now', 'Ounass_Change':'Change'}
        if display_change_df(new_o, "New", cols_new_o, map_new_o, 'Ounass_Count_T2', False): displayed_any_o = True
        if display_change_df(drop_o, "Dropped", cols_drop_o, map_drop_o, 'Ounass_Count_T1', False): displayed_any_o = True
        if display_change_df(inc_o, "Increased", cols_inc_dec_o, map_inc_dec_o, 'Ounass_Change', False): displayed_any_o = True
        dec_o_display = dec_o[dec_o['Ounass_Count_T2'] > 0]
        if display_change_df(dec_o_display, "Decreased", cols_inc_dec_o, map_inc_dec_o, 'Ounass_Change', True): displayed_any_o = True
        if not displayed_any_o: st.info("No significant changes for Ounass.")
    with tc_col2:
        st.write("**Level Shoes Changes**"); displayed_any_l = False
        cols_new_l = ['Display_Brand', 'LevelShoes_Count_T2']; map_new_l = {'LevelShoes_Count_T2': 'Now'}
        cols_drop_l = ['Display_Brand', 'LevelShoes_Count_T1']; map_drop_l = {'LevelShoes_Count_T1': 'Was'}
        cols_inc_dec_l = ['Display_Brand', 'LevelShoes_Count_T1', 'LevelShoes_Count_T2', 'LevelShoes_Change']; map_inc_dec_l = {'LevelShoes_Count_T1':'Was', 'LevelShoes_Count_T2':'Now', 'LevelShoes_Change':'Change'}
        if display_change_df(new_l, "New", cols_new_l, map_new_l, 'LevelShoes_Count_T2', False): displayed_any_l = True
        if display_change_df(drop_l, "Dropped", cols_drop_l, map_drop_l, 'LevelShoes_Count_T1', False): displayed_any_l = True
        if display_change_df(inc_l, "Increased", cols_inc_dec_l, map_inc_dec_l, 'LevelShoes_Change', False): displayed_any_l = True
        dec_l_display = dec_l[dec_l['LevelShoes_Count_T2'] > 0]
        if display_change_df(dec_l_display, "Decreased", cols_inc_dec_l, map_inc_dec_l, 'LevelShoes_Change', True): displayed_any_l = True
        if not displayed_any_l: st.info("No significant changes for Level Shoes.")

    st.markdown("---"); csv_buffer = io.StringIO()
    if all(col in df_time_comp.columns for col in req_time_cols):
        df_time_comp[req_time_cols].to_csv(csv_buffer, index=False, encoding='utf-8'); csv_buffer.seek(0); st.download_button(label=f"Download Time Comparison ({ts1_str} vs {ts2_str})", data=csv_buffer.getvalue(), file_name=f"time_comparison_{id1}_vs_{id2}.csv", mime='text/csv', key='time_comp_dl_button')
    else: st.warning("Could not generate download: missing data.")


# --- Main Application Flow ---
init_db() # Ensure table exists on startup
confirm_id = st.session_state.get('confirm_delete_id')
viewing_saved_id = st.query_params.get("view_id", [None])[0] # Use the actual value for main logic

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
    # Display time comparison results FIRST if they exist in session state
    display_time_comparison_results(st.session_state.df_time_comparison, st.session_state.get('time_comp_meta1',{}), st.session_state.get('time_comp_meta2',{}))

elif viewing_saved_id:
    # If not showing time comparison, check if viewing a saved ID
    saved_meta, saved_df = load_specific_comparison(viewing_saved_id) # Cached function
    if saved_meta and saved_df is not None:
        display_all_results(None, None, saved_df, stats_title_prefix="Saved Comparison Details", is_saved_view=True, saved_meta=saved_meta)
    else:
        st.error(f"Could not load comparison ID: {viewing_saved_id}.");
        if st.button("Clear Invalid Saved View URL"): st.query_params.clear(); st.rerun()

else: # Live processing mode
    # process_button is defined conditionally earlier based on viewing_saved_id_check
    if process_button: # Check if button was clicked (only possible if not viewing saved)
        st.session_state.df_ounass = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned']); st.session_state.df_levelshoes = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned'])
        st.session_state.ounass_data = []; st.session_state.levelshoes_data = []; st.session_state.df_comparison_sorted = pd.DataFrame(); st.session_state.processed_ounass_url = ''
        st.session_state.df_time_comparison = pd.DataFrame(); st.session_state.time_comp_meta1 = {}; st.session_state.time_comp_meta2 = {}
        st.session_state.df_ounass_processed = False; st.session_state.df_levelshoes_processed = False

        if st.session_state.ounass_url_input:
            with st.spinner("Processing Ounass URL..."):
                st.session_state.processed_ounass_url = ensure_ounass_full_list_parameter(st.session_state.ounass_url_input)
                ounass_html_content = fetch_html_content(st.session_state.processed_ounass_url) # Cached fetch
                if ounass_html_content:
                    st.session_state.ounass_data = ounass_extractor.get_processed_ounass_data(ounass_html_content) # Use imported function
                if st.session_state.ounass_data:
                    try:
                        st.session_state.df_ounass = pd.DataFrame(st.session_state.ounass_data)
                        if not st.session_state.df_ounass.empty:
                            st.session_state.df_ounass['Brand_Cleaned'] = st.session_state.df_ounass['Brand'].apply(clean_brand_name)
                            st.session_state.df_ounass_processed = True
                        # else: Warning shown below if needed
                    except Exception as e: st.error(f"Error creating Ounass DF: {e}")

        if st.session_state.levelshoes_url_input:
             with st.spinner("Processing Level Shoes URL..."):
                levelshoes_html_content = fetch_html_content(st.session_state.levelshoes_url_input) # Cached fetch
                if levelshoes_html_content:
                    st.session_state.levelshoes_data = levelshoes_extractor.get_processed_levelshoes_data(levelshoes_html_content) # Use imported function
                if st.session_state.levelshoes_data:
                    try:
                        st.session_state.df_levelshoes = pd.DataFrame(st.session_state.levelshoes_data)
                        if not st.session_state.df_levelshoes.empty:
                            st.session_state.df_levelshoes['Brand_Cleaned'] = st.session_state.df_levelshoes['Brand'].apply(clean_brand_name)
                            st.session_state.df_levelshoes_processed = True
                        # else: Warning shown below if needed
                    except Exception as e: st.error(f"Error creating Level Shoes DF: {e}")

        # Show warnings based on extraction results after trying both
        if st.session_state.ounass_url_input and not st.session_state.ounass_data:
             st.warning("Could not extract any brand data from the Ounass URL.")
             st.session_state.df_ounass_processed = False
        if st.session_state.levelshoes_url_input and not st.session_state.levelshoes_data:
             st.warning("Could not extract any brand data from the Level Shoes URL.")
             st.session_state.df_levelshoes_processed = False

        # Create Comparison only if both were successfully processed
        if st.session_state.df_ounass_processed and st.session_state.df_levelshoes_processed:
            with st.spinner("Generating comparison..."):
                try:
                    df_o = st.session_state.df_ounass[['Brand','Count','Brand_Cleaned']].copy(); df_l = st.session_state.df_levelshoes[['Brand','Count','Brand_Cleaned']].copy(); df_comp = pd.merge(df_o, df_l, on='Brand_Cleaned', how='outer', suffixes=('_Ounass', '_LevelShoes'))
                    df_comp['Ounass_Count'] = df_comp['Count_Ounass'].fillna(0).astype(int); df_comp['LevelShoes_Count'] = df_comp['Count_LevelShoes'].fillna(0).astype(int); df_comp['Difference'] = df_comp['Ounass_Count'] - df_comp['LevelShoes_Count']
                    df_comp['Display_Brand'] = np.where(df_comp['Ounass_Count'] > 0, df_comp['Brand_Ounass'], df_comp['Brand_LevelShoes']); df_comp['Display_Brand'].fillna(df_comp['Brand_Cleaned'], inplace=True); df_comp['Display_Brand'].fillna("Unknown", inplace=True)
                    final_cols = ['Display_Brand','Brand_Cleaned','Ounass_Count','LevelShoes_Count','Difference','Brand_Ounass','Brand_LevelShoes'];
                    for col in final_cols:
                        if col not in df_comp.columns: df_comp[col] = np.nan
                    df_comp['Total_Count'] = df_comp['Ounass_Count'] + df_comp['LevelShoes_Count']; st.session_state.df_comparison_sorted = df_comp.sort_values(by=['Total_Count', 'Ounass_Count', 'Display_Brand'], ascending=[False, False, True]).reset_index(drop=True)[final_cols + ['Total_Count']]
                except Exception as merge_e: st.error(f"Error during comparison merge: {merge_e}"); st.session_state.df_comparison_sorted = pd.DataFrame()
        else:
             st.session_state.df_comparison_sorted = pd.DataFrame()
             # Removed redundant warnings here

        st.rerun() # Rerun after processing to display results

    # Display current live data (or initial empty state)
    df_ounass_live = st.session_state.get('df_ounass', pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned']))
    df_levelshoes_live = st.session_state.get('df_levelshoes', pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned']))
    df_comparison_sorted_live = st.session_state.get('df_comparison_sorted', pd.DataFrame())
    display_all_results(df_ounass_live, df_levelshoes_live, df_comparison_sorted_live, stats_title_prefix="Current Comparison")


# --- END OF CORRECTED FILE ---
