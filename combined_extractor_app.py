# --- START OF REFACTORED combined_extractor_app.py ---

import streamlit as st
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
import re # Needed for enhanced clean_brand_name

# --- NEW IMPORTS ---
import ounass_extractor
import levelshoes_extractor
import sephora_extractor # <-- Added Sephora extractor

# Try importing pytz for timezone handling, but don't fail if it's not installed
try:
    import pytz
except ImportError:
    pytz = None
    print("Warning: pytz library not found. Timestamps will be displayed in UTC or naive format.")

# --- END NEW IMPORTS ---

# --- App Configuration ---
APP_VERSION = "3.1.0" # Updated version: Improved cleaning/matching
st.set_page_config(layout="wide", page_title="Ounass vs Competitor PLP Comparison")

# --- App Title and Info ---
st.title(f"Ounass vs Competitor PLP Designer Comparison (v{APP_VERSION})")
st.write("Enter a Product Listing Page (PLP) URL from Ounass. Then, select a competitor (Level Shoes or Sephora). For Level Shoes, provide the PLP URL. For Sephora, upload the saved HTML file of the PLP. The tool compares designer/brand counts.")
st.info("Ensure URLs point to relevant listing pages. Ounass URL loads all designers. Level Shoes uses __NEXT_DATA__. Sephora uses uploaded HTML. Comparison history is stored.")

# --- Session State Initialization ---
# Site specific data holders (before processing)
if 'ounass_data' not in st.session_state: st.session_state.ounass_data = []
if 'competitor_data' not in st.session_state: st.session_state.competitor_data = []
if 'uploaded_sephora_html' not in st.session_state: st.session_state.uploaded_sephora_html = None

# Processed DataFrames
if 'df_ounass' not in st.session_state: st.session_state.df_ounass = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned'])
if 'df_competitor' not in st.session_state: st.session_state.df_competitor = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned'])

# Comparison results
if 'df_comparison_sorted' not in st.session_state: st.session_state.df_comparison_sorted = pd.DataFrame()
if 'df_time_comparison' not in st.session_state: st.session_state.df_time_comparison = pd.DataFrame()

# Input fields and selections
if 'ounass_url_input' not in st.session_state: st.session_state.ounass_url_input = ''
if 'levelshoes_url_input' not in st.session_state: st.session_state.levelshoes_url_input = '' # Keep for LS convenience
if 'competitor_selection' not in st.session_state: st.session_state.competitor_selection = "Level Shoes" # Default competitor

# State tracking
if 'processed_ounass_url' not in st.session_state: st.session_state.processed_ounass_url = ''
if 'confirm_delete_id' not in st.session_state: st.session_state.confirm_delete_id = None
if 'time_comp_meta1' not in st.session_state: st.session_state.time_comp_meta1 = {}
if 'time_comp_meta2' not in st.session_state: st.session_state.time_comp_meta2 = {}
if 'df_ounass_processed' not in st.session_state: st.session_state.df_ounass_processed = False
if 'df_competitor_processed' not in st.session_state: st.session_state.df_competitor_processed = False
if 'selections_by_group' not in st.session_state: st.session_state.selections_by_group = {}
if 'show_saved_comparisons' not in st.session_state: st.session_state.show_saved_comparisons = False
if 'competitor_input_identifier' not in st.session_state: st.session_state.competitor_input_identifier = '' # Stores URL or filename

# --- Competitor Selection ---
competitor_options = ["Level Shoes", "Sephora"]
st.session_state.competitor_selection = st.radio(
    "Select Competitor to Compare Against Ounass:",
    options=competitor_options,
    key="competitor_radio",
    horizontal=True,
    index=competitor_options.index(st.session_state.get('competitor_selection', "Level Shoes")) # Persist selection
)
competitor_name = st.session_state.competitor_selection # Use this variable throughout

# --- URL / File Input Section (Conditional) ---
viewing_saved_id_check = st.query_params.get("view_id", [None])[0]
process_button = False # Default value
uploaded_file = None # Initialize

if not viewing_saved_id_check and st.session_state.get('df_time_comparison', pd.DataFrame()).empty:
    st.markdown("---") # Separator
    st.subheader("Provide Inputs for Comparison")
    col1, col2 = st.columns(2)
    with col1:
        st.session_state.ounass_url_input = st.text_input(
            "Ounass URL",
            key="ounass_url_widget_main",
            value=st.session_state.ounass_url_input,
            placeholder="https://www.ounass.ae/..."
        )
    with col2:
        if competitor_name == "Level Shoes":
            st.session_state.levelshoes_url_input = st.text_input(
                "Level Shoes URL",
                key="levelshoes_url_widget_main",
                value=st.session_state.levelshoes_url_input,
                placeholder="https://www.levelshoes.com/..."
            )
            st.session_state.uploaded_sephora_html = None # Clear file if switching back
        elif competitor_name == "Sephora":
            uploaded_file = st.file_uploader(
                "Upload Sephora HTML File",
                type=["html", "htm"],
                key="sephora_file_uploader_main",
                help="Save the Sephora PLP page (Ctrl+S or Cmd+S -> 'Webpage, HTML Only') and upload it here."
            )
            if uploaded_file is not None:
                # Store content immediately if a new file is uploaded
                try:
                    st.session_state.uploaded_sephora_html = uploaded_file.read().decode("utf-8", errors="ignore")
                    st.session_state.competitor_input_identifier = uploaded_file.name # Store filename
                    st.success(f"File '{uploaded_file.name}' uploaded successfully.")
                except Exception as e:
                    st.error(f"Error reading uploaded file: {e}")
                    st.session_state.uploaded_sephora_html = None
                    st.session_state.competitor_input_identifier = ''

            elif st.session_state.uploaded_sephora_html and st.session_state.competitor_input_identifier:
                 # If no new file is uploaded, but we have one in state, keep it.
                 st.info(f"Using previously uploaded file: {st.session_state.competitor_input_identifier}")
            # else: No file uploaded and none in state

            st.session_state.levelshoes_url_input = '' # Clear URL if switching

    process_button_label = f"Process Ounass vs {competitor_name}"
    process_button = st.button(process_button_label, key="process_button_main")
    st.markdown("---") # Separator before results
# --- End Input Section ---


# --- Database Setup & Functions (PostgreSQL Version - Updated for Competitor) ---
@st.cache_resource
def get_connection_details():
    try:
        if hasattr(st, 'secrets') and "connections" in st.secrets and "postgres" in st.secrets["connections"]:
            return st.secrets["connections"]["postgres"]["url"]
        elif "DATABASE_URL" in os.environ:
            return os.environ["DATABASE_URL"]
        else:
            st.error("Database connection details not found in Streamlit Secrets or DATABASE_URL env var.")
            return None
    except Exception as e:
        st.error(f"Error accessing connection details: {e}")
        return None

def get_db_connection():
    db_url = get_connection_details()
    if not db_url: return None
    try:
        conn = psycopg2.connect(db_url, sslmode='require')
        return conn
    except psycopg2.OperationalError as e:
        if "authentication failed" in str(e): st.error("Database Connection Error: Authentication failed. Check credentials.")
        elif "does not exist" in str(e): st.error("Database Connection Error: Database not found. Check connection string.")
        else: st.error(f"Database Connection Error: Could not connect. Details: {e}")
        return None
    except Exception as e:
        st.error(f"Unexpected Database Connection Error: {e}")
        return None

def init_db():
    conn = get_db_connection()
    if conn is None: return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DO $$
                BEGIN
                    CREATE TABLE IF NOT EXISTS comparisons (
                        id SERIAL PRIMARY KEY, timestamp TIMESTAMPTZ NOT NULL, ounass_url TEXT NOT NULL,
                        levelshoes_url TEXT, comparison_data JSONB NOT NULL, comparison_name TEXT,
                        competitor_name TEXT, competitor_input TEXT
                    );
                    ALTER TABLE comparisons ADD COLUMN IF NOT EXISTS competitor_name TEXT;
                    ALTER TABLE comparisons ADD COLUMN IF NOT EXISTS competitor_input TEXT;
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name='comparisons' AND column_name='levelshoes_url' AND is_nullable='NO'
                    ) THEN
                       ALTER TABLE comparisons ALTER COLUMN levelshoes_url DROP NOT NULL;
                    END IF;
                 EXCEPTION
                    WHEN duplicate_object THEN RAISE NOTICE 'Table comparisons already exists.';
                    WHEN others THEN RAISE WARNING 'Error during DB init: %', SQLERRM;
                END $$;
            """)
        conn.commit()
        print("Database initialized/checked successfully.")
    except Exception as e:
        st.error(f"Fatal DB Init Error: {e}")
        try: conn.rollback()
        except Exception as rb_e: st.error(f"Rollback failed after init error: {rb_e}")
    finally:
        if conn: conn.close()

# Updated save_comparison
def save_comparison(ounass_url, competitor_name_arg, competitor_input_arg, df_comparison):
    if df_comparison is None or df_comparison.empty:
        st.error("Cannot save empty comparison data.")
        return False
    conn = get_db_connection()
    if conn is None: return False
    try:
      # ------------------------------------------------------------------
# Choose a clock that always ticks in Dubai 🇦🇪  (UTC+4, no DST)
# ------------------------------------------------------------------
        if pytz is not None:                             # <- 4 spaces
            timestamp = datetime.now(                    # <- 8 spaces
            pytz.timezone("Asia/Dubai")
            )
        else:                                            # <- 4 spaces
            timestamp = datetime.now()                   # <- 8 spaces
    
        df_to_save = df_comparison.copy()
        generic_cols = ['Display_Brand', 'Ounass_Count', 'Competitor_Count', 'Difference', 'Brand_Cleaned', 'Brand_Ounass', 'Brand_Competitor']
        rename_map = {}
        actual_competitor_count_col = f"{competitor_name_arg.replace(' ', '')}_Count"
        actual_competitor_brand_col = f"Brand_{competitor_name_arg.replace(' ', '')}"

        if actual_competitor_count_col in df_to_save.columns: rename_map[actual_competitor_count_col] = 'Competitor_Count'
        elif 'Competitor_Count' not in df_to_save.columns:
             found_comp_col = next((col for col in df_to_save.columns if col.endswith('_Count') and col not in ['Ounass_Count', 'Total_Count']), None)
             if found_comp_col: rename_map[found_comp_col] = 'Competitor_Count'
             else: st.error(f"Save Error: Cannot find competitor count column."); return False

        if actual_competitor_brand_col in df_to_save.columns: rename_map[actual_competitor_brand_col] = 'Brand_Competitor'
        elif 'Brand_Competitor' not in df_to_save.columns:
             found_brand_comp_col = next((col for col in df_to_save.columns if col.startswith('Brand_') and col not in ['Brand_Ounass', 'Brand_Cleaned']), None)
             if found_brand_comp_col: rename_map[found_brand_comp_col] = 'Brand_Competitor'
             else: df_to_save['Brand_Competitor'] = np.nan

        df_to_save.rename(columns=rename_map, inplace=True)
        for col in generic_cols:
            if col not in df_to_save.columns: df_to_save[col] = np.nan

        data_json = df_to_save[generic_cols].to_json(orient="records", date_format="iso", default_handler=str)
        ls_url_to_save = competitor_input_arg if competitor_name_arg == "Level Shoes" else None
        with conn.cursor() as cur:
            sql = """INSERT INTO comparisons (timestamp, ounass_url, levelshoes_url, comparison_data, comparison_name, competitor_name, competitor_input) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            cur.execute(sql, (timestamp, ounass_url, ls_url_to_save, data_json, None, competitor_name_arg, competitor_input_arg))
        conn.commit()
        load_saved_comparisons_meta.clear()
        return True
    except Exception as e:
        st.error(f"Database Error: Could not save comparison - {e}")
        try: conn.rollback()
        except Exception as rb_e: st.error(f"Rollback failed after save error: {rb_e}")
        return False
    finally:
        if conn: conn.close()

# Updated load_saved_comparisons_meta
@st.cache_data(ttl=300)
def load_saved_comparisons_meta():
    conn = get_db_connection()
    if conn is None: return []
    comparisons_list = []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""SELECT id, timestamp, ounass_url, levelshoes_url, comparison_name, competitor_name, competitor_input FROM comparisons ORDER BY timestamp DESC""")
            comparisons = cur.fetchall()
            comparisons_list = [dict(row) for row in comparisons] if comparisons else []
    except psycopg2.Error as e:
        st.error(f"Database Error loading comparisons list: {e}")
        if "relation" in str(e) and "does not exist" in str(e): init_db()
        elif "column" in str(e) and "does not exist" in str(e): init_db()
        comparisons_list = []
    except Exception as e:
        st.error(f"Unexpected Error loading comparisons list: {e}")
        comparisons_list = []
    finally:
        if conn: conn.close()
    return comparisons_list

# Updated load_specific_comparison
@st.cache_data(ttl=600)
def load_specific_comparison(comp_id):
    st.info(f"Loading details for saved comparison ID: {comp_id}")
    conn = get_db_connection()
    if conn is None: return None, None
    meta, df = None, None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            sql = """SELECT id, timestamp, ounass_url, levelshoes_url, comparison_data, comparison_name, competitor_name, competitor_input FROM comparisons WHERE id = %s"""
            cur.execute(sql, (comp_id,))
            comp = cur.fetchone()
            if comp:
                comp_dict = dict(comp)
                saved_competitor_name = comp_dict.get('competitor_name')
                if not saved_competitor_name and comp_dict.get('levelshoes_url'): saved_competitor_name = 'Level Shoes'
                elif not saved_competitor_name: saved_competitor_name = 'Unknown Competitor'
                saved_competitor_input = comp_dict.get('competitor_input')
                if not saved_competitor_input and saved_competitor_name == 'Level Shoes': saved_competitor_input = comp_dict.get('levelshoes_url')
                elif not saved_competitor_input: saved_competitor_input = 'N/A'
                fallback_name = f"ID {comp_dict['id']} ({comp_dict['timestamp']})"
                meta = {"timestamp": comp_dict["timestamp"], "ounass_url": comp_dict["ounass_url"], "competitor_name": saved_competitor_name, "competitor_input": saved_competitor_input, "name": comp_dict["comparison_name"] or fallback_name, "id": comp_dict["id"], "levelshoes_url_raw": comp_dict.get("levelshoes_url")}
                json_data = comp_dict["comparison_data"]
                if isinstance(json_data, str): df = pd.read_json(io.StringIO(json_data), orient="records")
                elif isinstance(json_data, (list, dict)): df = pd.DataFrame(json_data)
                else: st.error(f"Unexpected data type for comparison_data: {type(json_data)}"); df = pd.DataFrame()

                if not df.empty:
                    competitor_col_generic = 'Competitor_Count'; competitor_col_specific = f"{saved_competitor_name.replace(' ', '')}_Count"
                    brand_col_generic = 'Brand_Competitor'; brand_col_specific = f"Brand_{saved_competitor_name.replace(' ', '')}"
                    rename_load_map = {}
                    if competitor_col_generic in df.columns: rename_load_map[competitor_col_generic] = competitor_col_specific
                    if brand_col_generic in df.columns: rename_load_map[brand_col_generic] = brand_col_specific
                    df.rename(columns=rename_load_map, inplace=True)
                    if 'Ounass_Count' not in df.columns: df['Ounass_Count'] = 0
                    if competitor_col_specific not in df.columns: df[competitor_col_specific] = 0
                    df['Ounass_Count'] = pd.to_numeric(df['Ounass_Count'], errors='coerce').fillna(0).astype(int)
                    df[competitor_col_specific] = pd.to_numeric(df[competitor_col_specific], errors='coerce').fillna(0).astype(int)
                    if 'Difference' not in df.columns or df['Difference'].isnull().all(): df['Difference'] = df['Ounass_Count'] - df[competitor_col_specific]
                    if 'Display_Brand' not in df.columns or df['Display_Brand'].isnull().all():
                        brand_ounass_col = 'Brand_Ounass' if 'Brand_Ounass' in df.columns else None
                        brand_comp_col = brand_col_specific if brand_col_specific in df.columns else None
                        brand_cleaned_col = 'Brand_Cleaned' if 'Brand_Cleaned' in df.columns else None
                        df['Display_Brand'] = df[brand_ounass_col] if brand_ounass_col else None
                        if brand_comp_col: df['Display_Brand'] = df['Display_Brand'].fillna(df[brand_comp_col])
                        if brand_cleaned_col: df['Display_Brand'] = df['Display_Brand'].fillna(df[brand_cleaned_col])
                        df['Display_Brand'].fillna("Unknown", inplace=True)
            else: st.warning(f"Saved comparison with ID {comp_id} not found."); meta, df = None, None
    except Exception as e: st.error(f"Database Error: Could not load comparison ID {comp_id} - {e}"); meta, df = None, None
    finally:
        if conn: conn.close()
    return meta, df

# Delete function remains the same structurally
def delete_comparison(comp_id):
    conn = get_db_connection()
    if conn is None: return False
    success = False
    try:
        with conn.cursor() as cur:
            sql = "DELETE FROM comparisons WHERE id = %s"
            cur.execute(sql, (comp_id,))
            success = cur.rowcount > 0
        conn.commit()
        if success: load_saved_comparisons_meta.clear(); load_specific_comparison.clear()
    except Exception as e:
        st.error(f"Database Error: Could not delete comparison ID {comp_id} - {e}")
        success = False
        try: conn.rollback()
        except Exception as rb_e: st.error(f"Rollback failed after delete error: {rb_e}")
    finally:
        if conn: conn.close()
    return success

# --- Helper Functions ---

# ----- Replace this function in combined_extractor_app.py -----
def clean_brand_name(brand_name):
    """Cleans brand names for better matching across sources."""
    if not isinstance(brand_name, str) or not brand_name:
        return "" # Return empty string for non-strings or empty input

    # 1. NFKC Normalization: Handles compatibility characters and improves standardization.
    try:
        normalized = unicodedata.normalize('NFKC', brand_name)
    except Exception as e:
        print(f"Warning: NFKC normalization failed for '{brand_name}': {e}")
        normalized = brand_name # Fallback to original on error

    # 2. Convert to Uppercase for case-insensitive operations
    cleaned_upper = normalized.upper()

    # 3. Define common suffixes/qualifiers to remove
    suffixes_to_remove = [
        "BEAUTY", "PERFUMES", "PARFUMS", "FRAGRANCES", "FRAGRANCE",
        "COSMETICS", "MAKEUP", "MAQUILLAGE", "SKINCARE", "HAIRCARE",
        "COLLECTION", "BEAUTE", "PROFESSIONAL", "PROFESSIONNEL",
        # Add more specific ones if needed, e.g., "COUTURE"
        "COUTURE"
    ]
    suffixes_to_remove = sorted(list(set(suffixes_to_remove)), key=len, reverse=True) # Ensure unique and sorted

    # 4. Remove suffixes if they appear as the last word(s)
    words = cleaned_upper.split()
    words_processed = []
    # Iterate backwards to potentially remove multiple suffixes
    temp_words = list(words) # Work on a copy
    removed_suffix = False
    while len(temp_words) > 1:
         last_word = temp_words[-1]
         is_suffix = False
         # Check if the last word IS one of the suffixes
         if last_word in suffixes_to_remove:
             is_suffix = True
         # Check if last word ENDS WITH a suffix (e.g. "SKINCARE.") - less common but possible
         # else:
         #     for suffix in suffixes_to_remove:
         #         if last_word.endswith(suffix): # requires punctuation removal first maybe?
         #             is_suffix = True; break

         if is_suffix:
             temp_words.pop() # Remove it
             removed_suffix = True
         else:
             break # Stop if last word isn't a suffix

    # Use the potentially shortened word list
    cleaned_suffix_removed = " ".join(temp_words) if removed_suffix else " ".join(words)


    # 5. Basic punctuation and symbol removal (keep spaces for now)
    # Replace hyphen with space, remove common symbols
    cleaned_punct = cleaned_suffix_removed.replace('.', ' ').replace("'", '').replace('&', ' ')
    cleaned_punct = re.sub(r'[-]', ' ', cleaned_punct) # Hyphen to space
    cleaned_punct = re.sub(r'[®™©]', '', cleaned_punct) # Remove Trademarks/Copyright symbols
    cleaned_punct = re.sub(r'[()\[\]{}<>/"?]', ' ', cleaned_punct) # Remove brackets/slashes/quotes/qmarks
    cleaned_punct = re.sub(r'\s+', ' ', cleaned_punct).strip() # Consolidate multiple spaces

    # 6. Decompose accents and remove non-ASCII characters (NFKD method)
    try:
        # Normalize again after punctuation changes, then encode/decode
        cleaned_ascii = unicodedata.normalize('NFKD', cleaned_punct).encode('ascii', 'ignore').decode('utf-8')
    except Exception as e:
        print(f"Warning: ASCII conversion failed for '{cleaned_punct}': {e}")
        cleaned_ascii = cleaned_punct # Fallback

    # 7. Final step: Remove ALL remaining spaces and non-alphanumeric characters for the merge key
    final_key = ''.join(c for c in cleaned_ascii if c.isalnum())

    # Handle edge case: if result is empty string after all cleaning
    if not final_key:
        # Fallback: just alphanumeric from the original uppercase name
        return ''.join(c for c in brand_name.upper() if c.isalnum())

    return final_key
# ----- End of function replacement -----


# Keep custom_scorer as is
def custom_scorer(s1, s2):
    scores = [fuzz.ratio(s1, s2), fuzz.partial_ratio(s1, s2), fuzz.token_set_ratio(s1, s2), fuzz.token_sort_ratio(s1, s2)]
    return max(scores)

# Keep handle_checkbox_change as is
def handle_checkbox_change(group_key, comp_id):
    checkbox_state_key = f"cb_{comp_id}"
    current_state = st.session_state.get(checkbox_state_key, False)
    st.session_state.selections_by_group.setdefault(group_key, set())
    selections = st.session_state.selections_by_group[group_key]
    if current_state:
        if len(selections) >= 2 and comp_id not in selections:
            st.warning("You can only select two snapshots for comparison.")
            st.session_state[checkbox_state_key] = False
        else: selections.add(comp_id)
    else: selections.discard(comp_id)

# Fetch HTML function remains the same
@st.cache_data(ttl=600)
def fetch_html_content(url):
    if not url: print("Fetch error: URL cannot be empty."); return None
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9', 'Accept-Language': 'en-US,en;q=0.9', 'Connection': 'keep-alive', 'DNT': '1', 'Upgrade-Insecure-Requests': '1'}
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.exceptions.Timeout: st.error(f"Error: Timeout fetching {url}"); return None
    except requests.exceptions.HTTPError as http_err: st.error(f"HTTP error occurred fetching {url}: {http_err} (Status code: {http_err.response.status_code})"); return None
    except requests.exceptions.RequestException as e: st.error(f"Error fetching {url}: {e}"); return None
    except Exception as e: st.error(f"Unexpected error during fetch for {url}: {e}"); return None

# Ounass URL parameter function remains the same
def ensure_ounass_full_list_parameter(url):
    param_key = 'fh_maxdisplaynrvalues_designer'; param_value = '-1'
    try:
        if not url or 'ounass' not in urlparse(url).netloc.lower(): return url
    except Exception: st.warning(f"Could not parse Ounass URL: {url}"); return url
    try:
        parsed_url = urlparse(url); query_params = parse_qs(parsed_url.query, keep_blank_values=True)
        needs_update = (param_key not in query_params or not query_params[param_key] or query_params[param_key][0] != param_value)
        if needs_update:
            query_params[param_key] = [param_value]; new_query_string = urlencode(query_params, doseq=True)
            url_components = list(parsed_url); url_components[4] = new_query_string
            new_url = urlunparse(url_components); print(f"Updated Ounass URL with param: {new_url}"); return new_url
        else: return url
    except Exception as e: st.warning(f"Error processing Ounass URL parameters: {e}"); return url

# URL info extraction function remains the same
def extract_info_from_url(url):
    try:
        if not url or not isinstance(url, str) or not url.startswith('http'): return None, None
        parsed = urlparse(url)
        ignore_segments = {'ae', 'com', 'sa', 'kw', 'om', 'bh', 'qa', 'eg', 'en', 'ar', 'shop', 'category', 'all', 'view-all', 'plp', 'sale', 'new-arrivals', 'products', 'list', 'women', 'men', 'kids', 'unisex', 'home'}
        path_segments = [s for s in parsed.path.lower().split('/') if s and s not in ignore_segments and not s.isdigit()]
        gender = None; original_path_parts = [p for p in parsed.path.lower().split('/') if p]
        gender_keywords = {"women", "woman", "men", "man", "kids", "kid", "child", "children", "unisex", "home"}
        for part in original_path_parts:
             if part in gender_keywords:
                 if part in ["women", "woman"]: gender = "Women"
                 elif part in ["men", "man"]: gender = "Men"
                 elif part in ["kids", "kid", "child", "children"]: gender = "Kids"
                 elif part == "unisex": gender = "Unisex"
                 elif part == "home": gender = "Home"
                 else: gender = part.title()
                 break
        cleaned_category_parts = []
        for part in path_segments:
            if gender and part == gender.lower(): continue
            cleaned = part.replace('.html', '').replace('-', ' ').replace('_',' ').strip()
            capitalized_part = ' '.join(word.capitalize() for word in cleaned.split())
            if capitalized_part: cleaned_category_parts.append(capitalized_part)
        category = " > ".join(cleaned_category_parts) if cleaned_category_parts else None
        if category and category.lower() in ['all', 'view all', 'shop all']: category = None
        return gender, category
    except Exception as e: print(f"Warning: Error parsing URL {url}: {e}"); return None, None

# --- Sidebar ---
st.sidebar.title("Options & History")
st.sidebar.caption(f"App Version: {APP_VERSION}")
if viewing_saved_id_check or not st.session_state.get('df_time_comparison', pd.DataFrame()).empty:
    if st.sidebar.button("<< Back to Live Processing", key="back_live", use_container_width=True):
        st.query_params.clear(); st.session_state.confirm_delete_id = None
        st.session_state.df_time_comparison = pd.DataFrame(); st.session_state.time_comp_meta1 = {}; st.session_state.time_comp_meta2 = {}
        st.session_state.show_saved_comparisons = False; st.session_state.selections_by_group = {}
        st.session_state.levelshoes_url_input = ''; st.session_state.uploaded_sephora_html = None
        st.session_state.competitor_input_identifier = ''; st.session_state.df_competitor = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned'])
        st.session_state.df_competitor_processed = False; st.session_state.ounass_url_input = ''
        st.session_state.df_ounass = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned']); st.session_state.df_ounass_processed = False
        st.session_state.df_comparison_sorted = pd.DataFrame(); st.rerun()
st.sidebar.markdown("---")
st.sidebar.subheader("Saved Comparisons")
if not st.session_state.get('show_saved_comparisons', False):
    if st.sidebar.button("Load Saved Comparisons", key="load_saved_btn", use_container_width=True): st.session_state.show_saved_comparisons = True; st.rerun()
else:
    if st.sidebar.button("Hide Saved Comparisons", key="hide_saved_btn", use_container_width=True): st.session_state.show_saved_comparisons = False; st.session_state.selections_by_group = {}; st.rerun()
    saved_comps_meta = load_saved_comparisons_meta()
    if not saved_comps_meta: st.sidebar.caption("No comparisons found in the database.")
    else:
        grouped_comps = defaultdict(list)
        for comp_meta in saved_comps_meta:
            comp_name = comp_meta.get('competitor_name'); comp_input = comp_meta.get('competitor_input')
            if not comp_name and comp_meta.get('levelshoes_url'): comp_name = 'Level Shoes'
            if not comp_input and comp_name == 'Level Shoes': comp_input = comp_meta.get('levelshoes_url')
            if not comp_name: comp_name = "Unknown"; comp_input = "N/A"
            url_key = (comp_meta.get('ounass_url',''), comp_name, comp_input); grouped_comps[url_key].append(comp_meta)
        if 'selections_by_group' not in st.session_state: st.session_state.selections_by_group = {}
        st.sidebar.caption("Select two snapshots from the *same group* below to compare changes over time.")
        url_group_keys = sorted(list(grouped_comps.keys()), key=lambda x: (x[0] or '', x[1] or ''))
        for idx, url_key in enumerate(url_group_keys):
            comps_list = grouped_comps[url_key]; ounass_url_grp, comp_name_grp, comp_input_grp = url_key
            g, c = extract_info_from_url(ounass_url_grp); cat_info = f"{g or '?'} / {c or '?'}" if (g or c) else "Category N/A"
            input_display = '';
            if comp_name_grp == "Level Shoes": input_display = f": {urlparse(comp_input_grp or '').path}"
            elif comp_name_grp == "Sephora": input_display = f": {os.path.basename(comp_input_grp or '')}" if comp_input_grp else ""
            else: input_display = f": {comp_input_grp[:20]}..." if comp_input_grp and len(comp_input_grp)>20 else f": {comp_input_grp}"
            input_display = input_display[:30] + '...' if len(input_display) > 33 else input_display
            expander_label = f"Ounass vs {comp_name_grp} ({cat_info}) - {len(comps_list)} snapshots"
            if not (g or c): oun_path_part = urlparse(ounass_url_grp or '').path.split('/')[-1].replace('.html','') or "Ounass"; expander_label = f"{oun_path_part} vs {comp_name_grp}{input_display} ({len(comps_list)} snapshots)"
            with st.sidebar.expander(expander_label, expanded=True):
                st.session_state.selections_by_group.setdefault(url_key, set()); current_selections = st.session_state.selections_by_group[url_key]; st.write("Select two snapshots:")
                for comp_meta in sorted(comps_list, key=lambda x: x.get('timestamp', datetime.min)):
                     comp_id = comp_meta['id']; ts = comp_meta['timestamp']; display_ts_str="Invalid Date"
                     try:
                         if pytz:
                              dt = ts; tz_name = 'Asia/Dubai'
                              if isinstance(ts, str): dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                              if isinstance(dt, datetime):
                                   if dt.tzinfo is None: dt = pytz.utc.localize(dt)
                                   display_ts_str = dt.astimezone(pytz.timezone(tz_name)).strftime('%Y-%m-%d %H:%M')
                         else:
                              if isinstance(ts, datetime): display_ts_str = ts.strftime('%Y-%m-%d %H:%M')
                              elif isinstance(ts, str): display_ts_str = ts[:16].replace('T', ' ')
                     except Exception as ts_e: print(f"Timestamp formatting error for ID {comp_id}: {ts_e}"); display_ts_str = str(ts)[:16]
                     display_label = f"{display_ts_str} (ID: {comp_id})"; is_currently_selected_in_state = comp_id in current_selections
                     col_cb, col_view, col_del = st.columns([0.15, 0.7, 0.15])
                     with col_cb: st.checkbox(" ", key=f"cb_{comp_id}", value=is_currently_selected_in_state, on_change=handle_checkbox_change, args=(url_key, comp_id), label_visibility="collapsed")
                     with col_view:
                          is_being_viewed = str(comp_id) == viewing_saved_id_check; button_type = "primary" if is_being_viewed else "secondary"
                          if st.button(display_label, key=f"view_detail_{comp_id}", type=button_type, use_container_width=True): st.query_params["view_id"] = str(comp_id); st.session_state.confirm_delete_id = None; st.session_state.df_time_comparison = pd.DataFrame(); st.rerun()
                     with col_del:
                          if st.button("🗑️", key=f"del_detail_{comp_id}", help=f"Delete snapshot from {display_ts_str}", use_container_width=True): st.session_state.confirm_delete_id = comp_id; st.query_params.clear(); st.rerun()
                st.markdown("---")
                selected_ids_list = list(current_selections); compare_button_disabled = (len(selected_ids_list) != 2)
                if st.button("Compare Selected Snapshots", key=f"compare_chk_{idx}", disabled=compare_button_disabled, use_container_width=True):
                     if len(selected_ids_list) == 2:
                         id1, id2 = selected_ids_list[0], selected_ids_list[1]
                         meta1, df1 = load_specific_comparison(id1); meta2, df2 = load_specific_comparison(id2); valid_load = True
                         if not (meta1 and df1 is not None and not df1.empty): st.error(f"Failed to load valid data for snapshot ID: {id1}."); valid_load = False
                         if not (meta2 and df2 is not None and not df2.empty): st.error(f"Failed to load valid data for snapshot ID: {id2}."); valid_load = False
                         if valid_load and meta1.get('competitor_name') != meta2.get('competitor_name'): st.error(f"Cannot compare snapshots: Competitors mismatch."); valid_load = False
                         if valid_load:
                             ts1 = meta1['timestamp']; ts2 = meta2['timestamp']
                             try:
                                 if isinstance(ts1, str): ts1 = datetime.fromisoformat(ts1.replace('Z', '+00:00'))
                                 if isinstance(ts2, str): ts2 = datetime.fromisoformat(ts2.replace('Z', '+00:00'))
                                 if pytz:
                                     if ts1.tzinfo is None: ts1 = pytz.utc.localize(ts1)
                                     if ts2.tzinfo is None: ts2 = pytz.utc.localize(ts2)
                                 if ts1 > ts2: id1, id2, meta1, df1, meta2, df2 = id2, id1, meta2, df2, meta1, df1
                             except Exception as ts_parse_e: st.error(f"Error parsing timestamps for comparison: {ts_parse_e}"); valid_load = False
                             if valid_load:
                                 time_comp_competitor_name = meta1.get('competitor_name', 'Level Shoes'); time_comp_competitor_col = f"{time_comp_competitor_name.replace(' ', '')}_Count"
                                 required_cols = ['Display_Brand', 'Ounass_Count', time_comp_competitor_col]
                                 for i, df_check in enumerate([df1, df2]):
                                     df_id = id1 if i == 0 else id2
                                     for col in required_cols:
                                          if col not in df_check.columns: st.error(f"Snapshot ID {df_id} is missing required column: '{col}'. Cannot compare."); valid_load = False; break
                                     if not valid_load: break
                                 if valid_load:
                                     df_time = pd.merge(df1[required_cols], df2[required_cols], on='Display_Brand', how='outer', suffixes=('_T1', '_T2'))
                                     count_cols_time = [f'Ounass_Count_T1', f'Ounass_Count_T2', f'{time_comp_competitor_col}_T1', f'{time_comp_competitor_col}_T2']
                                     for col in count_cols_time:
                                          if col in df_time.columns: df_time[col] = pd.to_numeric(df_time[col], errors='coerce').fillna(0).astype(int)
                                          else: st.error(f"Internal Error: Expected column '{col}' missing after merge."); valid_load = False; break
                                     if valid_load:
                                         comp_col_t1 = f"{time_comp_competitor_col}_T1"; comp_col_t2 = f"{time_comp_competitor_col}_T2"
                                         df_time['Ounass_Change'] = (df_time['Ounass_Count_T2'] - df_time['Ounass_Count_T1'])
                                         df_time['Competitor_Change'] = (df_time[comp_col_t2] - df_time[comp_col_t1])
                                         st.session_state.df_time_comparison = df_time; st.session_state.time_comp_meta1 = meta1; st.session_state.time_comp_meta2 = meta2
                                         st.query_params.clear(); st.session_state.selections_by_group[url_key] = set(); st.rerun()
                     else: st.warning("Please select exactly two snapshots from this group to compare.")

# --- OPTIMIZATION: Helper function for displaying single site results ---
def display_single_site_results(df, site_name, processing_flag, input_provided_flag, process_button_pressed):
    """Displays the results (DataFrame, download) for a single site."""
    st.subheader(f"{site_name} Results")
    if df is not None and not df.empty and 'Brand' in df.columns and 'Count' in df.columns:
        st.write(f"Brands Found: {len(df)}")
        df['Count'] = pd.to_numeric(df['Count'], errors='coerce').fillna(0)
        df_display = df.sort_values(by='Count', ascending=False).reset_index(drop=True); df_display.index += 1
        st.dataframe(df_display[['Brand', 'Count']], height=400, use_container_width=True)
        try:
            csv_buffer = io.StringIO(); df_display[['Brand', 'Count']].to_csv(csv_buffer, index=False, encoding='utf-8'); csv_buffer.seek(0)
            download_key = f"{site_name.lower().replace(' ','_')}_dl_disp"; download_filename = f"{site_name.lower().replace(' ','_')}_brands.csv"
            st.download_button(f"Download {site_name} List (CSV)", csv_buffer.getvalue(), download_filename, 'text/csv', key=download_key)
        except Exception as e: st.error(f"Could not generate download for {site_name}: {e}")
    elif not processing_flag and input_provided_flag:
         if process_button_pressed: st.warning(f"No data extracted from {site_name}.")
         else: action = "Upload HTML File" if site_name == "Sephora" else "Enter URL"; st.info(f"{action} for {site_name} and click 'Process'.")
    elif not input_provided_flag: action = "Upload HTML File" if site_name == "Sephora" else "Enter URL"; st.info(f"{action} for {site_name} if you wish to include it.")
    elif processing_flag and (df is None or df.empty): st.warning(f"Data processed for {site_name}, but no valid brands were found matching the extraction rules.")

# --- Unified Display Function (Updated for Competitor) ---
def display_all_results(df_ounass, df_competitor, competitor_name_arg, df_comparison_sorted, stats_title_prefix="Overall Statistics", is_saved_view=False, saved_meta=None):
    global process_button
    stats_title = stats_title_prefix; detected_gender, detected_category = None, None
    ounass_url_for_meta = ''; competitor_input_for_meta = ''; comp_name_for_meta = competitor_name_arg
    if is_saved_view and saved_meta:
        comp_name_for_meta = saved_meta.get('competitor_name', 'Unknown Competitor'); ounass_url_for_meta = saved_meta.get('ounass_url', ''); competitor_input_for_meta = saved_meta.get('competitor_input', 'N/A')
        oun_g, oun_c = extract_info_from_url(ounass_url_for_meta); ls_g, ls_c = None, None
        if comp_name_for_meta == "Level Shoes" and isinstance(competitor_input_for_meta, str) and competitor_input_for_meta.startswith('http'): ls_g, ls_c = extract_info_from_url(competitor_input_for_meta)
        if oun_g or ls_g: detected_gender = oun_g or ls_g;
        if oun_c or ls_c: detected_category = oun_c or ls_c
        ts = saved_meta.get('timestamp', 'N/A'); display_ts_str="N/A"
        try:
             if pytz:
                  dt = ts; tz_name = 'Asia/Dubai'
                  if isinstance(ts, str): dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                  if isinstance(dt, datetime):
                       if dt.tzinfo is None: dt = pytz.utc.localize(dt)
                       display_ts_str = dt.astimezone(pytz.timezone(tz_name)).strftime('%Y-%m-%d %H:%M:%S (%Z)')
             else:
                  if isinstance(ts, datetime): display_ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')
                  elif isinstance(ts, str): display_ts_str = ts[:19].replace('T',' ')
        except Exception: pass
        st.subheader(f"Viewing Saved Comparison (Ounass vs {comp_name_for_meta})")
        st.caption(f"Saved: {display_ts_str} (ID: {saved_meta.get('id', 'N/A')})")
        st.caption(f"Ounass URL: `{ounass_url_for_meta}`")
        competitor_input_label = "URL" if comp_name_for_meta == "Level Shoes" else "File"
        st.caption(f"{comp_name_for_meta} {competitor_input_label}: `{competitor_input_for_meta}`")
    else:
        comp_name_for_meta = competitor_name_arg; ounass_url_for_meta = st.session_state.get('processed_ounass_url') or st.session_state.get('ounass_url_input')
        if comp_name_for_meta == "Level Shoes": competitor_input_for_meta = st.session_state.get('levelshoes_url_input', '')
        else: competitor_input_for_meta = st.session_state.get('competitor_input_identifier', '')
        if ounass_url_for_meta: g_live, c_live = extract_info_from_url(ounass_url_for_meta); detected_gender = g_live; detected_category = c_live
        elif comp_name_for_meta == "Level Shoes" and competitor_input_for_meta: g_live, c_live = extract_info_from_url(competitor_input_for_meta); detected_gender = g_live; detected_category = c_live
    if detected_gender and detected_category: stats_title = f"{stats_title_prefix}: Ounass vs {comp_name_for_meta} - {detected_gender} / {detected_category}"
    elif detected_gender: stats_title = f"{stats_title_prefix}: Ounass vs {comp_name_for_meta} - {detected_gender}"
    elif detected_category: stats_title = f"{stats_title_prefix}: Ounass vs {comp_name_for_meta} - {detected_category}"
    else: stats_title = f"{stats_title_prefix}: Ounass vs {comp_name_for_meta}"
    if not is_saved_view and df_comparison_sorted is not None and not df_comparison_sorted.empty:
        stat_title_col, stat_save_col = st.columns([0.8, 0.2])
        with stat_title_col: st.subheader(stats_title)
        with stat_save_col:
            st.write(""); can_save = bool(ounass_url_for_meta and competitor_input_for_meta); save_help = "Save current comparison results" if can_save else "Cannot save without valid inputs for both sites"
            save_button_key = f"save_live_comp_confirm_{comp_name_for_meta.replace(' ','_')}"
            if st.button("💾 Save", key=save_button_key, help=save_help, use_container_width=True, disabled=not can_save):
                if save_comparison(ounass_url_for_meta, comp_name_for_meta, competitor_input_for_meta, df_comparison_sorted):
                    st.success(f"Comparison saved! ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
                    load_saved_comparisons_meta.clear(); st.session_state.confirm_delete_id = None; st.rerun()
    else: st.subheader(stats_title)
    df_o_safe = df_ounass if df_ounass is not None and not df_ounass.empty else pd.DataFrame(columns=['Brand', 'Count'])
    df_c_safe = df_competitor if df_competitor is not None and not df_competitor.empty else pd.DataFrame(columns=['Brand', 'Count'])
    df_comp_safe = df_comparison_sorted if df_comparison_sorted is not None and not df_comparison_sorted.empty else pd.DataFrame()
    if 'Count' in df_o_safe.columns: df_o_safe['Count'] = pd.to_numeric(df_o_safe['Count'], errors='coerce').fillna(0)
    if 'Count' in df_c_safe.columns: df_c_safe['Count'] = pd.to_numeric(df_c_safe['Count'], errors='coerce').fillna(0)
    total_ounass_brands = len(df_o_safe['Brand'].unique()) if 'Brand' in df_o_safe.columns else 0; total_ounass_products = int(df_o_safe['Count'].sum()) if 'Count' in df_o_safe.columns else 0
    total_competitor_brands = len(df_c_safe['Brand'].unique()) if 'Brand' in df_c_safe.columns else 0; total_competitor_products = int(df_c_safe['Count'].sum()) if 'Count' in df_c_safe.columns else 0
    common_brands_count = 0; ounass_only_count = 0; competitor_only_count = 0; competitor_count_col_name = f"{comp_name_for_meta.replace(' ', '')}_Count"
    if not df_comp_safe.empty and 'Ounass_Count' in df_comp_safe.columns and competitor_count_col_name in df_comp_safe.columns:
        df_comp_safe['Ounass_Count'] = pd.to_numeric(df_comp_safe['Ounass_Count'], errors='coerce').fillna(0)
        df_comp_safe[competitor_count_col_name] = pd.to_numeric(df_comp_safe[competitor_count_col_name], errors='coerce').fillna(0)
        if total_ounass_products == 0: total_ounass_products = int(df_comp_safe['Ounass_Count'].sum())
        if total_competitor_products == 0: total_competitor_products = int(df_comp_safe[competitor_count_col_name].sum())
        if total_ounass_brands == 0: total_ounass_brands = len(df_comp_safe[df_comp_safe['Ounass_Count'] > 0])
        if total_competitor_brands == 0: total_competitor_brands = len(df_comp_safe[df_comp_safe[competitor_count_col_name] > 0])
        common_brands_count = len(df_comp_safe[(df_comp_safe['Ounass_Count'] > 0) & (df_comp_safe[competitor_count_col_name] > 0)])
        ounass_only_count = len(df_comp_safe[(df_comp_safe['Ounass_Count'] > 0) & (df_comp_safe[competitor_count_col_name] == 0)])
        competitor_only_count = len(df_comp_safe[(df_comp_safe['Ounass_Count'] == 0) & (df_comp_safe[competitor_count_col_name] > 0)])
    stat_col1, stat_col2, stat_col3 = st.columns(3)
    with stat_col1: st.metric("Ounass Brands", f"{total_ounass_brands:,}"); st.metric("Ounass Products", f"{total_ounass_products:,}")
    with stat_col2: st.metric(f"{comp_name_for_meta} Brands", f"{total_competitor_brands:,}"); st.metric(f"{comp_name_for_meta} Products", f"{total_competitor_products:,}")
    with stat_col3:
        if not df_comp_safe.empty and 'Ounass_Count' in df_comp_safe.columns and competitor_count_col_name in df_comp_safe.columns: st.metric("Common Brands", f"{common_brands_count:,}"); st.metric("Ounass Only", f"{ounass_only_count:,}"); st.metric(f"{comp_name_for_meta} Only", f"{competitor_only_count:,}")
        else: st.metric("Common Brands", "N/A"); st.metric("Ounass Only", "N/A"); st.metric(f"{comp_name_for_meta} Only", "N/A")
        ounass_input_exists = bool(st.session_state.get('ounass_url_input')); competitor_input_exists = bool(st.session_state.get('levelshoes_url_input') if comp_name_for_meta == "Level Shoes" else st.session_state.get('uploaded_sephora_html'))
        if not is_saved_view and (ounass_input_exists or competitor_input_exists): st.caption("Comparison requires processed data from *both* sites.")
    st.write(""); st.markdown("---")
    if not is_saved_view:
        col1, col2 = st.columns(2)
        with col1: display_single_site_results(st.session_state.get('df_ounass'), "Ounass", st.session_state.get('df_ounass_processed', False), bool(st.session_state.get('ounass_url_input')), process_button)
        with col2: competitor_input_provided = bool(st.session_state.get('levelshoes_url_input') if comp_name_for_meta=="Level Shoes" else st.session_state.get('uploaded_sephora_html')); display_single_site_results(st.session_state.get('df_competitor'), comp_name_for_meta, st.session_state.get('df_competitor_processed', False), competitor_input_provided, process_button)
    if not df_comp_safe.empty:
        if not is_saved_view: st.markdown("---")
        st.subheader(f"Ounass vs {comp_name_for_meta} Brand Comparison"); df_display_comp = df_comp_safe.copy(); df_display_comp.index += 1
        display_cols = ['Display_Brand', 'Ounass_Count', competitor_count_col_name, 'Difference']; missing_cols = [col for col in display_cols if col not in df_display_comp.columns]
        if missing_cols: st.warning(f"Comp table missing: {', '.join(missing_cols)}"); st.dataframe(df_display_comp, height=500, use_container_width=True)
        else: display_rename = {competitor_count_col_name: f"{comp_name_for_meta} Count"}; st.dataframe(df_display_comp[display_cols].rename(columns=display_rename), height=500, use_container_width=True)
        st.markdown("---"); st.subheader("Visual Comparison"); viz_col1, viz_col2 = st.columns(2)
        with viz_col1:
            st.write("**Brand Overlap**"); pie_data = pd.DataFrame({'Category': ['Common Brands', 'Ounass Only', f'{comp_name_for_meta} Only'],'Count': [common_brands_count, ounass_only_count, competitor_only_count]}); pie_data = pie_data[pie_data['Count'] > 0]
            if not pie_data.empty:
                try: fig_pie = px.pie(pie_data, names='Category', values='Count', title="Brand Presence Distribution", color_discrete_sequence=px.colors.qualitative.Pastel); fig_pie.update_traces(textposition='inside', textinfo='percent+label+value'); st.plotly_chart(fig_pie, use_container_width=True)
                except Exception as e: st.error(f"Error creating overlap chart: {e}")
            else: st.info("No data available for overlap chart.")
        with viz_col2:
            st.write(f"**Top 10 Largest Differences (Ounass - {comp_name_for_meta})**")
            if 'Difference' in df_comp_safe.columns and 'Display_Brand' in df_comp_safe.columns:
                df_comp_safe['Difference'] = pd.to_numeric(df_comp_safe['Difference'], errors='coerce'); df_diff_valid = df_comp_safe.dropna(subset=['Difference'])
                top_pos = df_diff_valid[df_diff_valid['Difference'] > 0].nlargest(5, 'Difference'); top_neg = df_diff_valid[df_diff_valid['Difference'] < 0].nsmallest(5, 'Difference'); top_diff = pd.concat([top_pos, top_neg]).sort_values('Difference', ascending=False)
                if not top_diff.empty:
                    try: fig_diff = px.bar(top_diff, x='Display_Brand', y='Difference', title=f"Largest Product Count Differences", labels={'Display_Brand': 'Brand', 'Difference': f'Difference (Ounass - {comp_name_for_meta})'}, color='Difference', color_continuous_scale=px.colors.diverging.RdBu); fig_diff.update_layout(xaxis_title=None); st.plotly_chart(fig_diff, use_container_width=True)
                    except Exception as e: st.error(f"Error creating difference chart: {e}")
                else: st.info("No significant differences found for the chart.")
            else: st.info("Difference data unavailable for the chart.")
        st.markdown("---"); st.subheader("Top 15 Brands Comparison (Total Products Combined)")
        required_top_cols = ['Display_Brand', 'Ounass_Count', competitor_count_col_name]
        if not df_comp_safe.empty and all(c in df_comp_safe.columns for c in required_top_cols):
            df_comp_copy = df_comp_safe.copy(); df_comp_copy['Ounass_Count'] = pd.to_numeric(df_comp_copy['Ounass_Count'], errors='coerce').fillna(0); df_comp_copy[competitor_count_col_name] = pd.to_numeric(df_comp_copy[competitor_count_col_name], errors='coerce').fillna(0)
            df_comp_copy['Total_Count'] = df_comp_copy['Ounass_Count'] + df_comp_copy[competitor_count_col_name]; top_n = 15; top_brands = df_comp_copy.nlargest(top_n, 'Total_Count')
            if not top_brands.empty:
                try:
                    melted = top_brands.melt(id_vars='Display_Brand', value_vars=['Ounass_Count', competitor_count_col_name], var_name='Website', value_name='Product Count'); melted['Website'] = melted['Website'].replace({'Ounass_Count': 'Ounass', competitor_count_col_name: comp_name_for_meta})
                    fig_top = px.bar(melted, x='Display_Brand', y='Product Count', color='Website', barmode='group', title=f"Top {top_n} Brands by Total Products (Combined)", labels={'Display_Brand': 'Brand'}, category_orders={"Display_Brand": top_brands['Display_Brand'].tolist()}); fig_top.update_layout(xaxis_title=None); st.plotly_chart(fig_top, use_container_width=True)
                except Exception as e: st.error(f"Error creating Top {top_n} Brands chart: {e}")
            else: st.info(f"Not enough data to display the Top {top_n} Brands chart.")
        else: st.info(f"Comparison data is unavailable for the Top {top_n} Brands chart.")
        st.markdown("---"); col_comp1, col_comp2 = st.columns(2); req_cols_exist = all(c in df_comp_safe.columns for c in ['Display_Brand', 'Ounass_Count', competitor_count_col_name, 'Difference'])
        with col_comp1:
            st.subheader("Brands in Ounass Only");
            if req_cols_exist:
                df_comp_safe['Ounass_Count'] = pd.to_numeric(df_comp_safe['Ounass_Count'], errors='coerce').fillna(0); df_comp_safe[competitor_count_col_name] = pd.to_numeric(df_comp_safe[competitor_count_col_name], errors='coerce').fillna(0)
                df_f = df_comp_safe[(df_comp_safe[competitor_count_col_name] == 0) & (df_comp_safe['Ounass_Count'] > 0)]
                if not df_f.empty: df_d = df_f[['Display_Brand', 'Ounass_Count']].sort_values('Ounass_Count', ascending=False).reset_index(drop=True); df_d.index += 1; st.dataframe(df_d, height=400, use_container_width=True)
                else: st.info("No unique Ounass brands found.")
            else: st.info("Required data unavailable.")
        with col_comp2:
            st.subheader(f"Brands in {comp_name_for_meta} Only");
            if req_cols_exist:
                df_comp_safe['Ounass_Count'] = pd.to_numeric(df_comp_safe['Ounass_Count'], errors='coerce').fillna(0); df_comp_safe[competitor_count_col_name] = pd.to_numeric(df_comp_safe[competitor_count_col_name], errors='coerce').fillna(0)
                df_f = df_comp_safe[(df_comp_safe['Ounass_Count'] == 0) & (df_comp_safe[competitor_count_col_name] > 0)]
                if not df_f.empty: df_d = df_f[['Display_Brand', competitor_count_col_name]].sort_values(competitor_count_col_name, ascending=False).reset_index(drop=True); df_d.index += 1; st.dataframe(df_d.rename(columns={competitor_count_col_name: f"{comp_name_for_meta} Count"}), height=400, use_container_width=True)
                else: st.info(f"No unique {comp_name_for_meta} brands found.")
            else: st.info(f"Required data unavailable.")
        st.markdown("---"); col_comp3, col_comp4 = st.columns(2)
        with col_comp3:
            st.subheader(f"Common Brands: Ounass > {comp_name_for_meta}");
            if req_cols_exist:
                df_comp_safe['Ounass_Count'] = pd.to_numeric(df_comp_safe['Ounass_Count'], errors='coerce').fillna(0); df_comp_safe[competitor_count_col_name] = pd.to_numeric(df_comp_safe[competitor_count_col_name], errors='coerce').fillna(0); df_comp_safe['Difference'] = pd.to_numeric(df_comp_safe['Difference'], errors='coerce')
                df_f = df_comp_safe[(df_comp_safe['Ounass_Count'] > 0) & (df_comp_safe[competitor_count_col_name] > 0) & (df_comp_safe['Difference'] > 0)].sort_values('Difference', ascending=False)
                if not df_f.empty: df_d = df_f[['Display_Brand', 'Ounass_Count', competitor_count_col_name, 'Difference']].reset_index(drop=True); df_d.index += 1; st.dataframe(df_d.rename(columns={competitor_count_col_name: f"{comp_name_for_meta} Count"}), height=400, use_container_width=True)
                else: st.info(f"No common brands found where Ounass > {comp_name_for_meta}.")
            else: st.info(f"Required data unavailable.")
        with col_comp4:
            st.subheader(f"Common Brands: {comp_name_for_meta} > Ounass");
            if req_cols_exist:
                df_comp_safe['Ounass_Count'] = pd.to_numeric(df_comp_safe['Ounass_Count'], errors='coerce').fillna(0); df_comp_safe[competitor_count_col_name] = pd.to_numeric(df_comp_safe[competitor_count_col_name], errors='coerce').fillna(0); df_comp_safe['Difference'] = pd.to_numeric(df_comp_safe['Difference'], errors='coerce')
                df_f = df_comp_safe[(df_comp_safe['Ounass_Count'] > 0) & (df_comp_safe[competitor_count_col_name] > 0) & (df_comp_safe['Difference'] < 0)].sort_values('Difference', ascending=True)
                if not df_f.empty: df_d = df_f[['Display_Brand', 'Ounass_Count', competitor_count_col_name, 'Difference']].reset_index(drop=True); df_d.index += 1; st.dataframe(df_d.rename(columns={competitor_count_col_name: f"{comp_name_for_meta} Count"}), height=400, use_container_width=True)
                else: st.info(f"No common brands found where {comp_name_for_meta} > Ounass.")
            else: st.info(f"Required data unavailable.")
        st.markdown("---"); dl_cols = ['Display_Brand', 'Ounass_Count', competitor_count_col_name, 'Difference']
        if req_cols_exist:
            try:
                csv_buffer_comparison = io.StringIO(); df_download = df_comp_safe[dl_cols].rename(columns={competitor_count_col_name: f"{comp_name_for_meta}_Count"})
                df_download.to_csv(csv_buffer_comparison, index=False, encoding='utf-8'); csv_buffer_comparison.seek(0); download_label = f"Download {'Saved' if is_saved_view else 'Current'} Comparison (CSV)"
                view_id_part = saved_meta['id'] if is_saved_view and saved_meta else 'live'; download_key = f"comp_dl_button_{'saved' if is_saved_view else 'live'}_{view_id_part}"
                filename_desc = f"Ounass_vs_{comp_name_for_meta.replace(' ','_')}";
                if detected_gender: filename_desc += f"_{detected_gender}"
                if detected_category: filename_desc += f"_{detected_category.replace(' > ','-').replace(' ','_')}"
                filename_desc = filename_desc.lower().replace('/','_'); download_filename = f"brand_comparison_{filename_desc}_{view_id_part}.csv".replace('?_?', 'unknown')
                st.download_button(download_label, csv_buffer_comparison.getvalue(), download_filename, 'text/csv', key=download_key)
            except Exception as e: st.error(f"Could not generate comparison download: {e}")
        else: st.warning("Could not generate comparison download: required columns missing.")
    elif process_button and not is_saved_view: st.markdown("---"); st.warning(f"Comparison (Ounass vs {comp_name_for_meta}) could not be generated. Check individual site results.")


# --- Time Comparison Display Function (Updated for Competitor) ---
def display_time_comparison_results(df_time_comp, meta1, meta2):
    st.markdown("---"); st.subheader("Snapshot Comparison Over Time")
    time_comp_competitor_name = meta1.get('competitor_name', 'Unknown Competitor'); ts_format = '%Y-%m-%d %H:%M (%Z)'
    ts1_str, ts2_str = "N/A", "N/A"; id1, id2 = meta1.get('id','N/A'), meta2.get('id','N/A')
    def format_ts(ts, tz_name='Asia/Dubai'):
        if ts is None: return "N/A"
        try:
            dt = ts;
            if isinstance(ts, str): dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            if isinstance(dt, datetime):
                if pytz and tz_name:
                    if dt.tzinfo is None: dt = pytz.utc.localize(dt)
                    return dt.astimezone(pytz.timezone(tz_name)).strftime(ts_format)
                else: return dt.strftime('%Y-%m-%d %H:%M')
            return str(ts)
        except Exception as e: print(f"Timestamp formatting error: {e}"); return str(ts)[:16]
    ts1_str = format_ts(meta1.get('timestamp')); ts2_str = format_ts(meta2.get('timestamp'))
    comparison_markdown = (f"Comparing Ounass vs **{time_comp_competitor_name}**:\n"
                           f"* **Snapshot 1 (Earlier):** `{ts1_str}` (ID: {id1})\n"
                           f"* **Snapshot 2 (Later):**   `{ts2_str}` (ID: {id2})")
    st.markdown(comparison_markdown)
    with st.expander("Show URLs/Inputs for Compared Snapshots"):
        input_label1 = "URL" if meta1.get('competitor_name') == "Level Shoes" else "File"; input_label2 = "URL" if meta2.get('competitor_name') == "Level Shoes" else "File"
        st.caption(f"**Snap 1 ({ts1_str}):** O: `{meta1.get('ounass_url', 'N/A')}` | {meta1.get('competitor_name')}({input_label1}): `{meta1.get('competitor_input', 'N/A')}`")
        st.caption(f"**Snap 2 ({ts2_str}):** O: `{meta2.get('ounass_url', 'N/A')}` | {meta2.get('competitor_name')}({input_label2}): `{meta2.get('competitor_input', 'N/A')}`")
    st.markdown("---")
    competitor_col_base = time_comp_competitor_name.replace(' ', ''); competitor_col_t1 = f"{competitor_col_base}_Count_T1"; competitor_col_t2 = f"{competitor_col_base}_Count_T2"
    req_time_cols = ['Display_Brand', 'Ounass_Count_T1', 'Ounass_Count_T2', 'Ounass_Change', competitor_col_t1, competitor_col_t2, 'Competitor_Change']
    missing_cols_time = [col for col in req_time_cols if col not in df_time_comp.columns]
    if missing_cols_time: st.error(f"Time comparison data missing required columns: {', '.join(missing_cols_time)}."); st.dataframe(df_time_comp); return
    numeric_cols = ['Ounass_Count_T1', 'Ounass_Count_T2', 'Ounass_Change', competitor_col_t1, competitor_col_t2, 'Competitor_Change']
    for col in numeric_cols: df_time_comp[col] = pd.to_numeric(df_time_comp[col], errors='coerce').fillna(0)
    new_o=df_time_comp[(df_time_comp['Ounass_Count_T1']==0)&(df_time_comp['Ounass_Count_T2']>0)].copy(); drop_o=df_time_comp[(df_time_comp['Ounass_Count_T1']>0)&(df_time_comp['Ounass_Count_T2']==0)].copy()
    inc_o=df_time_comp[(df_time_comp['Ounass_Change']>0) & (df_time_comp['Ounass_Count_T1'] > 0)].copy(); dec_o=df_time_comp[(df_time_comp['Ounass_Change']<0)].copy()
    new_c=df_time_comp[(df_time_comp[competitor_col_t1]==0)&(df_time_comp[competitor_col_t2]>0)].copy(); drop_c=df_time_comp[(df_time_comp[competitor_col_t1]>0)&(df_time_comp[competitor_col_t2]==0)].copy()
    inc_c=df_time_comp[(df_time_comp['Competitor_Change']>0) & (df_time_comp[competitor_col_t1] > 0)].copy(); dec_c=df_time_comp[(df_time_comp['Competitor_Change']<0)].copy()
    st.subheader("Summary of Changes Between Snapshots"); t_stat_col1, t_stat_col2 = st.columns(2)
    with t_stat_col1: st.metric("New Brands (Ounass)", len(new_o)); st.metric("Dropped Brands (Ounass)", len(drop_o)); st.metric("Increased Count Brands (Ounass)", len(inc_o)); st.metric("Decreased Count Brands (Ounass)", len(dec_o[dec_o['Ounass_Count_T2'] > 0])); st.metric("Net Product Change (Ounass)", f"{int(df_time_comp['Ounass_Change'].sum()):+,}")
    with t_stat_col2: st.metric(f"New Brands ({time_comp_competitor_name})", len(new_c)); st.metric(f"Dropped Brands ({time_comp_competitor_name})", len(drop_c)); st.metric(f"Increased Count Brands ({time_comp_competitor_name})", len(inc_c)); st.metric(f"Decreased Count Brands ({time_comp_competitor_name})", len(dec_c[dec_c[competitor_col_t2] > 0])); st.metric(f"Net Product Change ({time_comp_competitor_name})", f"{int(df_time_comp['Competitor_Change'].sum()):+,}")
    st.markdown("---"); st.subheader("Detailed Brand Changes"); tc_col1, tc_col2 = st.columns(2); height=250
    def display_change_df(df_change, category_name, site_prefix, cols_to_select, rename_map, sort_col, sort_ascending):
        if not df_change.empty:
            valid_cols_to_select = [col for col in cols_to_select if col in df_change.columns]
            if not valid_cols_to_select: st.error(f"Internal Error: No valid columns for '{category_name}' ({site_prefix})."); return False
            st.write(f"**{category_name}** ({len(df_change)}):")
            try:
                df_display = df_change[valid_cols_to_select].rename(columns=rename_map); final_sort_col = rename_map.get(sort_col, sort_col)
                if final_sort_col in df_display.columns and pd.api.types.is_numeric_dtype(df_display[final_sort_col]): df_display = df_display.sort_values(final_sort_col, ascending=sort_ascending).reset_index(drop=True); df_display.index += 1
                else: sort_key_display = 'Brand' if 'Brand' in df_display.columns else df_display.columns[0]; df_display = df_display.sort_values(sort_key_display, ascending=True).reset_index(drop=True); df_display.index += 1; print(f"Sorted {category_name} ({site_prefix}) by {sort_key_display}.")
                st.dataframe(df_display, height=height, use_container_width=True); return True
            except Exception as e: st.error(f"Unexpected error displaying '{category_name}' ({site_prefix}) details: {e}"); return False
        return False
    with tc_col1:
        st.write(f"#### Ounass Changes ({ts1_str} vs {ts2_str})"); displayed_any_o = False
        cols_new_o = ['Display_Brand', 'Ounass_Count_T2']; map_new_o = {'Display_Brand':'Brand', 'Ounass_Count_T2': 'Now'}
        cols_drop_o = ['Display_Brand', 'Ounass_Count_T1']; map_drop_o = {'Display_Brand':'Brand', 'Ounass_Count_T1': 'Was'}
        cols_inc_dec_o = ['Display_Brand', 'Ounass_Count_T1', 'Ounass_Count_T2', 'Ounass_Change']; map_inc_dec_o = {'Display_Brand':'Brand', 'Ounass_Count_T1':'Was', 'Ounass_Count_T2':'Now', 'Ounass_Change':'Change'}
        if display_change_df(new_o, "New Brands", "Ounass", cols_new_o, map_new_o, 'Ounass_Count_T2', False): displayed_any_o = True
        if display_change_df(drop_o, "Dropped Brands", "Ounass", cols_drop_o, map_drop_o, 'Ounass_Count_T1', False): displayed_any_o = True
        if display_change_df(inc_o, "Increased Count", "Ounass", cols_inc_dec_o, map_inc_dec_o, 'Ounass_Change', False): displayed_any_o = True
        dec_o_display = dec_o[dec_o['Ounass_Count_T2'] > 0]
        if display_change_df(dec_o_display, "Decreased Count", "Ounass", cols_inc_dec_o, map_inc_dec_o, 'Ounass_Change', True): displayed_any_o = True
        if not displayed_any_o: st.info("No significant brand count changes detected for Ounass.")
    with tc_col2:
        st.write(f"#### {time_comp_competitor_name} Changes ({ts1_str} vs {ts2_str})"); displayed_any_c = False
        cols_new_c = ['Display_Brand', competitor_col_t2]; map_new_c = {'Display_Brand':'Brand', competitor_col_t2: 'Now'}
        cols_drop_c = ['Display_Brand', competitor_col_t1]; map_drop_c = {'Display_Brand':'Brand', competitor_col_t1: 'Was'}
        cols_inc_dec_c = ['Display_Brand', competitor_col_t1, competitor_col_t2, 'Competitor_Change']; map_inc_dec_c = {'Display_Brand':'Brand', competitor_col_t1:'Was', competitor_col_t2:'Now', 'Competitor_Change':'Change'}
        if display_change_df(new_c, "New Brands", time_comp_competitor_name, cols_new_c, map_new_c, competitor_col_t2, False): displayed_any_c = True
        if display_change_df(drop_c, "Dropped Brands", time_comp_competitor_name, cols_drop_c, map_drop_c, competitor_col_t1, False): displayed_any_c = True
        if display_change_df(inc_c, "Increased Count", time_comp_competitor_name, cols_inc_dec_c, map_inc_dec_c, 'Competitor_Change', False): displayed_any_c = True
        dec_c_display = dec_c[dec_c[competitor_col_t2] > 0]
        if display_change_df(dec_c_display, "Decreased Count", time_comp_competitor_name, cols_inc_dec_c, map_inc_dec_c, 'Competitor_Change', True): displayed_any_c = True
        if not displayed_any_c: st.info(f"No significant brand count changes detected for {time_comp_competitor_name}.")
    st.markdown("---")
    if not missing_cols_time:
         try:
             csv_buffer_time = io.StringIO(); download_rename_map = {competitor_col_t1: f"{time_comp_competitor_name}_Count_T1", competitor_col_t2: f"{time_comp_competitor_name}_Count_T2", 'Competitor_Change': f"{time_comp_competitor_name}_Change"}
             df_download_time = df_time_comp[req_time_cols].rename(columns=download_rename_map); df_download_time.to_csv(csv_buffer_time, index=False, encoding='utf-8'); csv_buffer_time.seek(0)
             st.download_button(label=f"Download Time Comparison Data", data=csv_buffer_time.getvalue(), file_name=f"time_comparison_{id1}_vs_{id2}.csv", mime='text/csv', key='time_comp_dl_button')
         except Exception as e: st.error(f"Could not generate time comparison download: {e}")
    else: st.warning("Could not generate time comparison download: required data columns missing.")


# --- Main Application Flow ---
init_db()
confirm_id = st.session_state.get('confirm_delete_id'); viewing_saved_id = st.query_params.get("view_id", [None])[0]
if confirm_id:
    st.warning(f"Are you sure you want to delete comparison ID {confirm_id}?"); col_confirm, col_cancel, _ = st.columns([1,1,3])
    with col_confirm:
        if st.button("Yes, Delete", type="primary", key=f"confirm_delete_{confirm_id}"):
            if delete_comparison(confirm_id): st.success(f"Comparison ID {confirm_id} deleted.")
            else: st.error(f"Deletion failed for ID {confirm_id}.")
            st.session_state.confirm_delete_id = None; st.query_params.clear(); st.rerun()
    with col_cancel:
        if st.button("Cancel", key=f"cancel_delete_{confirm_id}"): st.session_state.confirm_delete_id = None; st.rerun()
elif 'df_time_comparison' in st.session_state and not st.session_state.df_time_comparison.empty: display_time_comparison_results(st.session_state.df_time_comparison, st.session_state.get('time_comp_meta1',{}), st.session_state.get('time_comp_meta2',{}))
elif viewing_saved_id:
    saved_meta, saved_df = load_specific_comparison(viewing_saved_id)
    if saved_meta and saved_df is not None: display_all_results(None, None, saved_meta.get('competitor_name', 'Level Shoes'), saved_df, stats_title_prefix="Saved Comparison Details", is_saved_view=True, saved_meta=saved_meta)
    else:
        if st.button("Clear Invalid Saved View URL & Go Back"): st.query_params.clear(); st.rerun()
else:
    if process_button:
        st.session_state.df_ounass = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned']); st.session_state.df_competitor = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned'])
        st.session_state.ounass_data = []; st.session_state.competitor_data = []; st.session_state.df_comparison_sorted = pd.DataFrame()
        st.session_state.processed_ounass_url = ''; st.session_state.df_ounass_processed = False; st.session_state.df_competitor_processed = False
        ounass_processed_ok = False
        if st.session_state.ounass_url_input:
            with st.spinner("Processing Ounass URL..."):
                st.session_state.processed_ounass_url = ensure_ounass_full_list_parameter(st.session_state.ounass_url_input); ounass_html_content = fetch_html_content(st.session_state.processed_ounass_url)
                if ounass_html_content: st.session_state.ounass_data = ounass_extractor.get_processed_ounass_data(ounass_html_content)
                if st.session_state.ounass_data:
                    try:
                        df_o = pd.DataFrame(st.session_state.ounass_data)
                        if not df_o.empty and 'Brand' in df_o.columns and 'Count' in df_o.columns:
                             df_o['Count'] = pd.to_numeric(df_o['Count'], errors='coerce').fillna(0); df_o = df_o[df_o['Count'] > 0]
                             if not df_o.empty: df_o['Brand_Cleaned'] = df_o['Brand'].apply(clean_brand_name); st.session_state.df_ounass = df_o; st.session_state.df_ounass_processed = True; ounass_processed_ok = True
                             else: print("Warning: Ounass data filtered out.")
                        else: print("Warning: Ounass DF invalid.")
                    except Exception as e: st.error(f"Error creating Ounass DF: {e}")
        else: st.warning("Ounass URL is required.")
        competitor_processed_ok = False; competitor_name_live = st.session_state.competitor_selection
        if competitor_name_live == "Level Shoes":
            if st.session_state.levelshoes_url_input:
                 with st.spinner("Processing Level Shoes URL..."):
                    st.session_state.competitor_input_identifier = st.session_state.levelshoes_url_input; levelshoes_html_content = fetch_html_content(st.session_state.levelshoes_url_input)
                    if levelshoes_html_content: st.session_state.competitor_data = levelshoes_extractor.get_processed_levelshoes_data(levelshoes_html_content)
                    if st.session_state.competitor_data:
                        try:
                            df_ls = pd.DataFrame(st.session_state.competitor_data)
                            if not df_ls.empty and 'Brand' in df_ls.columns and 'Count' in df_ls.columns:
                                df_ls['Count'] = pd.to_numeric(df_ls['Count'], errors='coerce').fillna(0); df_ls = df_ls[df_ls['Count'] > 0]
                                if not df_ls.empty: df_ls['Brand_Cleaned'] = df_ls['Brand'].apply(clean_brand_name); st.session_state.df_competitor = df_ls; st.session_state.df_competitor_processed = True; competitor_processed_ok = True
                                else: print("Warning: Level Shoes data filtered out.")
                            else: print("Warning: Level Shoes DF invalid.")
                        except Exception as e: st.error(f"Error creating Level Shoes DF: {e}")
            else: st.warning("Level Shoes URL is required.")
        elif competitor_name_live == "Sephora":
             sephora_html_to_process = st.session_state.get('uploaded_sephora_html')
             if sephora_html_to_process:
                  with st.spinner("Processing Sephora HTML File..."): st.session_state.competitor_data = sephora_extractor.get_processed_sephora_data(sephora_html_to_process)
                  if st.session_state.competitor_data:
                       try:
                           df_s = pd.DataFrame(st.session_state.competitor_data)
                           if not df_s.empty and 'Brand' in df_s.columns and 'Count' in df_s.columns:
                                df_s['Count'] = pd.to_numeric(df_s['Count'], errors='coerce').fillna(0); df_s = df_s[df_s['Count'] > 0]
                                if not df_s.empty: df_s['Brand_Cleaned'] = df_s['Brand'].apply(clean_brand_name); st.session_state.df_competitor = df_s; st.session_state.df_competitor_processed = True; competitor_processed_ok = True
                                else: print("Warning: Sephora data filtered out.")
                           else: print("Warning: Sephora DF invalid.")
                       except Exception as e: st.error(f"Error creating Sephora DF: {e}")
             else: st.warning("Sephora HTML file upload is required.")
        if st.session_state.ounass_url_input and not ounass_processed_ok: st.warning("Could not process Ounass URL."); st.session_state.df_ounass_processed = False
        competitor_input_provided_live = bool(st.session_state.levelshoes_url_input if competitor_name_live=="Level Shoes" else st.session_state.uploaded_sephora_html)
        if competitor_input_provided_live and not competitor_processed_ok: input_type = "URL" if competitor_name_live == "Level Shoes" else "HTML File"; st.warning(f"Could not process {competitor_name_live} {input_type}."); st.session_state.df_competitor_processed = False
        if st.session_state.df_ounass_processed and st.session_state.df_competitor_processed:
            with st.spinner(f"Generating Ounass vs {competitor_name_live} comparison..."):
                try:
                    df_o = st.session_state.df_ounass[['Brand','Count','Brand_Cleaned']].copy(); df_c = st.session_state.df_competitor[['Brand','Count','Brand_Cleaned']].copy()
                    competitor_suffix = f"_{competitor_name_live.replace(' ', '')}"
                    df_comp = pd.merge(df_o, df_c, on='Brand_Cleaned', how='outer', suffixes=('_Ounass', competitor_suffix))
                    ounass_count_col = 'Count_Ounass'; competitor_count_col = f'Count{competitor_suffix}'; ounass_brand_col = 'Brand_Ounass'; competitor_brand_col = f'Brand{competitor_suffix}'
                    df_comp['Ounass_Count'] = pd.to_numeric(df_comp[ounass_count_col], errors='coerce').fillna(0).astype(int); final_competitor_count_col = f"{competitor_name_live.replace(' ','')}_Count"
                    df_comp[final_competitor_count_col] = pd.to_numeric(df_comp[competitor_count_col], errors='coerce').fillna(0).astype(int); df_comp['Difference'] = df_comp['Ounass_Count'] - df_comp[final_competitor_count_col]
                    df_comp['Display_Brand'] = df_comp[ounass_brand_col]
                    if competitor_brand_col in df_comp.columns: df_comp['Display_Brand'] = df_comp['Display_Brand'].fillna(df_comp[competitor_brand_col])
                    df_comp['Display_Brand'] = df_comp['Display_Brand'].fillna(df_comp['Brand_Cleaned']); df_comp['Display_Brand'].fillna("Unknown", inplace=True)
                    final_cols_ordered = ['Display_Brand', 'Brand_Cleaned', 'Ounass_Count', final_competitor_count_col, 'Difference', ounass_brand_col, competitor_brand_col]
                    for col in final_cols_ordered:
                        if col not in df_comp.columns: df_comp[col] = np.nan
                    df_comp['Total_Count'] = df_comp['Ounass_Count'] + df_comp[final_competitor_count_col]
                    st.session_state.df_comparison_sorted = df_comp.sort_values(by=['Total_Count', 'Ounass_Count', 'Display_Brand'], ascending=[False, False, True]).reset_index(drop=True)[final_cols_ordered + ['Total_Count']]
                except Exception as merge_e: st.error(f"Error during comparison merge: {merge_e}"); st.session_state.df_comparison_sorted = pd.DataFrame()
        else: st.session_state.df_comparison_sorted = pd.DataFrame(); print("Comparison skipped.")
        st.rerun()
    df_ounass_live = st.session_state.get('df_ounass'); df_competitor_live = st.session_state.get('df_competitor'); df_comparison_sorted_live = st.session_state.get('df_comparison_sorted'); live_competitor_name = st.session_state.competitor_selection
    display_all_results(df_ounass_live, df_competitor_live, live_competitor_name, df_comparison_sorted_live, stats_title_prefix="Current Comparison")

# --- END OF UPDATED FILE ---
