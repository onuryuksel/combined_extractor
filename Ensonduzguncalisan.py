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
import json
from collections import defaultdict

# --- App Configuration ---
APP_VERSION = "2.4.5" # Added subcategory detection
st.set_page_config(layout="wide", page_title="Ounass vs Level Shoes PLP Comparison")

# --- App Title and Info ---
st.title(f"Ounass vs Level Shoes PLP Designer Comparison (v{APP_VERSION})")
st.write("Enter Product Listing Page (PLP) URLs from Ounass and Level Shoes (Women's Shoes/Bags recommended) to extract and compare designer brand counts, or compare previously saved snapshots.")
st.info("Ensure the URLs point to the relevant listing pages. For Ounass, the tool will attempt to load all designers.")

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
        conn = get_db_connection(); conn.execute('CREATE TABLE IF NOT EXISTS comparisons (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL, ounass_url TEXT NOT NULL, levelshoes_url TEXT NOT NULL, comparison_data TEXT NOT NULL, comparison_name TEXT)'); conn.commit(); conn.close()
    except Exception as e: st.error(f"Fatal DB Init Error: {e}")
def save_comparison(ounass_url, levelshoes_url, df_comparison):
    if df_comparison is None or df_comparison.empty: st.error("Cannot save empty comparison data."); return False
    try:
        conn = get_db_connection(); timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S"); df_to_save = df_comparison.copy(); cols_to_save = ['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference', 'Brand_Cleaned', 'Brand_Ounass', 'Brand_LevelShoes'];
        for col in cols_to_save:
            if col not in df_to_save.columns: df_to_save[col] = np.nan
        data_json = df_to_save[cols_to_save].to_json(orient="records", date_format="iso")
        conn.execute("INSERT INTO comparisons (timestamp, ounass_url, levelshoes_url, comparison_data, comparison_name) VALUES (?, ?, ?, ?, ?)",(timestamp, ounass_url, levelshoes_url, data_json, None)); conn.commit(); conn.close(); return True
    except Exception as e: st.error(f"Database Error: Could not save comparison - {e}"); return False
def load_saved_comparisons_meta():
    try:
        conn = get_db_connection(); comparisons = conn.execute("SELECT id, timestamp, ounass_url, levelshoes_url, comparison_name FROM comparisons ORDER BY timestamp DESC").fetchall(); conn.close(); return [dict(row) for row in comparisons] if comparisons else []
    except Exception as e: st.error(f"Database Error: Could not load saved comparisons - {e}"); return []
def load_specific_comparison(comp_id):
    try:
        conn = get_db_connection(); comp = conn.execute("SELECT timestamp, ounass_url, levelshoes_url, comparison_data, comparison_name FROM comparisons WHERE id = ?", (comp_id,)).fetchone(); conn.close();
        if comp:
            fallback_name = f"ID {comp_id} ({comp['timestamp']})"
            meta = {"timestamp": comp["timestamp"], "ounass_url": comp["ounass_url"], "levelshoes_url": comp["levelshoes_url"], "name": comp["comparison_name"] or fallback_name}
            df = pd.read_json(comp["comparison_data"], orient="records")
            if 'Difference' not in df.columns and 'Ounass_Count' in df.columns and 'LevelShoes_Count' in df.columns: df['Difference'] = df['Ounass_Count'] - df['LevelShoes_Count']
            if 'Display_Brand' not in df.columns:
                brand_ounass_col = 'Brand_Ounass' if 'Brand_Ounass' in df.columns else None; brand_ls_col = 'Brand_LevelShoes' if 'Brand_LevelShoes' in df.columns else None; brand_cleaned_col = 'Brand_Cleaned' if 'Brand_Cleaned' in df.columns else None;
                df['Display_Brand'] = np.where(df['Ounass_Count'] > 0, df[brand_ounass_col] if brand_ounass_col else df[brand_cleaned_col] if brand_cleaned_col else "Unknown", df[brand_ls_col] if brand_ls_col else df[brand_cleaned_col] if brand_cleaned_col else "Unknown")
                if brand_cleaned_col: df['Display_Brand'].fillna(df[brand_cleaned_col].fillna("Unknown"), inplace=True)
                else: df['Display_Brand'].fillna("Unknown", inplace=True)
            return meta, df
        else: st.warning(f"Saved comparison with ID {comp_id} not found."); return None, None
    except Exception as e: st.error(f"Database Error: Could not load comparison ID {comp_id} - {e}"); return None, None
def delete_comparison(comp_id):
    try: conn = get_db_connection(); conn.execute("DELETE FROM comparisons WHERE id = ?", (comp_id,)); conn.commit(); conn.close(); return True
    except Exception as e: st.error(f"Database Error: Could not delete comparison ID {comp_id} - {e}"); return False

# --- Helper Functions ---
def clean_brand_name(brand_name):
    if not isinstance(brand_name, str): return ""
    cleaned = brand_name.upper().replace('-', '').replace('&', '').replace('.', '').replace("'", '').replace(" ", '')
    cleaned = unicodedata.normalize('NFKD', cleaned).encode('ascii', 'ignore').decode('utf-8')
    cleaned = ''.join(c for c in cleaned if c.isalnum())
    return cleaned
def custom_scorer(s1, s2):
    scores = [fuzz.ratio(s1, s2), fuzz.partial_ratio(s1, s2), fuzz.token_set_ratio(s1, s2), fuzz.token_sort_ratio(s1, s2)]; return max(scores)

# --- HTML Processing Functions ---
# (process_ounass_html and process_levelshoes_html remain unchanged)
def process_ounass_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser'); data = []
    try:
        h = soup.find(lambda t: t.name=='header' and 'Designer' in t.get_text(strip=True) and t.find_parent('section', class_='Facet')); sec = h.find_parent('section', class_='Facet') if h else None
        if sec:
            items = sec.select('ul > li > a.FacetLink') or sec.find_all('a', href=True, class_=lambda x: x and 'FacetLink' in x)
            if not items: st.warning("Ounass: Could not find brand list elements.")
            else:
                for item in items:
                    try:
                        n_span = item.find('span', class_='FacetLink-name')
                        if n_span:
                            c_span=n_span.find('span', class_='FacetLink-count'); c_text=c_span.text.strip() if c_span else "(0)"; tmp_span=BeautifulSoup(str(n_span), 'html.parser').find(class_='FacetLink-name'); c_span_tmp=tmp_span.find(class_='FacetLink-count');
                            if c_span_tmp: c_span_tmp.decompose()
                            d_name = tmp_span.text.strip(); match = re.search(r'\((\d+)\)', c_text); count = int(match.group(1)) if match else 0
                            if d_name and "SHOW" not in d_name.upper(): data.append({'Brand': d_name, 'Count': count})
                    except Exception: pass
        else: st.warning("Ounass: Could not find the 'Designer' section structure.")
    except Exception as e: st.error(f"Ounass: Parsing error: {e}"); return []
    if not data and html_content: st.warning("Ounass: No brand data extracted.");
    return data
def process_levelshoes_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser'); data = []
    try:
        h4 = soup.find('h4', text='Designer', attrs={'data-cy': 'CategoryFacets-label'}) or soup.find(lambda t: t.name=='h4' and 'Designer' in t.get_text(strip=True))
        container = None
        if h4:
            curr = h4.parent; lvl=0
            while curr and curr.name != 'body' and lvl < 10:
                lvl += 1
                if curr.name == 'div' and 'accordion-root' in curr.get('class', []):
                    ul = curr.find('ul');
                    if ul and ul.find('label'): container = curr; break
                curr = curr.parent
        if container:
            ul = container.find('ul'); items = []
            if ul:
                items = [li for li in ul.find_all('li', recursive=False, limit=300) if li.find('label') and li.find('p')]
                if not items: items = [li for li in ul.find_all('li', limit=300) if li.find('label') and li.find('p')]
            if not items: st.warning("Level Shoes: No brand list items found in container.")
            else:
                for item in items:
                    try:
                        lbl = item.find('label')
                        if lbl:
                            p = lbl.find_all('p', limit=2)
                            if len(p) == 2:
                                d_name = p[0].text.strip(); c_text = p[1].text.strip(); match = re.search(r'\((\d+)\)', c_text); count = int(match.group(1)) if match else 0
                                if d_name and "VIEW ALL" not in d_name.upper() and "SHOW M" not in d_name.upper() and "SHOW L" not in d_name.upper(): data.append({'Brand': d_name, 'Count': count})
                    except Exception: pass
        else: st.warning("Level Shoes: Could not find the designer filter container.")
    except Exception as e: st.error(f"Level Shoes: Parsing error: {e}"); return []
    if not data and html_content: st.warning("Level Shoes: No brand data extracted.");
    return data

# --- Function to fetch HTML content from URL ---
def fetch_html_content(url):
    if not url: st.error("Fetch error: URL cannot be empty."); return None
    try:
        h = {'User-Agent':'Mozilla/5.0','Accept':'text/html','Accept-Language':'en-US,en;q=0.9','Connection':'keep-alive'}
        r = requests.get(url, headers=h, timeout=30); r.raise_for_status(); return r.text
    except requests.exceptions.Timeout: st.error(f"Error: Timeout fetching {url}"); return None
    except requests.exceptions.RequestException as e: st.error(f"Error fetching {url}: {e}"); return None

# --- Function to ensure Ounass URL has the correct parameter ---
def ensure_ounass_full_list_parameter(url):
    p, v = 'fh_maxdisplaynrvalues_designer', '-1'
    try:
        if not url: return url
        chk = urlparse(url);
        if 'ounass' not in chk.netloc.lower(): return url
    except Exception: return url
    try:
        par = urlparse(url); qp = parse_qs(par.query, keep_blank_values=True)
        upd = True if p not in qp or not qp[p] or qp[p][0] != v else False
        if upd: qp[p] = [v]; qs = urlencode(qp, doseq=True); uc = list(par); uc[4] = qs; return urlunparse(uc)
        else: return url
    except Exception as e: st.warning(f"Error processing Ounass URL: {e}"); return url

# --- !!! UPDATED URL Info Extraction Function !!! ---
def extract_info_from_url(url):
    """Attempts to extract gender and category/subcategory from Ounass/LevelShoes URL paths."""
    try:
        if not url: return None, None
        parsed = urlparse(url)
        # Filter out empty segments and ignore common noise immediately
        ignore_segments = ['ae', 'com', 'en', 'shop', 'category', 'all', 'view-all', 'plp']
        path_segments = [s for s in parsed.path.split('/') if s and s.lower() not in ignore_segments]

        if not path_segments:
            return None, None

        gender_keywords = ["women", "men", "kids", "unisex"]
        gender = None
        category_parts_raw = []

        # Check first *remaining* segment for gender
        if path_segments[0].lower() in gender_keywords:
            gender = path_segments[0].title()
            category_parts_raw = path_segments[1:] # The rest are potential category parts
        else:
            # Assume all remaining segments are category parts if first isn't gender
            category_parts_raw = path_segments

        # Clean and join category parts
        cleaned_category_parts = []
        for part in category_parts_raw:
            # Further check within category parts - skip if it's a gender keyword misplaced
            if gender and part.lower() in gender_keywords:
                continue
            cleaned = part.replace('.html', '').replace('-', ' ').strip()
            if cleaned:
                # Capitalize each word in the segment
                cleaned_category_parts.append(' '.join(word.capitalize() for word in cleaned.split()))

        category = " > ".join(cleaned_category_parts) if cleaned_category_parts else None

        return gender, category

    except Exception as e:
        # Optional: Log error to sidebar or console for debugging
        # st.sidebar.caption(f"URL Info Extraction Error: {e}")
        return None, None
# !!! END URL Info Extraction Function !!!

# Initialize Database
init_db()

# --- Sidebar ---
st.sidebar.image("https://1000logos.net/wp-content/uploads/2021/05/Ounass-logo.png", width=150)
st.sidebar.caption(f"App Version: {APP_VERSION}")
st.sidebar.header("Enter URLs")
st.session_state.ounass_url_input = st.sidebar.text_input("Ounass URL", key="ounass_url_widget", value=st.session_state.ounass_url_input)
st.session_state.levelshoes_url_input = st.sidebar.text_input("Level Shoes URL", key="levelshoes_url_widget", value=st.session_state.levelshoes_url_input)
process_button = st.sidebar.button("Process URLs")

# --- Saved Comparisons Sidebar ---
st.sidebar.markdown("---")
st.sidebar.subheader("Saved Comparisons")
saved_comps_meta = load_saved_comparisons_meta()
query_params = st.query_params.to_dict()
viewing_saved_id = query_params.get("view_id", [None])[0]
if viewing_saved_id and st.session_state.get('confirm_delete_id') != viewing_saved_id :
     if st.sidebar.button("<< Back to Live Processing", key="back_live", use_container_width=True): st.query_params.clear(); st.session_state.confirm_delete_id = None; st.rerun()
if not saved_comps_meta: st.sidebar.caption("No comparisons saved yet.")
else:
    grouped_comps = defaultdict(list);
    for comp_meta in saved_comps_meta: url_key = (comp_meta['ounass_url'], comp_meta['levelshoes_url']); grouped_comps[url_key].append(comp_meta)
    st.sidebar.caption("Select two snapshots from the same group to compare.")
    url_group_keys = list(grouped_comps.keys())
    if 'selected_url_key_for_time_comp' not in st.session_state: st.session_state.selected_url_key_for_time_comp = None
    for idx, url_key in enumerate(url_group_keys):
        comps_list = grouped_comps[url_key]; g, c = extract_info_from_url(url_key[0] or url_key[1]); expander_label = f"{g or '?'} / {c or '?'} ({len(comps_list)})"
        if not g and not c: oun_path = urlparse(url_key[0]).path.split('/')[-1] or "Ounass"; ls_path = urlparse(url_key[1]).path.split('/')[-1].replace('.html','') or "Level"; expander_label = f"{oun_path} vs {ls_path} ({len(comps_list)})"
        is_expanded = st.session_state.selected_url_key_for_time_comp == url_key
        with st.sidebar.expander(expander_label, expanded=is_expanded):
            comp_options = {f"{datetime.fromisoformat(comp['timestamp']).strftime('%Y-%m-%d %H:%M')} (ID: {comp['id']})": comp['id'] for comp in comps_list}; options_list = list(comp_options.keys()); ids_list = list(comp_options.values())
            if st.button("Select this group for Time Comparison", key=f"select_group_{idx}", use_container_width=True):
                 st.session_state.selected_url_key_for_time_comp = url_key; st.session_state.time_comp_id1 = None; st.session_state.time_comp_id2 = None; st.session_state.df_time_comparison = pd.DataFrame(); st.rerun()
            if st.session_state.selected_url_key_for_time_comp == url_key:
                st.caption("Select two snapshots:"); current_idx1 = ids_list.index(st.session_state.time_comp_id1) if st.session_state.time_comp_id1 in ids_list else 0; current_idx2 = ids_list.index(st.session_state.time_comp_id2) if st.session_state.time_comp_id2 in ids_list else min(1, len(ids_list)-1)
                selected_option1 = st.selectbox("Snapshot 1 (Older/Base):", options=options_list, index=current_idx1, key=f"time_sel1_{idx}", label_visibility="collapsed"); selected_option2 = st.selectbox("Snapshot 2 (Newer):", options=options_list, index=current_idx2, key=f"time_sel2_{idx}", label_visibility="collapsed")
                st.session_state.time_comp_id1 = comp_options.get(selected_option1); st.session_state.time_comp_id2 = comp_options.get(selected_option2)
                if st.button("Compare Snapshots", key=f"compare_time_{idx}", use_container_width=True, disabled=(len(ids_list)<2)):
                    if st.session_state.time_comp_id1 and st.session_state.time_comp_id2 and st.session_state.time_comp_id1 != st.session_state.time_comp_id2:
                        meta1, df1 = load_specific_comparison(st.session_state.time_comp_id1); meta2, df2 = load_specific_comparison(st.session_state.time_comp_id2)
                        if meta1 and df1 is not None and meta2 and df2 is not None:
                            ts1 = datetime.fromisoformat(meta1['timestamp']); ts2 = datetime.fromisoformat(meta2['timestamp'])
                            if ts1 > ts2: meta1, df1, meta2, df2 = meta2, df2, meta1, df1
                            if 'Display_Brand' not in df1.columns: df1['Display_Brand'] = df1['Brand_Ounass'].fillna(df1['Brand_LevelShoes']).fillna(df1['Brand_Cleaned']).fillna("Unknown");
                            if 'Display_Brand' not in df2.columns: df2['Display_Brand'] = df2['Brand_Ounass'].fillna(df2['Brand_LevelShoes']).fillna(df2['Brand_Cleaned']).fillna("Unknown");
                            df_time = pd.merge(df1[['Display_Brand','Ounass_Count','LevelShoes_Count']], df2[['Display_Brand','Ounass_Count','LevelShoes_Count']], on='Display_Brand', how='outer', suffixes=('_T1','_T2')); df_time.fillna(0, inplace=True);
                            df_time['Ounass_Change'] = (df_time['Ounass_Count_T2']-df_time['Ounass_Count_T1']).astype(int); df_time['LevelShoes_Change'] = (df_time['LevelShoes_Count_T2']-df_time['LevelShoes_Count_T1']).astype(int)
                            st.session_state.df_time_comparison = df_time; st.session_state.time_comp_meta1 = meta1; st.session_state.time_comp_meta2 = meta2; st.query_params.clear(); st.rerun()
                        else: st.error("Failed to load data."); st.session_state.df_time_comparison = pd.DataFrame()
                    else: st.warning("Please select two different snapshots."); st.session_state.df_time_comparison = pd.DataFrame()
            st.markdown("---"); st.caption("View/Delete individual snapshots:")
            for comp_meta in comps_list:
                 comp_id = comp_meta['id']; comp_ts_str = comp_meta['timestamp'];
                 try: display_ts = datetime.fromisoformat(comp_ts_str).strftime('%Y-%m-%d %H:%M')
                 except: display_ts = comp_ts_str
                 display_label = f"{display_ts} (ID: {comp_id})"; is_selected = str(comp_id) == viewing_saved_id; t_col1, t_col2 = st.columns([0.85, 0.15])
                 with t_col1:
                    button_type = "primary" if is_selected else "secondary";
                    if st.button(display_label, key=f"view_detail_{comp_id}", type=button_type, use_container_width=True): st.query_params["view_id"] = str(comp_id); st.session_state.confirm_delete_id = None; st.session_state.df_time_comparison = pd.DataFrame(); st.rerun()
                 with t_col2:
                    if st.button("🗑️", key=f"del_detail_{comp_id}", help=f"Delete snapshot from {display_ts}", use_container_width=True): st.session_state.confirm_delete_id = comp_id; st.query_params.clear(); st.rerun()

# --- Unified Display Function ---
def display_all_results(df_ounass, df_levelshoes, df_comparison_sorted, stats_title_prefix="Overall Statistics", is_saved_view=False, saved_meta=None):
    st.markdown("---")
    stats_title = stats_title_prefix
    detected_gender, detected_category = None, None

    if is_saved_view and saved_meta:
         oun_g, oun_c = extract_info_from_url(saved_meta.get('ounass_url', '')); ls_g, ls_c = extract_info_from_url(saved_meta.get('levelshoes_url', ''))
         if oun_g or ls_g: detected_gender = oun_g or ls_g
         if oun_c or ls_c: detected_category = oun_c or ls_c
         st.subheader(f"Viewing Saved Comparison ({saved_meta['timestamp']})")
         st.caption(f"Ounass URL: `{saved_meta['ounass_url']}`")
         st.caption(f"Level Shoes URL: `{saved_meta['levelshoes_url']}`")
         st.markdown("---")
    else:
        url_for_stats = st.session_state.get('processed_ounass_url') or st.session_state.get('ounass_url_input')
        if not url_for_stats: url_for_stats = st.session_state.get('levelshoes_url_input')
        if url_for_stats:
            # !!! DÜZELTİLMİŞ Atama !!!
            g_live, c_live = extract_info_from_url(url_for_stats)
            if g_live is not None: detected_gender = g_live
            if c_live is not None: detected_category = c_live

    if detected_gender and detected_category: stats_title = f"{stats_title_prefix} - {detected_gender} / {detected_category}"
    elif detected_gender: stats_title = f"{stats_title_prefix} - {detected_gender}"
    elif detected_category: stats_title = f"{stats_title_prefix} - {detected_category}"

    # Save Button Area
    if not is_saved_view and not df_comparison_sorted.empty:
        stat_title_col, stat_save_col = st.columns([0.8, 0.2])
        with stat_title_col: st.subheader(stats_title)
        with stat_save_col:
            st.write("")
            # !!! REMOVED Name Input Popover, Save directly !!!
            if st.button("💾 Save", key="save_live_comp_confirm", help="Save current comparison results", use_container_width=True):
                oun_url = st.session_state.get('processed_ounass_url', ''); ls_url = st.session_state.get('levelshoes_url_input', ''); df_save = st.session_state.df_comparison_sorted
                # Save without asking for name
                if save_comparison(oun_url, ls_url, df_save):
                    st.success(f"Comparison saved! (Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
                    st.session_state.confirm_delete_id = None; st.rerun()
    else: st.subheader(stats_title)

    # Calculate stats
    total_ounass_brands = len(df_ounass) if df_ounass is not None and not df_ounass.empty else len(df_comparison_sorted[df_comparison_sorted['Ounass_Count'] > 0]) if not df_comparison_sorted.empty else 0
    total_levelshoes_brands = len(df_levelshoes) if df_levelshoes is not None and not df_levelshoes.empty else len(df_comparison_sorted[df_comparison_sorted['LevelShoes_Count'] > 0]) if not df_comparison_sorted.empty else 0
    total_ounass_products = df_ounass['Count'].sum() if df_ounass is not None and not df_ounass.empty else df_comparison_sorted['Ounass_Count'].sum() if not df_comparison_sorted.empty else 0
    total_levelshoes_products = df_levelshoes['Count'].sum() if df_levelshoes is not None and not df_levelshoes.empty else df_comparison_sorted['LevelShoes_Count'].sum() if not df_comparison_sorted.empty else 0
    common_brands_count, ounass_only_count, levelshoes_only_count = 0, 0, 0
    if not df_comparison_sorted.empty:
        common_brands_count = len(df_comparison_sorted[(df_comparison_sorted['Ounass_Count'] > 0) & (df_comparison_sorted['LevelShoes_Count'] > 0)])
        ounass_only_count = len(df_comparison_sorted[(df_comparison_sorted['Ounass_Count'] > 0) & (df_comparison_sorted['LevelShoes_Count'] == 0)])
        levelshoes_only_count = len(df_comparison_sorted[(df_comparison_sorted['Ounass_Count'] == 0) & (df_comparison_sorted['LevelShoes_Count'] > 0)])

    # Display Stats
    stat_col1, stat_col2, stat_col3 = st.columns(3)
    with stat_col1: st.metric("Ounass Brands", f"{total_ounass_brands:,}"); st.metric("Ounass Products", f"{total_ounass_products:,}")
    with stat_col2: st.metric("Level Shoes Brands", f"{total_levelshoes_brands:,}"); st.metric("Level Shoes Products", f"{total_levelshoes_products:,}")
    with stat_col3:
        if common_brands_count or ounass_only_count or levelshoes_only_count: st.metric("Common Brands", f"{common_brands_count:,}"); st.metric("Ounass Only", f"{ounass_only_count:,}"); st.metric("Level Shoes Only", f"{levelshoes_only_count:,}")
        else: st.caption("Comparison stats unavailable.")
    st.write(""); st.markdown("---")

    # Individual Results Display (Only in Live View)
    if not is_saved_view:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Ounass Results");
            if df_ounass is not None and not df_ounass.empty: st.write(f"Brands: {len(df_ounass)}"); df_display = df_ounass.sort_values(by='Count', ascending=False).reset_index(drop=True); df_display.index += 1; st.dataframe(df_display[['Brand', 'Count']], height=400); csv_buffer = io.StringIO(); df_display[['Brand', 'Count']].to_csv(csv_buffer, index=False, encoding='utf-8'); csv_buffer.seek(0); st.download_button("Download Ounass List (CSV)", csv_buffer.getvalue(), 'ounass_brands.csv', 'text/csv', key='ounass_dl_disp')
            elif process_button and st.session_state.ounass_url_input: st.warning("No data from Ounass.")
            elif not process_button: st.info("Enter Ounass URL.")
        with col2:
            st.subheader("Level Shoes Results");
            if df_levelshoes is not None and not df_levelshoes.empty: st.write(f"Brands: {len(df_levelshoes)}"); df_display = df_levelshoes.sort_values(by='Count', ascending=False).reset_index(drop=True); df_display.index += 1; st.dataframe(df_display[['Brand', 'Count']], height=400); csv_buffer = io.StringIO(); df_display[['Brand', 'Count']].to_csv(csv_buffer, index=False, encoding='utf-8'); csv_buffer.seek(0); st.download_button("Download Level Shoes List (CSV)", csv_buffer.getvalue(), 'levelshoes_brands.csv', 'text/csv', key='ls_dl_disp')
            elif process_button and st.session_state.levelshoes_url_input: st.warning("No data from Level Shoes.")
            elif not process_button: st.info("Enter Level Shoes URL.")

    # Comparison Section
    if not df_comparison_sorted.empty:
        if not is_saved_view: st.markdown("---")
        st.subheader("Ounass vs Level Shoes Brand Comparison")
        df_display = df_comparison_sorted.copy(); df_display.index += 1
        st.dataframe(df_display[['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference']], height=500)
        st.markdown("---"); st.subheader("Visual Comparison")
        viz_col1, viz_col2 = st.columns(2)
        with viz_col1:
            st.write("**Brand Overlap**"); pie_data = pd.DataFrame({'Category': ['Common Brands', 'Ounass Only', 'Level Shoes Only'],'Count': [common_brands_count, ounass_only_count, levelshoes_only_count]}); pie_data = pie_data[pie_data['Count'] > 0]
            if not pie_data.empty: fig_pie = px.pie(pie_data, names='Category', values='Count', title="Brand Presence", color_discrete_sequence=px.colors.qualitative.Pastel); fig_pie.update_traces(textposition='inside', textinfo='percent+label+value'); st.plotly_chart(fig_pie, use_container_width=True)
            else: st.info("No data for overlap chart.")
        with viz_col2:
            st.write("**Top 10 Largest Differences**"); top_pos = df_comparison_sorted[df_comparison_sorted['Difference'] > 0].nlargest(5, 'Difference'); top_neg = df_comparison_sorted[df_comparison_sorted['Difference'] < 0].nsmallest(5, 'Difference'); top_diff = pd.concat([top_pos, top_neg]).sort_values('Difference', ascending=False)
            if not top_diff.empty: fig_diff = px.bar(top_diff, x='Display_Brand', y='Difference', title="Largest Differences (Ounass - Level Shoes)", labels={'Display_Brand': 'Brand'}, color='Difference', color_continuous_scale=px.colors.diverging.RdBu); fig_diff.update_layout(xaxis_title=None); st.plotly_chart(fig_diff, use_container_width=True)
            else: st.info("No data for difference chart.")
        st.markdown("---"); st.subheader("Top 15 Brands Comparison (Total Products)")
        if not df_comparison_sorted.empty:
            df_comp_copy = df_comparison_sorted.copy(); df_comp_copy['Total_Count'] = df_comp_copy['Ounass_Count'] + df_comp_copy['LevelShoes_Count']; top_n = 15; top_brands = df_comp_copy.nlargest(top_n, 'Total_Count');
            if not top_brands.empty:
                melted = top_brands.melt(id_vars='Display_Brand', value_vars=['Ounass_Count', 'LevelShoes_Count'], var_name='Website', value_name='Product Count'); melted['Website'] = melted['Website'].str.replace('_Count', '').str.replace('LevelShoes','Level Shoes');
                fig_top = px.bar(melted, x='Display_Brand', y='Product Count', color='Website', barmode='group', title=f"Top {top_n} Brands", labels={'Display_Brand': 'Brand'}, category_orders={"Display_Brand": top_brands['Display_Brand'].tolist()}); fig_top.update_layout(xaxis_title=None); st.plotly_chart(fig_top, use_container_width=True)
            else: st.info(f"Not enough data for Top {top_n} chart.")
        else: st.info(f"Not enough data for Top {top_n} chart.")
        st.markdown("---"); col_comp1, col_comp2 = st.columns(2)
        with col_comp1:
            st.subheader("Brands in Ounass Only"); df_f = df_comparison_sorted[lambda x: (x['LevelShoes_Count'] == 0) & (x['Ounass_Count'] > 0)]
            if not df_f.empty: df_d = df_f.reset_index(drop=True); df_d.index += 1; st.dataframe(df_d[['Display_Brand', 'Ounass_Count']], height=400)
            else: st.info("No unique Ounass brands found.")
        with col_comp2:
            st.subheader("Brands in Level Shoes Only"); df_f = df_comparison_sorted[lambda x: (x['Ounass_Count'] == 0) & (x['LevelShoes_Count'] > 0)]
            if not df_f.empty: df_d = df_f.reset_index(drop=True); df_d.index += 1; st.dataframe(df_d[['Display_Brand', 'LevelShoes_Count']], height=400)
            else: st.info("No unique Level Shoes brands found.")
        st.markdown("---"); col_comp3, col_comp4 = st.columns(2)
        with col_comp3:
            st.subheader("Common Brands: Ounass > Level Shoes"); df_f = df_comparison_sorted[lambda x: (x['Ounass_Count'] > 0) & (x['LevelShoes_Count'] > 0) & (x['Difference'] > 0)].sort_values('Difference', ascending=False)
            if not df_f.empty: df_d = df_f.reset_index(drop=True); df_d.index += 1; st.dataframe(df_d[['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference']], height=400)
            else: st.info("No common brands where Ounass > Level Shoes.")
        with col_comp4:
            st.subheader("Common Brands: Level Shoes > Ounass"); df_f = df_comparison_sorted[lambda x: (x['Ounass_Count'] > 0) & (x['LevelShoes_Count'] > 0) & (x['Difference'] < 0)].sort_values('Difference', ascending=True)
            if not df_f.empty: df_d = df_f.reset_index(drop=True); df_d.index += 1; st.dataframe(df_d[['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference']], height=400)
            else: st.info("No common brands where Level Shoes > Ounass.")
        st.markdown("---")
        csv_buffer_comparison = io.StringIO(); df_comparison_sorted[['Display_Brand', 'Ounass_Count', 'LevelShoes_Count', 'Difference']].to_csv(csv_buffer_comparison, index=False, encoding='utf-8'); csv_buffer_comparison.seek(0)
        download_label = f"Download {'Saved' if is_saved_view else 'Current'} Comparison (CSV)"; download_key = f"comp_dl_button_{'saved' if is_saved_view else 'live'}_{view_id or 'current'}"; saved_name_part = saved_meta.get('name','').replace(' ','_').replace('/','-') if is_saved_view and saved_meta else 'live'; download_filename = f"brand_comparison_{saved_name_part}.csv"
        st.download_button(download_label, csv_buffer_comparison.getvalue(), download_filename, 'text/csv', key=download_key)

    elif process_button and not is_saved_view:
        st.markdown("---"); st.warning("Comparison could not be generated. Check if data was successfully extracted from both URLs.")
    elif not process_button and not is_saved_view and st.session_state.get('df_ounass', pd.DataFrame()).empty and st.session_state.get('df_levelshoes', pd.DataFrame()).empty :
         st.info("Enter URLs in the sidebar and click 'Process URLs' or select a saved comparison.")

# --- Time Comparison Display Function ---
def display_time_comparison_results(df_time_comp, meta1, meta2):
    st.markdown("---"); st.subheader("Snapshot Comparison Over Time")
    ts_format = '%Y-%m-%d %H:%M'; ts1_str = datetime.fromisoformat(meta1['timestamp']).strftime(ts_format); ts2_str = datetime.fromisoformat(meta2['timestamp']).strftime(ts_format)
    st.markdown(f"Comparing **Snapshot 1** (`{ts1_str}`) **vs** **Snapshot 2** (`{ts2_str}`)")
    with st.expander("Show URLs for Compared Snapshots"): st.caption(f"**Snap 1:** O: `{meta1['ounass_url']}` | LS: `{meta1['levelshoes_url']}`"); st.caption(f"**Snap 2:** O: `{meta2['ounass_url']}` | LS: `{meta2['levelshoes_url']}`")
    st.markdown("---")
    new_o=df_time_comp[(df_time_comp['Ounass_Count_T1']==0)&(df_time_comp['Ounass_Count_T2']>0)]; drop_o=df_time_comp[(df_time_comp['Ounass_Count_T1']>0)&(df_time_comp['Ounass_Count_T2']==0)]; inc_o=df_time_comp[df_time_comp['Ounass_Change']>0]; dec_o=df_time_comp[df_time_comp['Ounass_Change']<0]
    new_l=df_time_comp[(df_time_comp['LevelShoes_Count_T1']==0)&(df_time_comp['LevelShoes_Count_T2']>0)]; drop_l=df_time_comp[(df_time_comp['LevelShoes_Count_T1']>0)&(df_time_comp['LevelShoes_Count_T2']==0)]; inc_l=df_time_comp[df_time_comp['LevelShoes_Change']>0]; dec_l=df_time_comp[df_time_comp['LevelShoes_Change']<0]
    st.subheader("Summary of Changes"); t_stat_col1, t_stat_col2 = st.columns(2)
    with t_stat_col1: st.metric("New Brands (Ounass)",len(new_o)); st.metric("Dropped Brands (Ounass)",len(drop_o)); st.metric("Net Product Change (Ounass)",f"{df_time_comp['Ounass_Change'].sum():+,}")
    with t_stat_col2: st.metric("New Brands (Level Shoes)",len(new_l)); st.metric("Dropped Brands (Level Shoes)",len(drop_l)); st.metric("Net Product Change (Level Shoes)",f"{df_time_comp['LevelShoes_Change'].sum():+,}")
    st.markdown("---"); st.subheader("Detailed Brand Changes"); tc_col1, tc_col2 = st.columns(2); height=250
    with tc_col1:
        st.write("**Ounass Changes**")
        if not new_o.empty: st.write(f"New ({len(new_o)}):"); df=new_o[['Display_Brand','Ounass_Count_T2']].rename(columns={'Ounass_Count_T2':'Now'}).sort_values('Now',ascending=False).reset_index(drop=True); df.index+=1; st.dataframe(df,height=height)
        if not drop_o.empty: st.write(f"Dropped ({len(drop_o)}):"); df=drop_o[['Display_Brand','Ounass_Count_T1']].rename(columns={'Ounass_Count_T1':'Was'}).sort_values('Was',ascending=False).reset_index(drop=True); df.index+=1; st.dataframe(df,height=height)
        if not inc_o.empty: st.write(f"Increased ({len(inc_o)}):"); df=inc_o[['Display_Brand','Ounass_Count_T1','Ounass_Count_T2','Ounass_Change']].rename(columns={'Ounass_Count_T1':'Was','Ounass_Count_T2':'Now','Ounass_Change':'Chg'}).sort_values('Chg',ascending=False).reset_index(drop=True); df.index+=1; st.dataframe(df,height=height)
        if not dec_o.empty: st.write(f"Decreased ({len(dec_o)}):"); df=dec_o[['Display_Brand','Ounass_Count_T1','Ounass_Count_T2','Ounass_Change']].rename(columns={'Ounass_Count_T1':'Was','Ounass_Count_T2':'Now','Ounass_Change':'Chg'}).sort_values('Chg',ascending=True).reset_index(drop=True); df.index+=1; st.dataframe(df,height=height)
        if new_o.empty and drop_o.empty and inc_o.empty and dec_o.empty: st.info("No significant changes for Ounass.")
    with tc_col2:
        st.write("**Level Shoes Changes**")
        if not new_l.empty: st.write(f"New ({len(new_l)}):"); df=new_l[['Display_Brand','LevelShoes_Count_T2']].rename(columns={'LevelShoes_Count_T2':'Now'}).sort_values('Now',ascending=False).reset_index(drop=True); df.index+=1; st.dataframe(df,height=height)
        if not drop_l.empty: st.write(f"Dropped ({len(drop_l)}):"); df=drop_l[['Display_Brand','LevelShoes_Count_T1']].rename(columns={'LevelShoes_Count_T1':'Was'}).sort_values('Was',ascending=False).reset_index(drop=True); df.index+=1; st.dataframe(df,height=height)
        if not inc_l.empty: st.write(f"Increased ({len(inc_l)}):"); df=inc_l[['Display_Brand','LevelShoes_Count_T1','LevelShoes_Count_T2','LevelShoes_Change']].rename(columns={'LevelShoes_Count_T1':'Was','LevelShoes_Count_T2':'Now','LevelShoes_Change':'Chg'}).sort_values('Chg',ascending=False).reset_index(drop=True); df.index+=1; st.dataframe(df,height=height)
        if not dec_l.empty: st.write(f"Decreased ({len(dec_l)}):"); df=dec_l[['Display_Brand','LevelShoes_Count_T1','LevelShoes_Count_T2','LevelShoes_Change']].rename(columns={'LevelShoes_Count_T1':'Was','LevelShoes_Count_T2':'Now','LevelShoes_Change':'Chg'}).sort_values('Chg',ascending=True).reset_index(drop=True); df.index+=1; st.dataframe(df,height=height)
        if new_l.empty and drop_l.empty and inc_l.empty and dec_l.empty: st.info("No significant changes for Level Shoes.")
    st.markdown("---"); csv_buffer = io.StringIO(); cols_dl = ['Display_Brand','Ounass_Count_T1','Ounass_Count_T2','Ounass_Change','LevelShoes_Count_T1','LevelShoes_Count_T2','LevelShoes_Change']
    df_time_comp[cols_dl].to_csv(csv_buffer, index=False, encoding='utf-8'); csv_buffer.seek(0)
    st.download_button(label=f"Download Time Comparison ({ts1_str} vs {ts2_str})",data=csv_buffer.getvalue(),file_name=f"time_comp_{st.session_state.time_comp_id1}_vs_{st.session_state.time_comp_id2}.csv",mime='text/csv',key='time_comp_dl_button')


# --- Main Application Flow ---
confirm_id = st.session_state.get('confirm_delete_id')
if confirm_id:
    # (Delete confirmation logic remains the same)
    st.warning(f"Are you sure you want to delete comparison ID {confirm_id}?")
    col_confirm, col_cancel, _ = st.columns([1,1,3])
    with col_confirm:
        if st.button("Yes, Delete", type="primary", key=f"confirm_delete_{confirm_id}"):
            if delete_comparison(confirm_id): st.success(f"Comparison ID {confirm_id} deleted."); st.session_state.confirm_delete_id = None; st.query_params.clear(); st.rerun()
            else: st.error("Deletion failed."); st.session_state.confirm_delete_id = None
    with col_cancel:
        if st.button("Cancel", key=f"cancel_delete_{confirm_id}"): st.session_state.confirm_delete_id = None; st.rerun()
elif 'df_time_comparison' in st.session_state and not st.session_state.df_time_comparison.empty:
    display_time_comparison_results(st.session_state.df_time_comparison, st.session_state.get('time_comp_meta1',{}), st.session_state.get('time_comp_meta2',{}))
elif viewing_saved_id:
    saved_meta, saved_df = load_specific_comparison(viewing_saved_id)
    if saved_meta and saved_df is not None: display_all_results(None, None, saved_df, stats_title_prefix="Saved Comparison", is_saved_view=True, saved_meta=saved_meta)
    else: st.error(f"Could not load comparison ID: {viewing_saved_id}.");
    if st.button("Clear Invalid Saved View"): st.query_params.clear(); st.rerun()
else:
    if process_button:
        # (Processing logic remains the same...)
        st.session_state.df_ounass = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned']); st.session_state.df_levelshoes = pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned']); st.session_state.ounass_data = []; st.session_state.levelshoes_data = []; st.session_state.df_comparison_sorted = pd.DataFrame(); st.session_state.processed_ounass_url = ''
        if st.session_state.ounass_url_input:
            with st.spinner("Processing Ounass URL..."):
                st.session_state.processed_ounass_url = ensure_ounass_full_list_parameter(st.session_state.ounass_url_input)
                ounass_html_content = fetch_html_content(st.session_state.processed_ounass_url)
                if ounass_html_content:
                    st.session_state.ounass_data = process_ounass_html(ounass_html_content)
                    if st.session_state.ounass_data: st.session_state.df_ounass = pd.DataFrame(st.session_state.ounass_data);
                    if not st.session_state.df_ounass.empty: st.session_state.df_ounass['Brand_Cleaned'] = st.session_state.df_ounass['Brand'].apply(clean_brand_name)
        if st.session_state.levelshoes_url_input:
             with st.spinner("Processing Level Shoes URL..."):
                levelshoes_html_content = fetch_html_content(st.session_state.levelshoes_url_input)
                if levelshoes_html_content:
                    st.session_state.levelshoes_data = process_levelshoes_html(levelshoes_html_content)
                    if st.session_state.levelshoes_data: st.session_state.df_levelshoes = pd.DataFrame(st.session_state.levelshoes_data);
                    if not st.session_state.df_levelshoes.empty: st.session_state.df_levelshoes['Brand_Cleaned'] = st.session_state.df_levelshoes['Brand'].apply(clean_brand_name)
        if not st.session_state.df_ounass.empty and not st.session_state.df_levelshoes.empty:
            df_o=st.session_state.df_ounass[['Brand','Count','Brand_Cleaned']].copy(); df_l=st.session_state.df_levelshoes[['Brand','Count','Brand_Cleaned']].copy(); df_comp=pd.merge(df_o,df_l,on='Brand_Cleaned',how='outer',suffixes=('_Ounass','_LevelShoes'));
            df_comp['Ounass_Count']=df_comp['Count_Ounass'].fillna(0).astype(int); df_comp['LevelShoes_Count']=df_comp['Count_LevelShoes'].fillna(0).astype(int); df_comp['Difference']=df_comp['Ounass_Count']-df_comp['LevelShoes_Count'];
            df_comp['Display_Brand']=np.where(df_comp['Ounass_Count']>0, df_comp['Brand_Ounass'], df_comp['Brand_LevelShoes']); df_comp['Display_Brand'].fillna(df_comp['Brand_Cleaned'], inplace=True);
            final_cols=['Display_Brand','Brand_Cleaned','Ounass_Count','LevelShoes_Count','Difference','Brand_Ounass','Brand_LevelShoes']
            for col in final_cols:
                if col not in df_comp.columns: df_comp[col]=np.nan
            st.session_state.df_comparison_sorted = df_comp[final_cols].sort_values(by=['Ounass_Count','LevelShoes_Count'], ascending=[False, False]).reset_index(drop=True)
        else: st.session_state.df_comparison_sorted = pd.DataFrame()
        st.session_state.df_time_comparison = pd.DataFrame(); st.session_state.time_comp_id1 = None; st.session_state.time_comp_id2 = None; st.session_state.selected_url_key_for_time_comp = None;

    df_ounass_live = st.session_state.get('df_ounass', pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned']))
    df_levelshoes_live = st.session_state.get('df_levelshoes', pd.DataFrame(columns=['Brand', 'Count', 'Brand_Cleaned']))
    df_comparison_sorted_live = st.session_state.get('df_comparison_sorted', pd.DataFrame())

    # Call display function for live data
    display_all_results(df_ounass_live, df_levelshoes_live, df_comparison_sorted_live)

    # Show initial message if needed
    if not process_button and df_ounass_live.empty and df_levelshoes_live.empty:
        st.info("Enter URLs in the sidebar and click 'Process URLs' or select a saved comparison.")