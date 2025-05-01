# levelshoes_extractor.py

import streamlit as st
from bs4 import BeautifulSoup
import json

# Note: Keep warnings/errors inside for now, but ideally, return specific values
# and handle UI messages in the main app based on the return.
# Cache decorator moved to the wrapper function.
def _process_levelshoes_html_internal(html_content):
    """Internal logic to parse Level Shoes HTML using __NEXT_DATA__."""
    data_extracted = []
    try:
        soup = BeautifulSoup(html_content, 'html.parser'); script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
        if not script_tag:
            print("Error (LevelShoes Extractor): Page structure changed, '__NEXT_DATA__' script tag not found.")
            # st.error("Level Shoes Error: Page structure changed, '__NEXT_DATA__' script tag not found.")
            return data_extracted
        json_data_str = script_tag.string
        if not json_data_str:
            print("Error (LevelShoes Extractor): __NEXT_DATA__ script tag content is empty.")
            # st.error("Level Shoes Error: __NEXT_DATA__ script tag content is empty.")
            return data_extracted
        data = json.loads(json_data_str); apollo_state = data.get('props', {}).get('pageProps', {}).get('__APOLLO_STATE__', {})
        if not apollo_state:
            print("Error (LevelShoes Extractor): '__APOLLO_STATE__' not found within __NEXT_DATA__.")
            # st.error("Level Shoes Error: '__APOLLO_STATE__' not found within __NEXT_DATA__.")
            return data_extracted
        root_query = apollo_state.get('ROOT_QUERY', {})
        if not root_query:
            print("Error (LevelShoes Extractor): 'ROOT_QUERY' not found within __APOLLO_STATE__.")
            # st.error("Level Shoes Error: 'ROOT_QUERY' not found within __APOLLO_STATE__.")
            return data_extracted
        product_list_key = next((key for key in root_query if key.startswith('_productList')), None)
        if not product_list_key: product_list_key = next((key for key in root_query if '_productList:({' in key), None)
        if not product_list_key:
            print("Error (LevelShoes Extractor): Could not find product list data key in ROOT_QUERY.")
            # st.error("Level Shoes Error: Could not find product list data key in ROOT_QUERY.")
            return data_extracted
        product_list_data = root_query.get(product_list_key, {}); facets = product_list_data.get('facets', [])
        if not facets:
            # print("Warning (LevelShoes Extractor): No 'facets' (filters) found in product list data.")
            # st.warning("Level Shoes Warning: No 'facets' (filters) found in product list data.")
            return data_extracted # Not necessarily an error if page has no facets
        designer_facet = None
        for facet in facets:
            facet_key = facet.get('key', '').lower(); facet_label = facet.get('label', '').lower()
            if facet_key == 'brand' or facet_label == 'designer':
                 designer_facet = facet; break
        if not designer_facet:
            # available_facets = [f.get('key') or f.get('label') for f in facets]
            # print(f"Error (LevelShoes Extractor): 'brand' or 'Designer' facet not found. Available: {available_facets}")
            # st.error(f"Level Shoes Error: 'brand' or 'Designer' facet not found. Available: {available_facets}")
            return data_extracted # Not necessarily an error if page has no designer facet
        designer_options = designer_facet.get('options', [])
        if not designer_options:
            # print("Warning (LevelShoes Extractor): 'Designer' facet found, but it contains no options.")
            # st.warning("Level Shoes Warning: 'Designer' facet found, but it contains no options.")
            return data_extracted
        for option in designer_options:
             name = option.get('name'); count = option.get('count')
             if name is not None and count is not None:
                 upper_name = name.upper()
                 if "VIEW ALL" not in upper_name and "SHOW M" not in upper_name and "SHOW L" not in upper_name:
                     data_extracted.append({'Brand': name.strip(), 'Count': int(count)})
        # if not data_extracted:
        #     print("Warning (LevelShoes Extractor): Designer options processed, but no valid brand data was extracted.")
            # st.warning("Level Shoes: Designer options processed, but no valid brand data was extracted.")
        return data_extracted
    except json.JSONDecodeError:
        print("Error (LevelShoes Extractor): Failed to decode JSON data from __NEXT_DATA__.")
        # st.error("Level Shoes Error: Failed to decode JSON data from __NEXT_DATA__.")
        return []
    except (AttributeError, KeyError, TypeError, IndexError) as e:
        print(f"Error (LevelShoes Extractor): Problem navigating JSON structure - {e}.")
        # st.error(f"Level Shoes Error: Problem navigating JSON structure - {e}.")
        return []
    except Exception as e:
        print(f"Error (LevelShoes Extractor): Unexpected error during processing - {e}")
        # st.error(f"Level Shoes Error: Unexpected error during processing - {e}")
        return []

@st.cache_data
def get_processed_levelshoes_data(html_content):
    """Cached function to process Level Shoes HTML content."""
    print("Processing LevelShoes HTML...") # Log processing start
    if not html_content:
        print("Warning (LevelShoes Extractor): get_processed_levelshoes_data received empty HTML.")
        return []
    processed_data = _process_levelshoes_html_internal(html_content)
    print(f"LevelShoes processing finished. Found {len(processed_data)} brands.") # Log processing end
    return processed_data