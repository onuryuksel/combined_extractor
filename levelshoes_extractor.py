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
        soup = BeautifulSoup(html_content, 'html.parser')
        script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

        if not script_tag:
            print("Error (LevelShoes Extractor): Page structure changed, '__NEXT_DATA__' script tag not found.")
            return data_extracted # Return empty list, can't proceed

        json_data_str = script_tag.string
        if not json_data_str:
            print("Error (LevelShoes Extractor): __NEXT_DATA__ script tag content is empty.")
            return data_extracted # Return empty list, can't proceed

        data = json.loads(json_data_str)
        # Navigate through the nested structure safely using .get()
        apollo_state = data.get('props', {}).get('pageProps', {}).get('__APOLLO_STATE__', {})
        if not apollo_state:
            print("Error (LevelShoes Extractor): '__APOLLO_STATE__' not found within __NEXT_DATA__.")
            return data_extracted

        root_query = apollo_state.get('ROOT_QUERY', {})
        if not root_query:
            print("Error (LevelShoes Extractor): 'ROOT_QUERY' not found within __APOLLO_STATE__.")
            return data_extracted

        # Find the product list key dynamically (it often contains filter parameters)
        product_list_key = next((key for key in root_query if key.startswith('_productList')), None)
        # Fallback if the prefix changes but the structure is similar
        if not product_list_key:
             product_list_key = next((key for key in root_query if '_productList:({' in key), None)

        if not product_list_key:
            print("Error (LevelShoes Extractor): Could not find product list data key (starting with _productList or containing _productList:({ ) in ROOT_QUERY.")
            # Log available keys for debugging if needed
            # print(f"Available ROOT_QUERY keys: {list(root_query.keys())}")
            return data_extracted

        product_list_data = root_query.get(product_list_key, {})
        facets = product_list_data.get('facets', []) # Get facets list

        if not facets:
            # This might not be an error if a page simply has no filters, but it's worth noting.
            print("Warning (LevelShoes Extractor): No 'facets' (filters) found in product list data.")
            return data_extracted # Return empty, as we can't find the designer facet

        # Find the 'brand' or 'Designer' facet
        designer_facet = None
        for facet in facets:
            # Check both 'key' and 'label' for flexibility, case-insensitive
            facet_key = facet.get('key', '').lower()
            facet_label = facet.get('label', '').lower()
            if facet_key == 'brand' or facet_label == 'designer':
                 designer_facet = facet
                 break # Found it, no need to check further

        if not designer_facet:
            available_facets = [(f.get('key'), f.get('label')) for f in facets]
            print(f"Error (LevelShoes Extractor): 'brand' or 'Designer' facet not found. Available facets (key, label): {available_facets}")
            return data_extracted # Return empty, can't find designers

        designer_options = designer_facet.get('options', [])
        if not designer_options:
            print("Warning (LevelShoes Extractor): 'Designer/brand' facet found, but it contains no options (brands).")
            return data_extracted # Return empty, no brands listed

        # Extract brand names and counts from the options
        for option in designer_options:
             name = option.get('name')
             count = option.get('count')
             # Ensure both name and count are present and count is convertible to int
             if name is not None and count is not None:
                 try:
                     brand_count = int(count)
                     # Clean up name and filter out common non-brand entries
                     upper_name = name.upper()
                     if "VIEW ALL" not in upper_name and "SHOW M" not in upper_name and "SHOW L" not in upper_name:
                         data_extracted.append({'Brand': name.strip(), 'Count': brand_count})
                 except (ValueError, TypeError):
                     print(f"Warning (LevelShoes Extractor): Could not convert count '{count}' for brand '{name}' to an integer. Skipping.")
                     continue # Skip this brand if count is invalid

        # Optional: Log if extraction completed but found nothing after filtering
        # if not data_extracted and designer_options:
            # print("Warning (LevelShoes Extractor): Designer options processed, but no valid brand data remained after filtering.")

        return data_extracted

    except json.JSONDecodeError:
        print("Error (LevelShoes Extractor): Failed to decode JSON data from __NEXT_DATA__. Page content might be corrupted or incomplete.")
        return [] # Return empty list on JSON error
    except (AttributeError, KeyError, TypeError, IndexError) as e:
        # Catch errors related to navigating the expected JSON structure
        print(f"Error (LevelShoes Extractor): Problem navigating the JSON structure - {e}. The website structure might have changed.")
        # Consider logging more details about the state of `data`, `apollo_state`, etc., here if needed
        return []
    except Exception as e:
        # Catch any other unexpected errors during processing
        print(f"Error (LevelShoes Extractor): Unexpected error during processing - {e}")
        # Log the full traceback here if needed for debugging
        # import traceback
        # print(traceback.format_exc())
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
