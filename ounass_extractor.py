# ounass_extractor.py

import streamlit as st
from bs4 import BeautifulSoup
import re

# Note: Keep warnings/errors inside for now, but ideally, return specific values
# and handle UI messages in the main app based on the return.
# Cache decorator moved to the wrapper function.
def _process_ounass_html_internal(html_content):
    """Internal logic to parse Ounass HTML."""
    soup = BeautifulSoup(html_content, 'html.parser'); data = []
    try:
        designer_header = soup.find(lambda tag: tag.name == 'header' and 'Designer' in tag.get_text(strip=True) and tag.find_parent('section', class_='Facet'))
        facet_section = designer_header.find_parent('section', class_='Facet') if designer_header else None
        if facet_section:
            items = facet_section.select('ul > li > a.FacetLink') or facet_section.find_all('a', href=True, class_=lambda x: x and 'FacetLink' in x)
            if not items:
                # Warning if no items found, might indicate structure change
                print("Warning (Ounass Extractor): Could not find brand list elements (FacetLink).") # Use print for logs inside cache
                # st.warning("Ounass: Could not find brand list elements (FacetLink).") # Avoid Streamlit calls inside cached data functions
            else:
                for item in items:
                    try:
                        name_span = item.find('span', class_='FacetLink-name')
                        if name_span:
                            count_span = name_span.find('span', class_='FacetLink-count'); count_text = count_span.text.strip() if count_span else "(0)"
                            # Clone to avoid modifying original soup element if reused
                            temp_name_span = BeautifulSoup(str(name_span), 'html.parser').find(class_='FacetLink-name')
                            temp_count_span = temp_name_span.find(class_='FacetLink-count')
                            if temp_count_span: temp_count_span.decompose()
                            designer_name = temp_name_span.text.strip(); match = re.search(r'\((\d+)\)', count_text); count = int(match.group(1)) if match else 0
                            if designer_name and "SHOW" not in designer_name.upper(): data.append({'Brand': designer_name, 'Count': count})
                    except Exception: pass # Ignore individual item errors
        else:
            print("Warning (Ounass Extractor): Could not find the 'Designer' facet section structure.")
            # st.warning("Ounass: Could not find the 'Designer' facet section structure.")
    except Exception as e:
        print(f"Error (Ounass Extractor): HTML parsing error: {e}") # Log error
        # st.error(f"Ounass: HTML parsing error: {e}")
        return [] # Return empty list on major error
    # if not data and html_content:
        # print("Warning (Ounass Extractor): No brand data extracted, though HTML was received.")
        # st.warning("Ounass: No brand data extracted, though HTML was received.")
    return data

@st.cache_data
def get_processed_ounass_data(html_content):
    """Cached function to process Ounass HTML content."""
    print("Processing Ounass HTML...") # Log processing start
    if not html_content:
        print("Warning (Ounass Extractor): get_processed_ounass_data received empty HTML.")
        return []
    processed_data = _process_ounass_html_internal(html_content)
    print(f"Ounass processing finished. Found {len(processed_data)} brands.") # Log processing end
    return processed_data