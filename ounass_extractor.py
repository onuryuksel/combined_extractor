
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
        # Try finding the header first, more specific
        designer_header = soup.find(lambda tag: tag.name == 'header' and 'Designer' in tag.get_text(strip=True) and tag.find_parent('section', class_='Facet'))
        facet_section = designer_header.find_parent('section', class_='Facet') if designer_header else None

        # Fallback: Look for any Facet section that seems to contain designer links if header isn't found
        if not facet_section:
             potential_sections = soup.find_all('section', class_='Facet')
             for section in potential_sections:
                  header = section.find('header')
                  if header and 'Designer' in header.get_text(strip=True):
                       facet_section = section
                       break
                  # Even weaker fallback: check for links that look like designer links directly
                  # This might be less reliable
                  # links = section.select('ul > li > a.FacetLink > span.FacetLink-name > span.FacetLink-count')
                  # if links:
                  #      # Check if parent link text contains common brand patterns? Too complex maybe.
                  #      pass


        if facet_section:
            # Prioritize the more specific selector first
            items = facet_section.select('ul > li > a.FacetLink')
            # If that yields nothing, try a broader search for FacetLink anchors within the section
            if not items:
                items = facet_section.find_all('a', href=True, class_=lambda x: x and 'FacetLink' in x)

            if not items:
                # Warning if no items found, might indicate structure change
                print("Warning (Ounass Extractor): Could not find brand list elements (FacetLink) within the identified Designer Facet section.")
            else:
                for item in items:
                    try:
                        # Find the name span within the link
                        name_span = item.find('span', class_='FacetLink-name')
                        if name_span:
                            # Find the count span nested within the name span
                            count_span = name_span.find('span', class_='FacetLink-count')
                            count_text = count_span.text.strip() if count_span else "(0)" # Default count if span not found

                            # --- Extract Name ---
                            # To get the name without the count, clone the name_span and remove the count_span from the clone
                            temp_name_span = BeautifulSoup(str(name_span), 'html.parser').find(class_='FacetLink-name')
                            temp_count_span = temp_name_span.find(class_='FacetLink-count')
                            if temp_count_span:
                                temp_count_span.decompose() # Remove the count part
                            designer_name = temp_name_span.text.strip() # Get remaining text

                            # --- Extract Count ---
                            match = re.search(r'\((\d+)\)', count_text) # Regex to find digits in parentheses
                            count = int(match.group(1)) if match else 0

                            # Add to data if name is valid and not a filter option
                            if designer_name and "SHOW" not in designer_name.upper():
                                data.append({'Brand': designer_name, 'Count': count})
                        # else: Link doesn't contain the expected name span structure
                    except Exception as item_e:
                        # Log individual item errors but continue processing others
                        print(f"Warning (Ounass Extractor): Error processing individual item link: {item_e} - Item HTML: {str(item)[:100]}")
                        pass # Ignore individual item errors
        else:
            # This warning is important if the primary structure isn't found
            print("Warning (Ounass Extractor): Could not find the 'Designer' facet section structure (header or section itself).")

    except Exception as e:
        print(f"Error (Ounass Extractor): Major HTML parsing error: {e}") # Log error
        return [] # Return empty list on major error

    # Final check: if data is empty but HTML was provided, maybe log a higher level warning
    # if not data and html_content:
        # print("Warning (Ounass Extractor): No brand data extracted, though HTML was received and parsed without major errors. Structure might have changed significantly.")

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
