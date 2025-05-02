# sephora_extractor.py

import streamlit as st
import pandas as pd
import re
import io
import unicodedata # Keep for potential future use, though fix is mainly string methods now

def _process_sephora_html_internal(html_content):
    """Internal logic to parse Sephora HTML using regex for JSON fragments."""
    data_extracted = []
    if not html_content:
        print("Error (Sephora Extractor): Received empty HTML content.")
        return data_extracted

    try:
        # Original pattern seems effective
        pattern = r'\\"hitCount\\":\s*(\d+),\\"label\\":\\"([^"\\]+)\\"'
        matches = re.findall(pattern, html_content)

        # --- Stricter Heuristic ---
        def looks_like_brand(label: str) -> bool:
            """
            Stricter heuristic based on original script:
            - Contains letters.
            - No digits.
            - ALL CAPS (no lowercase letters).
            - Not common filter values.
            """
            if not label: return False
            return (
                any(c.isalpha() for c in label)
                and not re.search(r'\d', label)
                and not any(c.islower() for c in label)
                and len(label) > 1
                and "VIEW ALL" not in label.upper()
                and "SHOW M" not in label.upper()
                and "SHOW L" not in label.upper()
                and label.upper() != 'NO'
                and label.upper() != 'YES'
            )
        # --- End Stricter Heuristic ---

        # De-duplicate and handle counts (keep max)
        brand_totals: dict[str, int] = {}
        label_is_brand_map = {} # Cache heuristic results for unique labels

        for count_str, label_raw in matches:
            label = label_raw # Start with raw

            # 1. Try decoding unicode escapes first (handles \\uXXXX)
            try:
                 # Decode JSON string escapes (like \\u0026 -> &)
                 # Using 'unicode_escape' codec directly on the raw string works for this
                 label_uni_decoded = label_raw.encode('latin-1', errors='ignore').decode('unicode_escape', errors='ignore') # intermediate encoding needed?
                 # Basic check if it improved things (removed backslashes)
                 if '\\' not in label_uni_decoded and label_uni_decoded != label_raw:
                     label = label_uni_decoded
                 else: # if no change or still has backslashes, maybe simple replace works better?
                     label = label_raw.replace('\\\\', '\\').encode().decode('unicode_escape', errors='ignore') # Test alternative

            except Exception as uni_e:
                 # print(f"Unicode escape decode failed for {label_raw}: {uni_e}") # Optional log
                 # Fallback: try simpler replacement for common escapes if direct decode fails
                 try:
                     label = label_raw.replace("\\u0026", "&").replace("\\u0027", "'") # Add more if needed
                 except Exception:
                     pass # Keep label_raw if all fails

            # 2. Try fixing potential Mojibake (UTF-8 bytes misinterpreted as Latin-1/Windows-1252)
            try:
                 # Encode using an encoding that preserves the original (misinterpreted) bytes
                 # then decode using the *intended* encoding (UTF-8)
                 fixed_label = label.encode('latin-1', errors='ignore').decode('utf-8', errors='ignore')
                 # Basic check: did it change and does it look less like Mojibake?
                 # This checks for common Mojibake artifacts starting with 'Ã'
                 if fixed_label != label and 'Ã' not in fixed_label and 'â' not in fixed_label:
                      # print(f"Mojibake fix applied: '{label}' -> '{fixed_label}'") # Optional log
                      label = fixed_label
            except Exception as moj_e:
                 # print(f"Mojibake fix error for '{label}': {moj_e}") # Optional log
                 pass # Keep current label if fix fails

            label = label.strip() # Final strip after potential fixes

            # Check heuristic using the potentially fixed label
            if label not in label_is_brand_map:
                 label_is_brand_map[label] = looks_like_brand(label)

            is_brand = label_is_brand_map[label]

            if not is_brand:
                # print(f"Sephora Skip (heuristic): {label}") # Optional debug log
                continue

            # Process count and add/update in dict
            try:
                count = int(count_str)
                current_brand_key = label # Use the potentially fixed label as the key

                if current_brand_key not in brand_totals:
                    brand_totals[current_brand_key] = count
                else:
                    brand_totals[current_brand_key] = max(brand_totals[current_brand_key], count)
            except ValueError:
                 print(f"Warning (Sephora Extractor): Could not convert count '{count_str}' to int for label '{label}'. Skipping.")
                 continue

        # Convert the dictionary to the desired list of dictionaries format
        for brand, count in brand_totals.items():
             if count > 0:
                  data_extracted.append({'Brand': brand, 'Count': count})

        if not data_extracted and len(matches) > 0:
            print("Warning (Sephora Extractor): Regex found matches, but none passed the 'looks_like_brand' heuristic.")
            # Log sample raw labels found to help debug the heuristic or Mojibake issues
            # sample_labels = [m[1].encode('latin-1', errors='ignore').decode('unicode_escape', errors='ignore') for m in matches[:20]]
            # print("Sample labels found (decoded):", sample_labels)

    except Exception as e:
        print(f"Error (Sephora Extractor): Unexpected error during processing - {e}")
        import traceback
        print(traceback.format_exc())
        return [] # Return empty list on major error

    return data_extracted

# Cache wrapper remains the same
@st.cache_data
def get_processed_sephora_data(html_content):
    """Cached function to process Sephora HTML content."""
    print("Processing Sephora HTML...") # Log processing start
    if not html_content:
        print("Warning (Sephora Extractor): get_processed_sephora_data received empty HTML.")
        return []
    processed_data = _process_sephora_html_internal(html_content)
    print(f"Sephora processing finished. Found {len(processed_data)} brands.") # Log processing end
    return processed_data
