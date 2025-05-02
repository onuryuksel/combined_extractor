"""Utility helpers shared by extractor modules and Streamlit app (v2)."""
import pandas as pd
import re
try:
    from thefuzz import process
    _HAS_FUZZ = True
except ModuleNotFoundError:
    _HAS_FUZZ = False

def _clean(name: str) -> str:
    """Remove non‑alphanumerics and uppercase – used as merge key."""
    return re.sub(r'[^A-Za-z0-9]+', '', str(name)).upper()

def merge_brand_frames(df_left: pd.DataFrame,
                       df_right: pd.DataFrame,
                       left_label: str = 'Ounass',
                       right_label: str = 'Competitor') -> pd.DataFrame:
    """
    Merge two ‹Designer, Count› dataframes and output:
      Designer | Count_<left_label> | Count_<right_label> | Delta
    - Uses a simple normalised key for first pass.
    - Optionally performs fuzzy matching **only** for rows still unmatched
      (requires `thefuzz`; if missing, skips fuzzy step).
    """
    # ── Normalise keys ──────────────────────────────────────────────────────────
    df_l = df_left.copy()
    df_r = df_right.copy()
    df_l['key'] = df_l['Designer'].map(_clean)
    df_r['key'] = df_r['Designer'].map(_clean)

    merged = pd.merge(df_l[['key', 'Designer', 'Count']],
                      df_r[['key', 'Designer', 'Count']],
                      on='key',
                      how='outer',
                      suffixes=(f'_{left_label}', f'_{right_label}'))

    # ── Optional fuzzy for leftover NaNs ────────────────────────────────────────
    if _HAS_FUZZ:
        left_missing = merged['Designer_'+left_label].isna()
        right_missing = merged['Designer_'+right_label].isna()

        if left_missing.any():
            choices = df_l['Designer'].tolist()
            for idx in merged[left_missing].index:
                cand, score = process.extractOne(merged.at[idx, 'Designer_'+right_label], choices)
                if score >= 90:
                    row_match = df_l.loc[df_l['Designer'] == cand].iloc[0]
                    merged.at[idx, 'Designer_'+left_label] = row_match['Designer']
                    merged.at[idx, 'Count_'+left_label] = row_match['Count']

        if right_missing.any():
            choices = df_r['Designer'].tolist()
            for idx in merged[right_missing].index:
                cand, score = process.extractOne(merged.at[idx, 'Designer_'+left_label], choices)
                if score >= 90:
                    row_match = df_r.loc[df_r['Designer'] == cand].iloc[0]
                    merged.at[idx, 'Designer_'+right_label] = row_match['Designer']
                    merged.at[idx, 'Count_'+right_label] = row_match['Count']

    # ── Final tidy‑up ───────────────────────────────────────────────────────────
    merged['Designer'] = merged['Designer_'+left_label].combine_first(merged['Designer_'+right_label])
    merged[f'Count_{left_label}'] = merged['Count_'+left_label].fillna(0).astype(int)
    merged[f'Count_{right_label}'] = merged['Count_'+right_label].fillna(0).astype(int)

    result = merged[['Designer', f'Count_{left_label}', f'Count_{right_label}']]
    result['Delta'] = result[f'Count_{left_label}'] - result[f'Count_{right_label}']
    return result.sort_values('Delta', ascending=False).reset_index(drop=True)
