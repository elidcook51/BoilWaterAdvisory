import pandas as pd
import numpy as np
from rapidfuzz import fuzz
from dateutil import parser
from datetime import timedelta
from sklearn.preprocessing import MinMaxScaler

ground_truth_path = "C:/Users/ucg8nb/Downloads/Boil_Water_Advisories_20260426.csv"
my_guess_path = "C:/Users/ucg8nb/Downloads/Canada First Run.csv"

gt = pd.read_csv(ground_truth_path)
md = pd.read_csv(my_guess_path)

#-------------Cleaning Helpers------------

def safe_date(val):
    try:
        return parser.parse(str(val), fuzzy = True)
    except Exception:
        return pd.NaT
    
def norm_text(val):
    if pd.isna(val):
        return ""
    return str(val).lower().strip()

gt['start_date'] = gt['Date Advisory Issued'].apply(safe_date)

gt['end_date'] = gt['Date Advisory Removed'].apply(safe_date)

md['start_date'] = md.get('start_date', pd.NaT).apply(safe_date)
md['end_date'] = md.get('end_date', pd.NaT).apply(safe_date)
md['backup_date'] = md.get('backup_date', pd.NaT).apply(safe_date)

gt['name'] = gt.get('Site Name', "").apply(norm_text)
gt['county'] = gt.get("County", "").apply(norm_text)

md['name'] = md.get('location_locality', "").apply(norm_text)
md['county'] = md.get('location_county', "").apply(norm_text)
#----------------------End Normalization-----------------------


#--------------------Computing Similarity----------------------

def date_similarity(d1, d2, max_days = 5):
    if pd.isna(d1) or pd.isna(d2):
        return 0
    delta = abs((d1 - d2).days)
    return max(0, 1 - min(delta, max_days) / max_days)

def text_similarity(a, b):
    if not a or not b:
        return 0
    return fuzz.token_set_ratio(a, b) / 100


def combined_score(gt_row, md_row, t = 1):
    gt_start = gt_row['start_date']
    gt_end = gt_row['end_date']

    md_start = md_row['start_date']
    md_end = md_row['end_date']
    md_backup = md_row['backup_date']

    start_diff = abs((gt_start - md_start).days)
    end_diff = abs((gt_end - md_end).days)

    backup_in = (md_backup > gt_start) & (md_backup < gt_end)

    if start_diff <= t and end_diff <= t:
        return 1
    
    if start_diff <= t and backup_in:
        return 0.9
    
    if end_diff <= t and backup_in:
        return 0.9

    if start_diff <= t:
        return 0.7
    
    if end_diff <= t:
        return 0.7
    
    if backup_in:
        return 0.5

    return 0

matches = []

for gt_idx, gt_row in gt.iterrows():
    best_match = None
    best_score = 0

    for md_idx, md_row in md.iterrows():
        score = combined_score(gt_row, md_row)
        if score > best_score:
            best_score = score
            best_match = md_idx

    matches.append({
        'gt_index': gt_idx,
        'md_index': best_match,
        "confidence": best_score,
    })

matches_df = pd.DataFrame(matches)

def evaluate(threshold):
    detected = matches_df[matches_df['confidence'] >= threshold]
    recall = len(detected) / len(matches_df)

    matched_md = set(detected['md_index'])
    unmatched_md = set(md.index) - matched_md

    return {
        'threshold': threshold,
        'ground_truth_total': len(gt),
        'ground_truth_matched': len(detected),
        'recall': recall,
        'extra_in_my_data': len(unmatched_md)
    }

results = pd.DataFrame([
    evaluate(t) for t in [0.49, 0.69, 0.89, 0.99]
])

print(results)
results.to_csv("C:/Users/ucg8nb/Downloads/First Results.csv")