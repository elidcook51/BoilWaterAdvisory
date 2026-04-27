import pandas as pd
from rapidfuzz import fuzz, process
from collections import defaultdict, deque
import re
from datetime import date
from calendar import month_abbr, month_name
import requests
from dotenv import load_dotenv
import os
import ast
from bs4 import BeautifulSoup
import pickle

load_dotenv()

HEADERS = ast.literal_eval(os.getenv('HEADERS'))

MONTHS = {}

for i in range(1, 13):
    MONTHS[month_name[i].lower()] = i
    MONTHS[month_abbr[i].lower()] = i

ADVISORY_WORDS = ['notice', 'advisory', 'issued', 'lifted', 'order', 'advisories', 'alert', 'urged', 'asked', 'crisis', 'disaster', 'break', 'main', 'told', 'must', 'have', 'recommends', 'warned', 'still', 'continue', 'stop', 'notification', 'advised', 'prepare', 'prepared','tell', 'advises', 'urges', 'recommended', 'asks', 'unsafe', 'need', 'warning', 'extends', 'lifts', 'lifting', 'imposes', 'declares', 'issues', 'issued', 'rescinds', 'rescinded', 'residents', 'county', 'city', 'utility', 'pressure-drop', 'water-contamination', 'water-system', 'tap-water', 'under-boil']

DATE_PATTERNS = [
    # YYYY-MM-DD, YYYY/MM/DD, YYYY.MM.DD
    re.compile(r'(?P<y>\d{4})[-/._](?P<m>\d{1,2})[-/._](?P<d>\d{1,2})'),

    # YYYYMMDD
    re.compile(r'(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})'),

    # MM-DD-YYYY
    re.compile(r'(?P<m>\d{1,2})[-/._](?P<d>\d{1,2})[-/._](?P<y>\d{4})'),

    # DD-MM-YYYY
    re.compile(r'(?P<d>\d{1,2})[-/._](?P<m>\d{1,2})[-/._](?P<y>\d{4})'),

    # MM-DD-YY ✅ (YOUR CASE)
    re.compile(r'(?P<m>\d{1,2})[-/._](?P<d>\d{1,2})[-/._](?P<y>\d{2})'),

    # DD-MM-YY
    re.compile(r'(?P<d>\d{1,2})[-/._](?P<m>\d{1,2})[-/._](?P<y>\d{2})'),

    # Month-DD-YYYY (Sep-27-2016)
    re.compile(
        r'(?P<m>[A-Za-z]+)[-/._](?P<d>\d{1,2})[-/._](?P<y>\d{4})',
        re.IGNORECASE
    ),

    # DD-Month-YYYY (27-Sep-2016)
    re.compile(
        r'(?P<d>\d{1,2})[-/._](?P<m>[A-Za-z]+)[-/._](?P<y>\d{4})',
        re.IGNORECASE
    ),
]


def build_similiar_df(wholeDf):
    links = wholeDf['link'].dropna().tolist()
    pairs = []
    for i, s1 in enumerate(links):
        for j, s2 in enumerate(links[i + 1:], i + 1):
            score = fuzz.token_sort_ratio(s1, s2)
            if score > 90:
                pairs.append((s1, s2, score))

    return pd.DataFrame(pairs, columns = ['string1', 'string2', 'similarity'])

def build_similarity_groups(similarDf):
    graph = defaultdict(set)

    for _, row in similarDf.iterrows():
        a, b = row['string1'], row['string2']
        graph[a].add(b)
        graph[b].add(a)
    
    visited = set()
    groups = []

    for node in graph:
        if node not in visited:
            stack = [node]
            component = set()

            while stack:
                cur = stack.pop()
                if cur not in visited:
                    visited.add(cur)
                    component.add(cur)
                    stack.extend(graph[cur] - visited)
            
            groups.append(component)
    return groups

def extract_date_from_string(s):
    if not s:
        return None
    
    for pattern in DATE_PATTERNS:
        match = pattern.search(s)
        if match:
            try:
                y = int(match.group('y'))
                m = int(match.group('m'))
                d = int(match.group('d'))
                return date(y, m, d)
            except Exception:
                return None
    return None

def split_groups_by_date(groups):
    final_groups = []

    for group in groups:
        buckets = {}

        for link in group:
            date = extract_date_from_string(link)
            buckets.setdefault(date, set()).add(link)

        final_groups.extend(buckets.values())
    return final_groups

def link_works(url):
    print(f"Checking Link {url}")
    try:
        r = requests.get(url, headers = HEADERS, timeout = 30)

        if r.status_code != 200:
            return False

        if 'html' not in r.headers.get('Content-Type', "").lower():
            return False
        
        soup = BeautifulSoup(r.text, 'html.parser')

        for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
            tag.decompose()

        text = soup.get_text(separator=' ', strip = True).lower()

        if len(text) < 500:
            return False
        return True
    except Exception:
        return False
    
def choose_one_link(group):
    if len(group) == 1:
        return next(iter(group))
    for link in group:
        if link_works(link):
            return link
    return next(iter(group))

def get_all_good_links(groups):
    cannoncial_links = set()

    totLen = len(groups)
    count = 0

    for group in groups:
        cannoncial_links.add(choose_one_link(group))
        count += 1
        print(f"Finished group {count}/{totLen} ({count / totLen * 100:.2f}%)")
    return cannoncial_links

def full_clean_GDELT(GDELT_path, output_path, pickle_path = 'bad_links'):
    gdeltDf = pd.read_csv(GDELT_path)
    all_links = set(gdeltDf['link'])

    print("Removing links which don't contain advisories")
    tempDf = gdeltDf.copy()

    for keyword in ADVISORY_WORDS:
        tempDf = tempDf[~tempDf['link'].str.contains(keyword, case = False)]

    bad_links = set(tempDf['link'])

    links_to_keep = all_links - bad_links

    advisory_gdelt = gdeltDf[gdeltDf['link'].isin(links_to_keep)]

    print(f"Removed {len(tempDf)} links!")

    print("Creating Similarity Df")
    similar_df = build_similiar_df(advisory_gdelt)

    similar_links = set(similar_df['string1']) | set(similar_df['string2'])

    print("Creating groups of similar links")
    groups = build_similarity_groups(similar_df)

    final_groups = split_groups_by_date(groups)

    print('Getting links for each group')
    cannoncial_links = get_all_good_links(final_groups)

    unique_links = links_to_keep - similar_links

    links_to_keep = cannoncial_links | unique_links

    bad_links = all_links - links_to_keep

    clean_GDELT = gdeltDf[gdeltDf['link'].isin(links_to_keep)]

    print("Storing clean database and bad links")
    clean_GDELT.to_csv(output_path, index = False)

    with open(pickle_path, 'wb') as f:
        pickle.dump(bad_links, f)

# canada_gdelt_path = "C:/Users/ucg8nb/Downloads/GDELT Boil Water Data Canada.csv"
# clean_path = "C:/Users/ucg8nb/Downloads/Cleaned Canada GDELT.csv"
# pickle_path = 'bad_links_canada.pkl'

# full_clean_GDELT(canada_gdelt_path, clean_path, pickle_path=pickle_path)