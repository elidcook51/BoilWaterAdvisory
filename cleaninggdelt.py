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


def buildSimilarDf(wholeDf):
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
        r = requests.get(url, headers = HEADERS)

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

fullGdeltPath = "C:/Users/ucg8nb/Downloads/GDELT news data.csv"

with open('full_bad_links.pkl', 'rb') as f:
    bad_link = pickle.load(f)

fullGDELT = pd.read_csv(fullGdeltPath)

cleanGDELT = fullGDELT[~fullGDELT['link'].isin(bad_link)]

# similarDfPath = 'C:/Users/ucg8nb/Downloads/Similar Df.csv'
cleanedGdeltPath = "C:/Users/ucg8nb/Downloads/Clean GDELT.csv"

cleanGDELT.to_csv(cleanedGdeltPath)

# cleanGDELTDf = pd.read_csv(cleanedGdeltPath)

# tempDf = cleanGDELTDf.copy()

# for keyword in ADVISORY_WORDS:
#     tempDf = tempDf[~tempDf['link'].str.contains(keyword, case = False)]

# with open('bad_links.pkl', 'rb') as f:
#     existing_set = pickle.load(f)

# existing_set = existing_set | (set(tempDf['link']))

# with open('full_bad_links.pkl', 'wb') as f:
#     pickle.dump(existing_set, f)


# print(len(tempDf))
# tempDf.to_csv("C:/Users/ucg8nb/Downloads/Bad GDELT.csv", index = False)

# gdeltDf = pd.read_csv(fullGdeltPath)
# similarDf = pd.read_csv(similarDfPath)

# similarLinks = set(similarDf['string1']) | set(similarDf['string2'])

# groups = build_similarity_groups(similarDf)

# final_groups = split_groups_by_date(groups)

# print("Created Groups!")

# cannoncial_links = get_all_good_links(final_groups)

# all_links = set(gdeltDf['link'])
# unique_links = all_links - similarLinks

# links_to_keep = cannoncial_links | unique_links

# bad_links = all_links - links_to_keep

# cleanGdelt = gdeltDf[gdeltDf['link'].isin(links_to_keep)].copy()

# cleanGdelt.to_csv('C:/Users/ucg8nb/Downloads/Clean GDELT.csv')
# with open('bad_links.pkl', 'wb') as f:
#     pickle.dump(bad_links, f)
