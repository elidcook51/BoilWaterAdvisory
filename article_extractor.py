import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import ast
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
import time
import random
from copy import deepcopy
import textwrap

load_dotenv()

testLinksPath = "C:/Users/ucg8nb/Downloads/Boil_Water_Truth_v0.csv"

testDf = pd.read_csv(testLinksPath)

HEADERS = ast.literal_eval(os.getenv('HEADERS'))

KEYWORDS = ['boil water', 'boil advisory', 'water advisory', 'precautionary boil', 'do not drink', 'issued', 'water system', 'public water supply', 'lifted', 'rescinded', 'effective', 'until further notice', 'posted', 'post', 'emergency', 'planned', 'boil-water', 'boil', 'water', 'pressure loss', 'bottled water']
WINDOW = 3

def is_valid_html(html):
    if not html or len(html) < 500:
        return False
    invalid_signals = ['enable javascript', 'access denied', 'captcha', 'checking your browser', 'cloudflare']

    lowered = html.lower()
    return not any(signal in lowered for signal in invalid_signals)

def render_html_playwright(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless = True, args = ['--disable-blink-features=AutomationControlled'])
        context = browser.new_context(
            user_agent = HEADERS['User-Agent'],
            locale = 'en-US',
            viewport = {'width':1366, 'height':768}
        )
        page = context.new_page()
        page.goto(url, wait_until='domcontentloaded')
        time.sleep(random.uniform(0.5, 1.5))
        html = page.content()
        context.close()
        browser.close()
        return html
    
def render_html_complete(url):
    try:
        response = requests.get(url, headers = HEADERS)

        if response.status_code == 200:
            html = response.text
            if is_valid_html(html):
                return html
    except requests.RequestException:
        pass

    try:
        html = render_html_playwright(url)
        if is_valid_html(html):
            return html
    except Exception:
        pass

    return None



def reduce_html(response_text):
    soup = BeautifulSoup(response_text, 'html.parser')

    for tag in soup(['script', 'style', 'noscript', 'svg']):
        tag.decompose()
    for cls in ['header', 'footer', 'nav', 'menu', 'cookie', 'breadcrumbs']:
        for el in soup.select(f".{cls}"):
            el.decompose()

    paragraphs = soup.get_text('\n').split("\n")
    keep = set()

    for i, p in enumerate(paragraphs):
        if any(k in p.lower() for k in KEYWORDS):
            for j in range(max(0, i - WINDOW), min(len(paragraphs), i + WINDOW + 1)):
                keep.add(j)
    trimmed_text = '\n'.join(paragraphs[i] for i in sorted(keep))
    return trimmed_text

testDataPath = "C:/Users/ucg8nb/Downloads/GDELT news data.csv"

gdeltDf = pd.read_csv(testDataPath)

virginiaData = gdeltDf[gdeltDf['location_fullname'].str.contains('virginia', case = False)]

outputDf = pd.DataFrame()
totalLen = len(virginiaData)
count = 0
for _, row in virginiaData.iterrows():
    html = render_html_complete(row['link'])
    if html is None:
        newRow = {
            'Link': row['link'],
            'Loaded': False,
            'Text': ''
        }
    else:
        reduced_text = reduce_html(html)
        newRow = {
            'Link': row['link'],
            'Loaded': True,
            'Text': reduced_text
        }
    outputDf = outputDf._append(newRow, ignore_index = True)
    count += 1
    print(f"Completed Entry {count}: {count * 100 / totalLen:.2f}% ({count}/{totalLen}) of the way")

outputDf.to_csv("C:/Users/ucg8nb/Downloads/Virginia News Text.csv")

# for _, row in testDf.iterrows():
#     fullLength = len(row['Request Text'])
#     reductionLength = len(str(row['Big Trim']))
#     print(f"Initial Size {fullLength}, reduced size {reductionLength}, percentage of length {reductionLength/fullLength}")

# firstPerc = []
# bigTrimPerc = []

# for _, row in testDf.iterrows():
#     fullLength = len(row['Request Text'])
#     initalReduction = len(row['First Trim'])
#     bigReduction = len(str(row['Big Trim']))
#     firstPerc.append(initalReduction / fullLength)
#     bigTrimPerc.append(bigReduction / fullLength)

# testDf['FirstPerc'] = firstPerc
# testDf['BigTrimPerc'] = bigTrimPerc

# firstTrims = []
# bigTrims = []

# for _, row in testDf.iterrows():
#     firstTrim, bigTrim = reduce_html(row['Request Text'])
#     firstTrims.append(firstTrim)
#     print(bigTrim.replace('\n', ""))
#     bigTrims.append(bigTrim)

# testDf['First Trim'] = firstTrims
# testDf['Big Trim'] = bigTrims

# testDf.to_csv(testLinksPath, index = False)
# print(testDf[['Playwright Text', 'Request Text']])
