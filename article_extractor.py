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
import matplotlib.pyplot as plt
import numpy as np
import re

load_dotenv()

testLinksPath = "C:/Users/ucg8nb/Downloads/Boil_Water_Truth_v0.csv"

testDf = pd.read_csv(testLinksPath)

HEADERS = ast.literal_eval(os.getenv('HEADERS'))

KEYWORDS = ['boil water', 'boil advisory', 'water advisory', 'precautionary boil', 'do not drink', 'issued', 'water system', 'public water supply', 'lifted', 'rescinded', 'effective', 'until further notice', 'posted', 'post', 'emergency', 'planned', 'boil-water', 'boil', 'water', 'pressure loss', 'bottled water', 'effective', 'issued', 'updated', 'until further notice', 'lifted on', 'in effect', 'as of', 'starting', 'ending']
    
DATE_REGEX = re.compile(
    r'\b('
    r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|'          # 3/5/2024, 03-05-24
    r'\d{4}[/-]\d{1,2}[/-]\d{1,2}|'            # 2024-03-05
    r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2},?\s+\d{4}|' 
    r'\d{1,2}\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{4}'
    r')\b',
    re.IGNORECASE
)

WINDOW = 4

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
    if 'html' not in response_text:
        return ""

    soup = BeautifulSoup(response_text, 'html.parser')

    paragraphs = soup.get_text('\n').split("\n")
    keep = set()

    for i, p in enumerate(paragraphs):
        text = p.lower()

        keyword_match = any(k in text for k in KEYWORDS)
        date_match = DATE_REGEX.search(p) is not None

        if keyword_match or date_match:
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