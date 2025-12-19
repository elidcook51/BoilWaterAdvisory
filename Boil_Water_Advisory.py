<<<<<<< HEAD
import os
import pandas as pd
import json
import json5
import anthropic
import requests
import matplotlib.pyplot as plt
import re
import csv
from dotenv import load_dotenv
import dateChecker
import numpy as np

load_dotenv()

API_KEY = os.getenv('API_KEY')
SEARCH_API_KEY = os.getenv('SEARCH_API_KEY')
SECOND_SEARCH_API_KEY = os.getenv('SECOND_SEARCH_API_KEY')
THIRD_SEARCH_API_KEY = os.getenv('THIRD_SEARCH_API_KEY')
FOURTH_SEARCH_API_KEY = os.getenv('FOURTH_SEARCH_API_KEY')
CSE_ID = os.getenv('CSE_ID')
SECOND_CSI_ID = os.getenv('SECOND_CSI_ID')
LLMmodel = os.getenv('LLMmodel')
NEW_SEARCH_API = os.getenv('NEW_SEARCH_API')

JSON_EXTRACTION_SYSTEM = (
    "You extract boil-water advisories from warning messages.\n"
    "Return ONLY valid JSON (no prose, no code fences) with this schema:\n"
    "{\n"
    "  \"advisories\": [\n"
    "    {\n"
    "      \"type\": \"E\" | \"P\",  // E=Emergency, P=Planned\n"
    "      \"location\": \"string\",\n"
    "      \"start_date\": \"YYYY-MM-DD\" | null,\n"
    "      \"end_date\": \"YYYY-MM-DD\" | null,\n"
    "    }\n"
    "  ]\n"
    "}\n"
    "Rules: Use 'E' for emergency, 'P' for planned. If date unknown, use null. "
    'Only list information that is specifically stated in the article'
    "If multiple advisories are mentioned, include one object per advisory."
    "If multiple locations are mentioned, list all locations in one object as a list of municipality names with no extra information."
    "Do not include any explanation—JSON only."
)

CSV_FIELDS = ["type", "location", "start_date", "end_date", "publish_date", 'county', 'state', 'link', 'summary']

def build_json_prompt(article_text: str) -> str:
    return (
        "Article text:\n"
        f"{article_text}\n\n"
        "Extract the advisories as per the schema."
    )

def googleSearch(query, api = FOURTH_SEARCH_API_KEY, cse = SECOND_CSI_ID, numResults = 5):
    url = 'https://www.googleapis.com/customsearch/v1'
    params = {
        'q': query,
        'key': api,
        'cx': cse,
        'num': numResults,
    }

    response = requests.get(url, params = params)
    results = response.json()
    print(results)
    searchResults = []
    for item in results.get('items', []):
        searchResults.append({
            'title': item['title'],
            'link': item['link'],
            'snippet': item['snippet'],
        })

    return searchResults

def langSearch(query, api = NEW_SEARCH_API, search_results = 2):
    headers ={
        'Authorization': "Bearer " + api,
        'Content-Type': 'application/json'
    }
    payload = json.dumps({
        'query': query,
        'freshness': 'oneYear',
        'summary': False,
        'count': search_results
    })
    url = 'https://api.langsearch.com/v1/web-search'

    response = requests.request('POST', url, headers = headers, data = payload)
    
    try:
        data = response.json()
    except requests.exceptions.JSONDecodeError:
        print("Warning: Response is not valid JSON or is empty.")
        return None

    results = data.get('data', {}).get('webPages', {}).get('value', [])
    return results

def get_url_from_langsearch(response):
    output_list = []
    for r in response:
        output_list.append(r['url'])
    return output_list

def queryLLM(query, api = API_KEY, model = LLMmodel, maxTokens = 1024):
    client = anthropic.Anthropic(api_key = api)
    message = client.messages.create(
        model = model,
        max_tokens = maxTokens,
        messages = [{
            'role': 'user',
            'content': query
        }]
    )
    output = ''
    for content in message.content:
        output += content.text
    return output


def call_claude_for_json(article_text: str, api_key: str, model: str = "claude-3-5-sonnet-20240620", max_output_tokens: int = 512):
    client = anthropic.Anthropic(api_key=api_key)

    message = client.messages.create(
        model=model,
        max_tokens=max_output_tokens,
        temperature=0,
        system=JSON_EXTRACTION_SYSTEM,
        messages=[{"role": "user", "content": build_json_prompt(article_text)}],
    )

    # Concatenate text parts from the response
    text = "".join(part.text for part in message.content if getattr(part, "text", None))

    # Be defensive: pull the first JSON object from the response
    m = re.search(r"\{.*\}", text, flags=re.S)
    if not m:
        raise ValueError("LLM did not return JSON.")
    data = json5.loads(m.group(0))
    if "advisories" not in data or not isinstance(data["advisories"], list):
        raise ValueError("JSON missing 'advisories' array.")
    return data["advisories"]

def save_advisories_csv(advisories: list[dict], filepath: str, extra_information: dict):
    file_exists = os.path.isfile(filepath)
    with open(filepath, 'a', newline= '', encoding = 'utf-8') as f:
        writer = csv.DictWriter(f, fieldnames = CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        for a in advisories:
            row = {
                "type": a.get("type") or "",
                "location": a.get("location") or "",
                "start_date": (a.get("start_date") or "") if a.get("start_date") not in (None, "null") else "",
                "end_date": (a.get("end_date") or "") if a.get("end_date") not in (None, "null") else "",
                # "publish_date": (a.get("publish_date") or "") if a.get("publish_date") not in (None, "null") else "",
            }
            for info in extra_information:
                row[info] = extra_information[info]
            writer.writerow(row)

def finalQueryMode(articleText, extraInformation):
    advisories = call_claude_for_json(article_text = articleText, api_key = API_KEY, model = LLMmodel, max_output_tokens = 512)
    save_advisories_csv(advisories, "C:/Users/ucg8nb/Downloads/BWA in VA over 1000.csv", extraInformation)

outputFilePath = "C:/Users/ucg8nb/Downloads/EntireCountryOver5000Utility.csv"


utilityData = pd.read_csv('C:/Users/ucg8nb/Downloads/utilityData.csv')
# FIPSCodes = [20203, 51121, 51113]
# FIPS = utilityData['COUNTYFIPS'].tolist()
# FIPSCodes = [20203, 51540, 51121, 51159, 51113]
# while len(FIPSCodes) < 15:
#     newNum = random.randint(10000, 90000)
#     if newNum in FIPS:
#         FIPSCodes.append(newNum)
# smallData = utilityData[utilityData['COUNTYFIPS'].isin(FIPSCodes)].drop_duplicates(subset = ['COUNTYFIPS'])
smallData = utilityData[utilityData['POPULATION_SERVED_COUNT'] > 5000]
years = np.arange(2020, 2026, 1)
for index, row in smallData.iterrows():
    for year in years:
        county = row['County Name']
        state = row['STATE_CODE']
        searchResults = langSearch(f'Boil Water Advsiory in {year} at {county}, {state}')
        if searchResults == None:
            continue
        for result in searchResults:
            if 'boil' in result['snippet']:
                header = {
                    "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
                }
                dateInfo = dateChecker.extract_webpage_dates(result['displayUrl'])
                if dateInfo == None:
                    published = ''
                    updated = ''
                else:
                    published = dateInfo['published']
                    updated = dateInfo['updated']
                information = {
                    'county': county,
                    'state': state,
                    'link': result['displayUrl'],
                    'snippet': result['snippet'],
                    'publish_date': published
                }
                writeheader = not os.path.exists(outputFilePath) or os.path.getsize(outputFilePath) == 0
                with open(outputFilePath, mode = 'a', newline = '') as file:
                    writer = csv.DictWriter(file, fieldnames = information.keys())
                    if writeheader:
                        writer.writeheader()
                    writer.writerow(information)


# for index, row in smallData.iterrows():
#     county = row['County Name']
#     state = row['PWSID'][:2]
#     searchResults = langSearch(f'boil water advisory warning from news sites {county}, {state}', search_results = 5)
#     for result in searchResults:
#         if 'boil' in result['snippet']:
#             header = {
#                 "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
#             }
#             # articleInfo = extract_article(result['url'])
#             # articleText = articleInfo['text']
#             dateInfo = dateChecker.extract_webpage_dates(result['displayUrl'])
#             if dateInfo == None:
#                 published = '',
#                 updated = '',
#             else:
#                 published = dateInfo['published']
#                 updated = dateInfo['updated']
#             extraInformation = {
#                 'county': county,
#                 'state': state,
#                 'link': result['displayUrl'],
#                 'summary': result['summary'],
#                 'publish_date': published, 
#             }
#             finalQueryMode(result['summary'], extraInformation)
=======
import os
import pandas as pd
import json
import anthropic
import requests
from bs4 import BeautifulSoup
import datetime
from datetime import timedelta
import matplotlib.pyplot as plt
import random
from readability import Document
import re
import trafilatura
import html2text
import csv
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('API_KEY')
SEARCH_API_KEY = os.getenv('SEARCH_API_KEY')
SECOND_SEARCH_API_KEY = os.getenv('SECOND_SEARCH_API_KEY')
THIRD_SEARCH_API_KEY = os.getenv('THIRD_SEARCH_API_KEY')
FOURTH_SEARCH_API_KEY = os.getenv('FOURTH_SEARCH_API_KEY')
CSE_ID = os.getenv('CSE_ID')
SECOND_CSI_ID = os.genenv('SECOND_CSI_ID')
LLMmodel = os.getenv('LLMmodel')

JSON_EXTRACTION_SYSTEM = (
    "You extract boil-water advisories from news text.\n"
    "Return ONLY valid JSON (no prose, no code fences) with this schema:\n"
    "{\n"
    "  \"advisories\": [\n"
    "    {\n"
    "      \"type\": \"E\" | \"P\",  // E=Emergency, P=Planned\n"
    "      \"location\": \"string\",\n"
    "      \"start_date\": \"YYYY-MM-DD\" | null,\n"
    "      \"end_date\": \"YYYY-MM-DD\" | null,\n"
    "    }\n"
    "  ]\n"
    "}\n"
    "Rules: Use 'E' for emergency, 'P' for planned. If date unknown, use null. "
    'Only list information that is specifically stated in the article'
    "If multiple advisories or locations are mentioned, include one object per advisory/location. "
    "Do not include any explanation—JSON only."
)

CSV_FIELDS = ["type", "location", "start_date", "end_date", "publish_date", 'county', 'state', 'link']

def build_json_prompt(article_text: str) -> str:
    return (
        "Article text:\n"
        f"{article_text}\n\n"
        "Extract the advisories as per the schema."
    )

def googleSearch(query, api = FOURTH_SEARCH_API_KEY, cse = SECOND_CSI_ID, numResults = 5):
    url = 'https://www.googleapis.com/customsearch/v1'
    params = {
        'q': query,
        'key': api,
        'cx': cse,
        'num': numResults,
    }

    response = requests.get(url, params = params)
    results = response.json()
    print(results)
    searchResults = []
    for item in results.get('items', []):
        searchResults.append({
            'title': item['title'],
            'link': item['link'],
            'snippet': item['snippet'],
        })

    return searchResults

def queryLLM(query, api = API_KEY, model = LLMmodel, maxTokens = 1024):
    client = anthropic.Anthropic(api_key = api)
    message = client.messages.create(
        model = model,
        max_tokens = maxTokens,
        messages = [{
            'role': 'user',
            'content': query
        }]
    )
    output = ''
    for content in message.content:
        output += content.text
    return output


def call_claude_for_json(article_text: str, api_key: str, model: str = "claude-3-5-sonnet-20240620", max_output_tokens: int = 512):
    client = anthropic.Anthropic(api_key=api_key)

    message = client.messages.create(
        model=model,
        max_tokens=max_output_tokens,
        temperature=0,
        system=JSON_EXTRACTION_SYSTEM,
        messages=[{"role": "user", "content": build_json_prompt(article_text)}],
    )

    # Concatenate text parts from the response
    text = "".join(part.text for part in message.content if getattr(part, "text", None))

    # Be defensive: pull the first JSON object from the response
    m = re.search(r"\{.*\}", text, flags=re.S)
    if not m:
        raise ValueError("LLM did not return JSON.")

    data = json.loads(m.group(0))
    if "advisories" not in data or not isinstance(data["advisories"], list):
        raise ValueError("JSON missing 'advisories' array.")
    return data["advisories"]

def save_advisories_csv(advisories: list[dict], filepath: str, extra_information: dict):
    file_exists = os.path.isfile(filepath)
    with open(filepath, 'a', newline= '', encoding = 'utf-8') as f:
        writer = csv.DictWriter(f, fieldnames = CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        for a in advisories:
            row = {
                "type": a.get("type") or "",
                "location": a.get("location") or "",
                "start_date": (a.get("start_date") or "") if a.get("start_date") not in (None, "null") else "",
                "end_date": (a.get("end_date") or "") if a.get("end_date") not in (None, "null") else "",
                "publish_date": (a.get("publish_date") or "") if a.get("publish_date") not in (None, "null") else "",
            }
            for info in extra_information:
                row[info] = extra_information[info]
            writer.writerow(row)



def fetch_html(url):
    header = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    }
    response = requests.get(url, headers = header)
    return response.text

def extract_from_jsonld(html: str) -> dict | None:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all("script", type="application/ld+json"):
        # Some sites pack multiple JSON objects/arrays in one script tag
        try:
            data = json.loads(tag.string or "{}")
        except json.JSONDecodeError:
            continue

        candidates = data if isinstance(data, list) else [data]
        for obj in candidates:
            t = obj.get("@type", "")
            if isinstance(t, list):
                is_article = any(x in ("NewsArticle", "Article", "ReportageNewsArticle") for x in t)
            else:
                is_article = t in ("NewsArticle", "Article", "ReportageNewsArticle")

            if is_article:
                out = {
                    "title": obj.get("headline") or obj.get("name"),
                    "author": None,
                    "date": obj.get("datePublished") or obj.get("dateCreated"),
                    "description": obj.get("description"),
                    "articleBody": obj.get("articleBody"),
                    "publisher": (obj.get("publisher") or {}).get("name")
                                 if isinstance(obj.get("publisher"), dict) else obj.get("publisher"),
                    "url": obj.get("url"),
                }
                # Normalize author(s)
                author = obj.get("author")
                if isinstance(author, list):
                    out["author"] = ", ".join(
                        [a.get("name") if isinstance(a, dict) else str(a) for a in author]
                    )
                elif isinstance(author, dict):
                    out["author"] = author.get("name")
                elif author:
                    out["author"] = str(author)

                # If articleBody is present, this is ideal.
                if out.get("articleBody"):
                    return out
                # If articleBody missing, keep metadata; we'll fill body via other extractors
                return out
    return None

def extract_main_text(html: str) -> str | None:
    # 3a) Trafilatura (very strong on news)
    downloaded = trafilatura.extract(html, output_format="txt", include_images=False, include_tables=False,
                                     favor_recall=False, with_metadata=False)
    if downloaded and len(downloaded.strip()) > 200:  # heuristic
        return downloaded.strip()

    # 3b) Readability
    try:
        doc = Document(html)
        article_html = doc.summary(html_partial=True)
        soup = BeautifulSoup(article_html, "lxml")
        for tag in soup(["script", "style", "noscript", "form", "footer", "nav", "aside"]):
            tag.decompose()
        text = soup.get_text(True)
        text = re.sub(r"\n{2,}", "\n\n", text).strip()
        if len(text) > 200:
            return text
    except Exception:
        pass

    # 3c) Very simple density fallback
    soup = BeautifulSoup(html, "lxml")
    for t in soup(["script", "style", "noscript", "form"]):
        t.decompose()

    best_node, best_score = None, 0
    for node in soup.find_all(["article", "section", "div", "main"]):
        text = node.get_text(True)
        link_count = len(node.find_all("a")) + 1
        score = len(text) / link_count  # crude text/link density
        if score > best_score and len(text) > 200:
            best_score, best_node = score, node
    if best_node:
        text = best_node.get_text(True)
        text = re.sub(r"\n{2,}", "\n\n", text)
        return text.strip()
    return None

def html_to_markdown(html: str) -> str:
    h = html2text.HTML2Text()
    h.ignore_links = False  # keep URLs if helpful
    h.ignore_images = True
    h.body_width = 0  # no wrapping
    md = h.handle(html)
    return "\n".join(line.rstrip() for line in md.splitlines()).strip()


def extract_article(url: str) -> dict:
    html = fetch_html(url)
    meta = extract_from_jsonld(html) or {}
    body = meta.get("articleBody")

    if not body:
        body = extract_main_text(html)

    # If we still somehow have only HTML (from readability), ensure plaintext:
    if body and ("<" in body and ">" in body and "</" in body):
        body = BeautifulSoup(body, "lxml").get_text(True)

    # Fallback: convert whole HTML to markdown if everything else fails
    if not body:
        body = html_to_markdown(html)

    # Collapse whitespace
    body = "\n".join(chunk.strip() for chunk in body.splitlines() if chunk.strip())

    return {
        "title": meta.get("title"),
        "author": meta.get("author"),
        "date": meta.get("date"),
        "publisher": meta.get("publisher"),
        "url": meta.get("url") or url,
        "description": meta.get("description"),
        "text": body,
    }

def finalQueryMode(articleText, extraInformation):
    advisories = call_claude_for_json(article_text = articleText, api_key = API_KEY, model = LLMmodel, max_output_tokens = 512)
    save_advisories_csv(advisories, "C:/Users/ucg8nb/Downloads/Boil Water Advisory List.csv", extraInformation)




# newsDf = pd.read_csv("C:/Users/ucg8nb/Downloads/Small News.csv")
# bwaLinkDf = getAllAdvisories(newsDf)
# bwaLinkDf.to_csv("C:/Users/ucg8nb/Downloads/Small BWA list.csv")








# crosswalk = pd.read_csv('C:/Users/ucg8nb/Downloads/COUNTYFIPS Codes.csv')
# utilityData = pd.read_csv("C:/Users/ucg8nb/Downloads/CWS_for_UVA.csv")
# utilityData = pd.merge(utilityData, crosswalk, on = 'COUNTYFIPS')
# utilityData.to_csv('C:/Users/ucg8nb/Downloads/utilityData.csv')

utilityData = pd.read_csv('C:/Users/ucg8nb/Downloads/utilityData.csv')
# FIPSCodes = [20203, 51121, 51113]
FIPS = utilityData['COUNTYFIPS'].tolist()
FIPSCodes = [20203, 51540, 51121, 51159, 51113]
while len(FIPSCodes) < 15:
    newNum = random.randint(10000, 90000)
    if newNum in FIPS:
        FIPSCodes.append(newNum)
smallData = utilityData[utilityData['COUNTYFIPS'].isin(FIPSCodes)].drop_duplicates(subset = ['COUNTYFIPS'])
for index, row in smallData.iterrows():
    county = row['County Name']
    state = row['PWSID'][:2]
    searchResult = googleSearch(f'local news in {county}, {state}', numResults = 1)
    if len(searchResult) == 0:
        continue
    result = searchResult[0]
    link = result['link']
    articles = googleSearch(f'Boil Water Notice site:{link} after:{datetime.datetime(2025, 1, 1).strftime("%Y-%m-%d")}', numResults = 2)
    for article in articles:
        if 'boil' in article['snippet']:
            message = queryLLM(
                f'{article['snippet']} Responding in either "YES" or "NO" do you think this news article is about a boil water advisory')
            print(message)
            if "YES" in message:
                header = {
                    "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
                }
                articleInfo = extract_article(article['link'])
                articleText = articleInfo['text']
                extraInformation = {
                    'publish_date': articleInfo['date'],
                    'county': county,
                    'state': state,
                    'link': article['link']
                }
                finalQueryMode(articleText, extraInformation)
>>>>>>> c3f4b91 (Removed extra commands)
