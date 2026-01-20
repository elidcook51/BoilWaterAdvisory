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
