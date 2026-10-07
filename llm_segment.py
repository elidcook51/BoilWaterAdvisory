from dotenv import load_dotenv
import os
from openai import OpenAI
import json
import pandas as pd
import numpy as np
from pathlib import Path
import re

load_dotenv()

BOIL_WATER_STRUCTURE_V2 = {
    'type': 'object',
    'properties': {
        'publish_date': {'type': ['string', 'null']},
        'start_date': {'type': ['string', 'null']},
        'end_date': {'type': ['string', 'null']},
        'backup_date': {'type': ['string', 'null']},
        'backup_date_note': {'type': ['string', 'null']},
        'date_evidence': {
            'type': 'object',
            'properties': {
                'start_date': {'type': ['string', 'null']},
                'end_date': {'type': ['string', 'null']},
                'backup_date': {'type': ['string', 'null']},
            },
            'required': ['start_date', 'end_date', 'backup_date'],
        },
        'date_notes': {'type': ['string', 'null']},
        'multiple_advisories': {'type': 'boolean'},
        'location': {
            'type': 'object',
            'properties': {
                'state': {'type': ['string', 'null']},
                'county': {'type': ['string', 'null']},
                'locality': {'type': ['string', 'null']},
                'utility_affected': {'type': ['string', 'null']},
                'PWS_ID': {'type': ['string', 'null']},
            },
            'required': ['state', 'county', 'locality', 'utility_affected', 'PWS_ID'],
        },
        'advisory_type': {
            'type': 'string',
            'enum': ['emergency', 'planned', 'unknown'],
        },
        'reason': {'type': ['string', 'null']},
        'people_affected': {'type': ['number', 'null']},
        'source_type': {
            'type': 'string',
            'enum': ['government website', 'utility website', 'government Facebook',
                     'utility Facebook', 'media', 'other', 'unknown'],
        },
    },
    'required': [
        'publish_date', 'start_date', 'end_date', 'backup_date', 'backup_date_note',
        'date_evidence', 'date_notes', 'multiple_advisories', 'location',
        'advisory_type', 'reason', 'people_affected', 'source_type',
    ],
}

EXTRACTION_INSTRUCTIONS = """You extract structured data about ONE boil water advisory from a news article.
Return ONLY valid JSON matching the schema exactly. No prose, no code fences, no comments.
If a field is not stated in the article, use null. Never invent dates, places, or numbers.

You are given ARTICLE METADATA (publish date and source URL) separately from the article text.
- Echo the provided publish_date back in the publish_date field. Do NOT re-derive it from the text.
  If no publish date was provided, use null.
- Use publish_date ONLY to resolve relative date expressions in the text:
  "yesterday" = publish_date minus 1 day; "last Tuesday" = the most recent Tuesday
  strictly before publish_date; "effective immediately" / "today" = publish_date.
  If publish_date is null, leave relative dates as null rather than guessing.

DATE RULES (read carefully -- the old prompt failed here):
- start_date: the date the advisory TOOK EFFECT. Prefer an explicitly stated effective
  date ("effective March 3", "beginning Monday"). If the article only gives the
  announcement/issue date, use that date and say so in date_notes.
- end_date: the date the advisory was LIFTED or ended ("lifted on Friday",
  "rescinded March 10"). If the article says the advisory is still active
  ("until further notice", "remains in effect", present tense with no end date),
  use null. NEVER use the publish_date as the end_date.
- backup_date: ONLY a date explicitly tied to THIS advisory's timing that is NEITHER
  the start NOR the end. Allowed: a water-retest/results date, a public meeting date
  about the advisory, or the date of a PREVIOUS related advisory (previous-advisory
  dates go here, never in start_date). If no such date is stated, use null.
  backup_date is NEVER the publish date.
- backup_date_note: at most 12 words saying what backup_date refers to
  (e.g. "water retest date", "date of previous advisory"), or null.
- date_evidence: for EACH of start_date, end_date, backup_date that is not null,
  a verbatim quote of at most 25 words from the article supporting that date.
  If you cannot quote supporting text, the date must be null.
- date_notes: caveats, e.g. "start_date is the announcement date; effective date
  not stated". Null if none.
- All dates must be YYYY-MM-DD.

OTHER RULES:
- advisory_type: "emergency" for advisories responding to a sudden event (pipe burst,
  pressure loss, contamination found); "planned" for scheduled work (construction,
  maintenance); "unknown" otherwise.
- location: fill state/county/locality/utility_affected/PWS_ID only with what the
  article states. Do not expand abbreviations into guesses; do not infer the county
  from the city unless the article names it.
- people_affected: a number only (e.g. 2000 for "about 2,000 residents"). Null if
  not stated ("hundreds" -> null, note it in date_notes is wrong place -- just null).
- source_type: classify the article's publisher.
- If the article is NOT about a boil water advisory, return null for every field
  (multiple_advisories=false) but still follow the schema exactly.
- If the article describes MORE THAN ONE distinct advisory (different towns and/or
  different dates), extract the FIRST/main one and set multiple_advisories=true.

EXAMPLE:
Article publish date: 2025-03-05. Source: local news site.
Article text: "The City of Springfield issued a precautionary boil water advisory
for the north side on Tuesday after a water main break on Elm Street. About 2,000
residents are affected. Officials say the advisory will remain in effect until
further notice. Water samples will be retested on Friday."

Correct output:
{"publish_date": "2025-03-05", "start_date": "2025-03-04", "end_date": null,
"backup_date": "2025-03-07", "backup_date_note": "water retest date",
"date_evidence": {"start_date": "issued a precautionary boil water advisory for the north side on Tuesday",
"end_date": null, "backup_date": "Water samples will be retested on Friday"},
"date_notes": null, "multiple_advisories": false,
"location": {"state": null, "county": null, "locality": "Springfield",
"utility_affected": null, "PWS_ID": null},
"advisory_type": "emergency", "reason": "water main break on Elm Street",
"people_affected": 2000, "source_type": "media"}

Note: "Tuesday" resolved against publish_date 2025-03-05 (a Wednesday) -> 2025-03-04.
"until further notice" -> end_date null, NOT the publish date. The retest Friday ->
backup_date with a note. State left null because the article never names it.
"""

LLM_model = 'gpt-4.1-nano'

client = OpenAI(api_key = os.getenv('OPEN_AI_API_KEY'))

# response = client.response.create(
#     model = LLM_model,
#     input = ""
# )

# print(response.output_text)

def flatten_dict(d, parent_key = "", sep = "_"):
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep))
        else:
            items[new_key] = v
    return items


def extract_advisory(client, article_text, publish_date=None, source_url=None,
                        model="gpt-4.1-mini"):
    """Extract one advisory record from article text.

    client: an OpenAI client (as in llm_segment.py).
    article_text: full extracted article text.
    publish_date: "YYYY-MM-DD" from dateChecker (or None).
    source_url: article URL (or None).
    Returns: dict matching BOIL_WATER_STRUCTURE_V2.
    Raises: ValueError if the model output cannot be parsed as JSON.
    """
    user_block = (
        f"Article publish date (metadata): {publish_date}\n"
        f"Article source URL (metadata): {source_url}\n\n"
        f"Article text:\n{article_text}\n\n"
        "Extract the advisory as per the instructions and schema."
    )
    response = client.responses.create(
        model=model,
        instructions=EXTRACTION_INSTRUCTIONS,
        input=user_block,
    )
    text = response.output_text or ""
    # Defensive: strip code fences if the model adds them anyway.
    text = re.sub(r"^```(?:json)?\s*", "", text.strip())
    text = re.sub(r"\s*```$", "", text)
    m = re.search(r"\{.*\}", text, flags=re.S)
    if not m:
        raise ValueError("LLM did not return JSON.")
    data = json.loads(m.group(0))
    # Light validation: all required top-level keys present.
    missing = [k for k in BOIL_WATER_STRUCTURE_V2["required"] if k not in data]
    if missing:
        raise ValueError(f"LLM JSON missing keys: {missing}")
    return data

def combine_df_with_csv(df, csvPath):
    csvPath = Path(csvPath)

    if csvPath.exists():
        existingDf = pd.read_csv(csvPath)
        combinedDf = pd.concat([existingDf, df], ignore_index = True)
    else:
        combinedDf = df.copy()
    
    combinedDf.to_csv(csvPath, index = False)

newsScrapedCsv = "C:/Users/ucg8nb/Downloads/Virginia News Text.csv"
structuredCsv = "C:/Users/ucg8nb/Downloads/Virginia Run.csv"

def unstructured_df_to_structured(inpustCSV, outputCSV, numRows = 100):

    if os.path.exists(outputCSV):
        previouslyCompleted = pd.read_csv(outputCSV)
        doneLinks = set(previouslyCompleted['Source URL'])

    fullNews = pd.read_csv(inpustCSV)
    loadedNews = fullNews[fullNews['Loaded']]
    loadedNews = loadedNews[:numRows]
    loadedNews = loadedNews.reset_index()

    totalCount = len(loadedNews)

    rows = []

    for idx, row in loadedNews.iterrows():
        try:
            if row['Link'] not in doneLinks:
                structured = extract_advisory(client, row['Text'])

                flat_structured = flatten_dict(structured)

                flat_structured['Source URL'] = row['Link']

                rows.append(flat_structured)
                print(f"Finished row {idx} ({idx / totalCount * 100:.2f}% {idx}/{totalCount})!")

        except Exception as e:
            print(f"Failed row {idx}: {e}")

    df_structured = pd.DataFrame(rows)
    combine_df_with_csv(df_structured, outputCSV)

    print(f"Processed {len(df_structured)} advisories")

unstructured_df_to_structured(newsScrapedCsv, structuredCsv, numRows = 10000)

# structured_data = pd.read_csv(structuredCsv)
# virginiaData = structured_data[structured_data['location_state'] == 'Virginia']
# print(len(virginiaData))