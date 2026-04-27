from dotenv import load_dotenv
import os
from openai import OpenAI
import json
import pandas as pd
import numpy as np

BOIL_WATER_STRUCTURE = {
    'type': 'object',
    'properties': {
        'start_date': {'type': ['string', 'null']},
        'end_date': {'type': ['string', 'null']},
        'backup_date': {'type': ['string', 'null']},
        'location': {
            'type': 'object',
            'properties': {
                'state': {'type': ['string', 'null']},
                'county': {'type': ['string', 'null']},
                'locality': {'type': ['string', 'null']},
                'utility_affected': {'type': ['string', 'null']},
                'PWS_ID': {'type': ['string', 'null']}
            },
            'required': ['state', 'county', 'locality','utility_affected', 'PWS_ID']
            },
        'advisory_type': {
            'type': 'string',
            'enum': ['emergency', 'planned', 'unknown']
        },
        'reason': {'type': ['string', 'null']},
        'people_affected': {'type': ['number', 'null']},
        'source_type': {
            'type': 'string',
            'enum': ['government website', 'utility website', 'government Facebook', 'utility Facebook', 'media', 'other', 'unknown']
        }
    },
    "required": [
    "start_date",
    "end_date",
    "backup_date",
    "location",
    "advisory_type",
    "reason",
    "people_affected",
    "source_type"
    ]
}

instructions = (
    "Extract structured data from a boil water advisory.\n"
    "Return ONLY valid JSON matching the exact schema.\n"
    "If a field is not present, use null.\n"
    "If neither start nor end date is known, but other information is known about the timing of the advisory, put that date in backup date. Do not make up information to place in this spot, if no date information is known then put null.\n",
    'Dates must be YYYY-MM-DD.\n'
    "Emergency boil water advisories are in response to an extreme event happening (pipe burst, loss of pressure etc.), while planned ones are for events such as construction which could cause an issue\n"
    "If the text is not about a boil water advisory return null for all values of the schema but still follow it exactly\n"
    f'Schema is {BOIL_WATER_STRUCTURE}'
)

load_dotenv()

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


def boil_water_LLM_query(client, advisory_text):

    response = client.responses.create(
        model = LLM_model,
        
        input = f"Follow instructions {instructions} with text {advisory_text}"
    )

    return json.loads(response.output_text)

newsScrapedCsv = "C:/Users/ucg8nb/Downloads/Canada News Text.csv"
structuredCsv = "C:/Users/ucg8nb/Downloads/Canada First Run.csv"

def unstructured_df_to_structured(inpustCSV, outputCSV, numRows = 100):

    fullNews = pd.read_csv(inpustCSV)
    loadedNews = fullNews[fullNews['Loaded']]
    loadedNews = loadedNews[:numRows]
    loadedNews = loadedNews.reset_index()

    totalCount = len(loadedNews)

    rows = []

    for idx, row in loadedNews.iterrows():
        try:
            structured = boil_water_LLM_query(client, row['Text'])

            flat_structured = flatten_dict(structured)

            flat_structured['Source URL'] = row['Link']

            rows.append(flat_structured)
            print(f"Finished row {idx} ({idx / totalCount * 100:.2f}% {idx}/{totalCount})!")

        except Exception as e:
            print(f"Failed row {idx}: {e}")

    df_structured = pd.DataFrame(rows)
    df_structured.to_csv(outputCSV, index = False)

    print(f"Processed {len(df_structured)} advisories")

unstructured_df_to_structured(newsScrapedCsv, structuredCsv, numRows = 10000)

# structured_data = pd.read_csv(structuredCsv)
# virginiaData = structured_data[structured_data['location_state'] == 'Virginia']
# print(len(virginiaData))