import pandas as pd
import importlib
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from boil_crawler import BoilAdvisorySpider
from dateChecker import estimate_publication_date
from multiprocessing import Process

# newsPaperDf = pd.read_csv('better_newspapers_by_state.csv')
# newsPaperDf['scraped'] = False
# newsPaperDf.to_csv('better_newspapers_by_state.csv')

# kentuckyList = pd.read_csv('KentuckyCountyList.csv')
# kentuckyList['scraped'] = False
# kentuckyList = kentuckyList.loc[:, ~df.columns.str.contains('Unnamed')]
# kentuckyList.to_csv('KentuckyCountyList.csv')

def run_batch(batch):
    process = CrawlerProcess()
    for seed in batch:
        process.crawl(BoilAdvisorySpider, start_url = seed)
    process.start()

def run_in_batches(seeds, batch_size):
    for i in range(0, len(seeds), batch_size):
        batch = seeds[i:i+batch_size]
        p = Process(target = run_batch, args = (batch,))
        p.start()
        p.join()
        print(f'********************* \n\n\n\n ELEMENT {i} COMPLETED\n\n\n\n **********************')
        for b in batch:
            newsPaperDf.loc[newsPaperDf['newspaper_url'] == b, 'scraped'] = True
            newsPaperDf.to_csv('better_newspaper_by_state.csv')

if __name__ == '__main__':
    newsPaperDf = pd.read_csv('better_newspapers_by_state.csv')
    kentuckyDf = newsPaperDf[newsPaperDf['state'] == 'Kentucky']
    SEEDS = kentuckyDf[kentuckyDf['scraped'] == False]['newspaper_url'].tolist()

    # kentuckyList = pd.read_csv('KentuckyCountyList.csv')
    # SEEDS = kentuckyList[kentuckyList['scraped'] == False]['Homepage'].tolist()
    run_in_batches(SEEDS, batch_size = 1)

# gottenLinks = pd.read_csv('boil_links.csv')
# gottenLinks = gottenLinks['url'].tolist()
# for url in gottenLinks:
#     datePub = estimate_publication_date(url)
#     print(f"For url {url}, estimated publish date of {datePub['estimated_publication_date']} with confidence {datePub['confidence_score']}")


# projID = 'awesome-sphere-477404-v5'
# # credentials = service_account.Credentials.from_service_account_file(r"C:\Users\ucg8nb\AppData\Roaming\gcloud\application_default_credentials.json")

# client = bigquery.Client(project = projID)

# QUERY = r"""
#     WITH candidates AS (
#     SELECT 
#         PARSE_TIMESTAMP('%Y%m%d%H%M%S', CAST(DATE AS STRING)) AS publish_time,
#         DocumentIdentifier AS link,
#         SourceCommonName AS source,
#         V2Locations
#     FROM `gdelt-bq.gdeltv2.gkg_partitioned`
#     WHERE _PARTITIONTIME BETWEEN TIMESTAMP('2013-01-01') AND CURRENT_TIMESTAMP()
#     AND (
#         LOWER(DocumentIdentifier) LIKE '%boil%water%'
#         -- Can add more slugs later to check for other things if this doesn't do great
#     )
# ),
# expanded AS (
#     SELECT 
#     publish_time, link, source, loc AS loc_raw
#     FROM candidates, UNNEST(SPLIT(V2Locations, ';')) AS loc
# ),
# parsed AS (
#     SELECT 
#     publish_time,
#     link,
#     source,
#     SAFE_CAST(SPLIT(loc_raw, '#')[OFFSET(0)] AS INT64) AS location_type,
#     SPLIT(loc_raw, '#')[OFFSET(1)] AS location_fullname,
#     SPLIT(loc_raw, '#')[OFFSET(2)] AS country_code,
#     SPLIT(loc_raw, '#')[OFFSET(3)] AS adm1_code,
#     SAFE_CAST(SPLIT(loc_raw, '#')[OFFSET(5)] AS FLOAT64) AS latitude,
#     SAFE_CAST(SPLIT(loc_raw, '#')[OFFSET(6)] AS FLOAT64) AS longitude
# FROM expanded
# )
# SELECT * 
# FROM parsed
# WHERE country_code = 'US'
# """

# # print(QUERY)

# df_hist = client.query(QUERY).result().to_arrow().to_pandas()
# df_hist = df_hist.drop_duplicates(subset= ['link'])

# df_hist.to_csv("C:/Users/ucg8nb/Downloads/GDELT news data.csv")
