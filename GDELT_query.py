from google.cloud import bigquery
import pandas as pd

client = bigquery.Client()

outputPath = "C:/Users/ucg8nb/Downloads/GDELT Boil Water Data Canada.csv"

query = """
WITH candidates AS (
  SELECT
    PARSE_TIMESTAMP('%Y%m%d%H%M%S', CAST(`DATE` AS STRING)) AS publish_time,
    DocumentIdentifier AS link,
    SourceCommonName AS source,
    V2Locations
  FROM `gdelt-bq.gdeltv2.gkg_partitioned`
  WHERE _PARTITIONTIME BETWEEN TIMESTAMP('2013-01-01')
        AND CURRENT_TIMESTAMP()
    AND DocumentIdentifier LIKE '%boil%water%'
    AND V2Locations LIKE '%#CA#%'
),
expanded AS (
  SELECT
    publish_time,
    link,
    source,
    loc AS raw_loc
  FROM candidates,
  UNNEST(SPLIT(V2Locations, ';')) AS loc
),
parsed AS (
  SELECT
    publish_time,
    link,
    source,
    SAFE_CAST(SPLIT(raw_loc, '#')[OFFSET(0)] AS INT64) AS location_type,
    SPLIT(raw_loc, '#')[OFFSET(1)] AS location_fullname,
    SPLIT(raw_loc, '#')[OFFSET(2)] AS country_code,
    SPLIT(raw_loc, '#')[OFFSET(3)] AS adm1_code,
    SAFE_CAST(SPLIT(raw_loc, '#')[OFFSET(5)] AS FLOAT64) AS latitude,
    SAFE_CAST(SPLIT(raw_loc, '#')[OFFSET(6)] AS FLOAT64) AS longitude
  FROM expanded
)
SELECT *
FROM parsed
WHERE country_code = 'CA';
"""

df = client.query(query).to_dataframe()
print(df.head())

df.to_csv(outputPath, index = False)