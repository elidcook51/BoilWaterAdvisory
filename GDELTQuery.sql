WITH candidates AS (
    SELECT 
        PARSE_TIMESTAMP('%Y%m%d%H%M%S', DATE) AS publish_time,
        DocumentIdentifier AS link,
        SourceCommonName AS source,
        V2Locations
    FROM `gdelt-bq.gdeltv2.gkg_partitioned`
    WHERE _PARTITIONTIME BETWEEEN TIMESTAMP('2013-01-01') AND CURRENT_TIMESTAMP()
    AND (
        LOWER(DocumentIdentifier) LIKE '%boil%water%'
        -- Can add more slugs later to check for other things if this doesn't do great
    )
),
expanded AS (
    SELECT 
    publish_time, link, source, loc AS raw_loc
    FROM candidates, UNNEST(SPLIT(V2Locations, ';')) AS loc
)
parsed AS (SELECT 
    publish_time,
    link,
    source,
    SAFE_CAST(SPLIT(loc_raw, '#')[OFFSET[0] AS INT64]) AS location_type,
    SPLIT(loc_raw, '#')[OFFSET(1)] AS location_fullname,
    SPLIT(loc_raw, '#')[OFFSET(2)] AS country_code,
    SPLIT(loc_raw, '#')[OFFSET(3)] AS adm1_code,
    SAFE_CAST(SPLIT(loc_raw, '#')[OFFSET(5)] AS FLOAT64) AS latitude,
    SAFE_CAST(SPLIT(loc_raw, '#')[OFFSET(6)] AS FLOAT64) AS longitude
FROM expanded
)
SELECT * 
FROM parsed
WHERE country_code = 'US'
