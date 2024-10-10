CREATE OR REPLACE TABLE `vendornews.news.gkg_clustered`
CLUSTER BY Persons, Organizations
AS
SELECT 
    DATE,
    DocumentIdentifier,
    Persons,
    Organizations,
    CAST(SPLIT(V2Tone, ',')[SAFE_OFFSET(0)] AS FLOAT64) AS V2Tone_First
FROM `gdelt-bq.gdeltv2.gkg_partitioned`
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR))
  AND CAST(SPLIT(V2Tone, ',')[SAFE_OFFSET(0)] AS FLOAT64) < -1
  AND NOT (Persons IS NULL AND Organizations IS NULL)
  AND NOT (DocumentIdentifier IS NULL);