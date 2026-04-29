SELECT
    ticker,
    date                         AS data,
    ROUND(open::numeric,  4)     AS abertura,
    ROUND(close::numeric, 4)     AS fechamento,
    ROUND(high::numeric,  4)     AS maxima,
    ROUND(low::numeric,   4)     AS minima,
    CAST(volume AS BIGINT)       AS volume,
    CURRENT_TIMESTAMP            AS extraido_em

FROM bronze.precos
WHERE close  IS NOT NULL AND close  > 0
  AND open   IS NOT NULL AND open   > 0
  AND high   IS NOT NULL AND high   > 0
  AND low    IS NOT NULL AND low    > 0
