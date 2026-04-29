WITH silver AS (
    SELECT * FROM {{ ref('fct_precos_diarios') }}
),

com_metricas AS (
    SELECT
        *,

        -- Retorno do dia em relação ao fechamento anterior
        ROUND(
            (fechamento - LAG(fechamento) OVER (PARTITION BY ticker ORDER BY data))
            / LAG(fechamento) OVER (PARTITION BY ticker ORDER BY data) * 100,
        4) AS retorno_pct,

        -- Média móvel de 7 dias (tendência curta)
        ROUND(AVG(fechamento) OVER (
            PARTITION BY ticker
            ORDER BY data
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 4) AS media_movel_7d,

        -- Média móvel de 30 dias (tendência longa)
        ROUND(AVG(fechamento) OVER (
            PARTITION BY ticker
            ORDER BY data
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 4) AS media_movel_30d

    FROM silver
)

SELECT
    preco_id,
    ticker,
    data,
    abertura,
    fechamento,
    maxima,
    minima,
    volume,
    retorno_pct,
    media_movel_7d,
    media_movel_30d,
    processado_em

FROM com_metricas
WHERE retorno_pct IS NOT NULL  