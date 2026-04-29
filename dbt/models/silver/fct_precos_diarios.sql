WITH bronze AS (
    SELECT * FROM {{ ref('stg_precos') }}
    WHERE fechamento IS NOT NULL
      AND data <= CURRENT_DATE
)

SELECT
    -- Chave única: identifica cada linha sem ambiguidade
    ticker || '_' || CAST(data AS VARCHAR) AS preco_id,

    ticker,
    data,
    abertura,
    fechamento,
    maxima,
    minima,
    volume,
    extraido_em,
    CURRENT_TIMESTAMP AS processado_em

FROM bronze