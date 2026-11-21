{{
    config(
        materialized = 'view',
        description  = 'Vendedores normalizados e desduplicados — Camada Silver.'
    )
}}

{% if var('pipeline', 'streaming') == 'batch' %}

WITH source AS (
    SELECT * FROM {{ source('raw_olist', 'sellers') }}
),

deduped AS (
    SELECT
        seller_id,
        seller_zip_code_prefix,
        LOWER(TRIM(seller_city))    AS seller_city,
        UPPER(TRIM(seller_state))   AS seller_state,
        CAST(NULL AS VARCHAR)       AS ingestion_timestamp,
        ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY seller_id) AS row_num
    FROM source
    WHERE seller_id IS NOT NULL
)

{% else %}

WITH source AS (
    SELECT * FROM {{ source('iceberg_bronze', 'bronze_sellers') }}
),

deduped AS (
    SELECT
        seller_id,
        seller_zip_code_prefix,
        LOWER(TRIM(seller_city))    AS seller_city,
        UPPER(TRIM(seller_state))   AS seller_state,
        ingestion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY seller_id
            ORDER BY CAST(kafka_offset AS BIGINT) DESC
        )                           AS row_num
    FROM source
    WHERE seller_id IS NOT NULL
)

{% endif %}

SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    ingestion_timestamp
FROM deduped
WHERE row_num = 1
