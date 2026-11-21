{{
    config(
        materialized = 'view',
        description  = 'Vendedores normalizados e desduplicados — Camada Silver.'
    )
}}

/*
  Fonte: bronze_sellers (Iceberg)

  Deduplicação por seller_id via kafka_offset DESC.

  Normalização:
    - seller_city: LOWER + TRIM
    - seller_state: UPPER + TRIM (UF em maiúsculas para consistência com dim_customers)
*/

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

SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    ingestion_timestamp
FROM deduped
WHERE row_num = 1
