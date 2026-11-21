{{
    config(
        materialized = 'view',
        description  = 'Clientes desduplicados por customer_unique_id — Camada Silver.'
    )
}}

{% if var('pipeline', 'streaming') == 'batch' %}

WITH source AS (
    SELECT * FROM {{ source('raw_olist', 'customers') }}
),

deduped AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        LOWER(TRIM(customer_city))              AS customer_city,
        UPPER(TRIM(customer_state))             AS customer_state,
        CAST(NULL AS VARCHAR)                   AS ingestion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY customer_id
        )                                       AS row_num
    FROM source
    WHERE customer_id IS NOT NULL
      AND customer_unique_id IS NOT NULL
)

{% else %}

WITH source AS (
    SELECT * FROM {{ source('iceberg_bronze', 'bronze_customers') }}
),

deduped AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        LOWER(TRIM(customer_city))              AS customer_city,
        UPPER(TRIM(customer_state))             AS customer_state,
        ingestion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY CAST(kafka_offset AS BIGINT) DESC
        )                                       AS row_num
    FROM source
    WHERE customer_id IS NOT NULL
      AND customer_unique_id IS NOT NULL
)

{% endif %}

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    ingestion_timestamp
FROM deduped
WHERE row_num = 1
