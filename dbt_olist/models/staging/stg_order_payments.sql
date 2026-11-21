{{
    config(
        materialized = 'view',
        description  = 'Pagamentos normalizados e desduplicados — Camada Silver.'
    )
}}

{% if var('pipeline', 'streaming') == 'batch' %}

WITH source AS (
    SELECT * FROM {{ source('raw_olist', 'order_payments') }}
),

deduped AS (
    SELECT
        order_id,
        payment_sequential,
        LOWER(TRIM(payment_type))                                           AS payment_type,
        CAST(COALESCE(payment_installments, 1) AS INTEGER)                  AS payment_installments,
        CAST(COALESCE(payment_value, 0.0)      AS DECIMAL(10, 2))           AS payment_value,
        CAST(NULL AS VARCHAR)                                               AS ingestion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, payment_sequential
            ORDER BY order_id
        )                                                                   AS row_num
    FROM source
    WHERE order_id IS NOT NULL
      AND payment_sequential IS NOT NULL
)

{% else %}

WITH source AS (
    SELECT * FROM {{ source('iceberg_bronze', 'bronze_order_payments') }}
),

deduped AS (
    SELECT
        order_id,
        payment_sequential,
        LOWER(TRIM(payment_type))                                           AS payment_type,
        CAST(COALESCE(payment_installments, '1') AS INTEGER)                AS payment_installments,
        CAST(COALESCE(payment_value, '0')        AS DECIMAL(10, 2))         AS payment_value,
        ingestion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, payment_sequential
            ORDER BY CAST(kafka_offset AS BIGINT) DESC
        )                                                                   AS row_num
    FROM source
    WHERE order_id IS NOT NULL
      AND payment_sequential IS NOT NULL
)

{% endif %}

SELECT
    order_id,
    CAST(payment_sequential AS INTEGER)                                     AS payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    ingestion_timestamp
FROM deduped
WHERE row_num = 1
