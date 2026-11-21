{{
    config(
        materialized = 'view',
        description  = 'Itens de pedido tipados e desduplicados — Camada Silver.'
    )
}}

{% if var('pipeline', 'streaming') == 'batch' %}

WITH source AS (
    SELECT * FROM {{ source('raw_olist', 'order_items') }}
),

deduped AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        TRY_CAST(shipping_limit_date AS TIMESTAMP)                          AS shipping_limit_at,
        CAST(COALESCE(CAST(price AS VARCHAR), '0')         AS DECIMAL(10, 2)) AS price,
        CAST(COALESCE(CAST(freight_value AS VARCHAR), '0') AS DECIMAL(10, 2)) AS freight_value,
        CAST(NULL AS VARCHAR)                                               AS ingestion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, order_item_id
            ORDER BY order_id
        )                                                                   AS row_num
    FROM source
    WHERE order_id IS NOT NULL
      AND order_item_id IS NOT NULL
      AND product_id IS NOT NULL
)

{% else %}

WITH source AS (
    SELECT * FROM {{ source('iceberg_bronze', 'bronze_order_items') }}
),

deduped AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        TRY_CAST(shipping_limit_date AS TIMESTAMP)                          AS shipping_limit_at,
        CAST(COALESCE(price, '0')         AS DECIMAL(10, 2))                AS price,
        CAST(COALESCE(freight_value, '0') AS DECIMAL(10, 2))                AS freight_value,
        ingestion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, order_item_id
            ORDER BY CAST(kafka_offset AS BIGINT) DESC
        )                                                                   AS row_num
    FROM source
    WHERE order_id IS NOT NULL
      AND order_item_id IS NOT NULL
      AND product_id IS NOT NULL
)

{% endif %}

SELECT
    order_id,
    CAST(order_item_id AS INTEGER)                                          AS order_item_id,
    product_id,
    seller_id,
    shipping_limit_at,
    price,
    freight_value,
    CAST(price + freight_value AS DECIMAL(10, 2))                           AS total_item_value,
    ingestion_timestamp
FROM deduped
WHERE row_num = 1
