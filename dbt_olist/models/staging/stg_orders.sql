{{
    config(
        materialized = 'view',
        description  = 'Pedidos limpos, tipados e desduplicados — Camada Silver.'
    )
}}

{% if var('pipeline', 'streaming') == 'batch' %}

WITH source AS (
    SELECT * FROM {{ source('raw_olist', 'orders') }}
),

deduped AS (
    SELECT
        order_id,
        customer_id,
        LOWER(TRIM(order_status))                                       AS order_status,
        TRY_CAST(order_purchase_timestamp    AS TIMESTAMP)              AS order_purchased_at,
        TRY_CAST(order_approved_at           AS TIMESTAMP)              AS order_approved_at,
        TRY_CAST(order_delivered_carrier_date   AS TIMESTAMP)           AS order_delivered_carrier_at,
        TRY_CAST(order_delivered_customer_date  AS TIMESTAMP)           AS order_delivered_customer_at,
        TRY_CAST(order_estimated_delivery_date  AS TIMESTAMP)           AS order_estimated_delivery_at,
        CAST(NULL AS VARCHAR)                                           AS ingestion_timestamp,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_id)    AS row_num
    FROM source
    WHERE order_id IS NOT NULL
      AND TRIM(order_id) <> ''
)

{% else %}

WITH source AS (
    SELECT * FROM {{ source('iceberg_bronze', 'bronze_orders') }}
),

deduped AS (
    SELECT
        order_id,
        customer_id,
        LOWER(TRIM(order_status))                                       AS order_status,
        TRY_CAST(order_purchase_timestamp    AS TIMESTAMP)              AS order_purchased_at,
        TRY_CAST(order_approved_at           AS TIMESTAMP)              AS order_approved_at,
        TRY_CAST(order_delivered_carrier_date   AS TIMESTAMP)           AS order_delivered_carrier_at,
        TRY_CAST(order_delivered_customer_date  AS TIMESTAMP)           AS order_delivered_customer_at,
        TRY_CAST(order_estimated_delivery_date  AS TIMESTAMP)           AS order_estimated_delivery_at,
        ingestion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY CAST(kafka_offset AS BIGINT) DESC
        )                                                               AS row_num
    FROM source
    WHERE order_id IS NOT NULL
      AND TRIM(order_id) <> ''
)

{% endif %}

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchased_at,
    CAST(order_purchased_at AS DATE)                                    AS order_date,
    order_approved_at,
    order_delivered_carrier_at,
    order_delivered_customer_at,
    order_estimated_delivery_at,
    CASE WHEN order_status = 'delivered' THEN TRUE ELSE FALSE END       AS is_delivered,
    CASE
        WHEN order_delivered_customer_at IS NOT NULL
         AND order_purchased_at IS NOT NULL
        THEN DATE_DIFF('day', order_purchased_at, order_delivered_customer_at)
    END                                                                 AS delivery_days,
    ingestion_timestamp
FROM deduped
WHERE row_num = 1
