{{
    config(
        materialized = 'view',
        description  = 'Itens de pedido tipados e desduplicados — Camada Silver.'
    )
}}

/*
  Fonte: bronze_order_items (Iceberg)

  PK composta: (order_id, order_item_id)
    Um pedido pode ter múltiplos itens numerados sequencialmente (order_item_id = 1, 2, 3...).
    A deduplicação deve preservar a combinação única de pedido + número do item.

  Transformações:
    - CAST para DECIMAL(10,2) em price e freight_value: evita erros de arredondamento
    - COALESCE(..., 0): garante que campos monetários nunca sejam NULL nos marts
    - total_item_value: coluna derivada para evitar recálculo nos marts
*/

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
