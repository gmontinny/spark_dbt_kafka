{{
    config(
        materialized = 'view',
        description  = 'Pagamentos normalizados e desduplicados — Camada Silver.'
    )
}}

/*
  Fonte: bronze_order_payments (Iceberg)

  PK composta: (order_id, payment_sequential)
    Um pedido pode ter múltiplos pagamentos (ex: cartão + voucher).
    payment_sequential numera cada forma de pagamento dentro do pedido.

  Normalização:
    - payment_type: LOWER + TRIM para padronizar valores como "Credit_Card" → "credit_card"
    - payment_installments: COALESCE para 1 (pagamento à vista quando ausente)
    - payment_value: DECIMAL(10,2) com COALESCE para 0
*/

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

SELECT
    order_id,
    CAST(payment_sequential AS INTEGER)                                     AS payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    ingestion_timestamp
FROM deduped
WHERE row_num = 1
