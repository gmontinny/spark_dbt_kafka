{{
    config(
        materialized = 'view',
        description  = 'Pedidos limpos, tipados e desduplicados — Camada Silver.'
    )
}}

/*
  Fonte: bronze_orders (Iceberg, gerada pelo streaming Kafka → Spark)

  Deduplicação:
    Usa kafka_offset DESC como critério de ordenação dentro da Window Function.
    O offset é monotonicamente crescente por partição Kafka, portanto o maior
    offset representa a versão mais recente do evento para aquele order_id.
    Isso é mais confiável que order_purchase_timestamp (string, pode ser nulo).

  Transformações:
    - TRY_CAST em todos os timestamps: retorna NULL em vez de falhar em datas malformadas
    - LOWER(TRIM()) no status: normaliza variações de case e espaços
    - CAST(order_purchased_at AS DATE): materializa a data para particionamento nos marts
    - is_delivered: flag booleana para filtros frequentes nos marts
*/

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
        kafka_offset,
        ingestion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY CAST(kafka_offset AS BIGINT) DESC
        )                                                               AS row_num
    FROM source
    WHERE order_id IS NOT NULL
      AND TRIM(order_id) <> ''
)

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
