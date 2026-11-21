{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'merge',
        unique_key           = 'order_id',
        on_schema_change     = 'sync_all_columns',
        post_hook = [
            "ALTER TABLE {{ this }} EXECUTE optimize(file_size_threshold => '32MB')",
            "ALTER TABLE {{ this }} EXECUTE expire_snapshots(retention_threshold => '7d')"
        ]
    )
}}

/*
  fct_orders — Tabela Fato de Pedidos (Gold / Iceberg)

  Granularidade: 1 linha por order_id.

  Merge incremental:
    Na primeira execução (full-refresh), processa todos os pedidos.
    Nas execuções seguintes, filtra apenas pedidos com ingestion_timestamp
    nos últimos 3 dias — janela suficiente para capturar late-arriving data
    (pedidos cujos itens/pagamentos chegaram após o pedido principal).

  Particionamento por month(order_date):
    Permite pruning de partições no Trino para queries mensais/anuais,
    reduzindo drasticamente o volume de dados lidos.

  post_hook:
    - optimize: compacta arquivos pequenos gerados pelo micro-batch do Spark
    - expire_snapshots: remove snapshots Iceberg com mais de 7 dias
*/

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    {% if is_incremental() %}
        WHERE TRY_CAST(ingestion_timestamp AS TIMESTAMP) >= (
            SELECT MAX(TRY_CAST(ingestion_timestamp AS TIMESTAMP)) - INTERVAL '3' DAY
            FROM {{ this }}
        )
    {% endif %}
),

payments_agg AS (
    SELECT
        order_id,
        SUM(payment_value)                              AS total_payment_value,
        COUNT(DISTINCT payment_sequential)              AS payment_count,
        -- Tipo de pagamento principal: maior valor pago
        MAX_BY(payment_type, payment_value)             AS primary_payment_type,
        MAX(payment_installments)                       AS max_installments
    FROM {{ ref('stg_order_payments') }}
    GROUP BY order_id
),

items_agg AS (
    SELECT
        order_id,
        COUNT(*)                                        AS total_items,
        COUNT(DISTINCT product_id)                      AS distinct_products,
        COUNT(DISTINCT seller_id)                       AS distinct_sellers,
        CAST(SUM(price)         AS DECIMAL(12, 2))      AS total_items_price,
        CAST(SUM(freight_value) AS DECIMAL(12, 2))      AS total_freight_value,
        CAST(SUM(total_item_value) AS DECIMAL(12, 2))   AS total_order_value
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
)

SELECT
    -- Chaves
    o.order_id,
    o.customer_id,

    -- Status e datas
    o.order_status,
    o.order_purchased_at,
    o.order_date,
    o.order_approved_at,
    o.order_delivered_customer_at,
    o.order_estimated_delivery_at,
    o.is_delivered,

    -- SLA de entrega
    o.delivery_days,
    CASE
        WHEN o.is_delivered
         AND o.order_delivered_customer_at <= o.order_estimated_delivery_at
        THEN TRUE
        ELSE FALSE
    END                                                 AS delivered_on_time,

    -- Pagamentos
    COALESCE(p.total_payment_value, 0)                  AS total_payment_value,
    COALESCE(p.payment_count, 0)                        AS payment_count,
    p.primary_payment_type,
    COALESCE(p.max_installments, 1)                     AS max_installments,

    -- Itens
    COALESCE(i.total_items, 0)                          AS total_items,
    COALESCE(i.distinct_products, 0)                    AS distinct_products,
    COALESCE(i.distinct_sellers, 0)                     AS distinct_sellers,
    COALESCE(i.total_items_price, 0)                    AS total_items_price,
    COALESCE(i.total_freight_value, 0)                  AS total_freight_value,
    COALESCE(i.total_order_value, 0)                    AS total_order_value,

    -- Controle dbt
    CURRENT_TIMESTAMP                                   AS dbt_updated_at

FROM orders o
LEFT JOIN payments_agg p ON o.order_id = p.order_id
LEFT JOIN items_agg    i ON o.order_id = i.order_id
