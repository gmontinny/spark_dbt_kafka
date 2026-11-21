{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'merge',
        unique_key           = 'customer_unique_id',
        on_schema_change     = 'sync_all_columns',
        post_hook = [
            "ALTER TABLE {{ this }} EXECUTE optimize(file_size_threshold => '32MB')"
        ]
    )
}}

/*
  dim_customers — Dimensão de Clientes (Gold / Iceberg)

  Granularidade: 1 linha por customer_unique_id (identidade real do cliente).

  Um cliente pode ter múltiplos customer_ids (um por pedido no dataset Olist).
  Consolidamos pelo customer_unique_id para métricas de LTV corretas.

  Métricas calculadas:
    - total_orders: volume de pedidos realizados
    - lifetime_value: receita total gerada pelo cliente
    - avg_order_value: ticket médio
    - delivered_orders: pedidos efetivamente entregues
    - days_since_last_order: recência (útil para segmentação RFM)
*/

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

order_metrics AS (
    SELECT
        customer_id,
        COUNT(*)                                            AS total_orders,
        CAST(SUM(total_payment_value) AS DECIMAL(12, 2))   AS lifetime_value,
        CAST(AVG(total_payment_value) AS DECIMAL(10, 2))   AS avg_order_value,
        MIN(order_purchased_at)                            AS first_order_at,
        MAX(order_purchased_at)                            AS last_order_at,
        SUM(CASE WHEN is_delivered THEN 1 ELSE 0 END)      AS delivered_orders,
        SUM(CASE WHEN NOT is_delivered
                  AND order_status = 'canceled'
                 THEN 1 ELSE 0 END)                        AS canceled_orders
    FROM {{ ref('fct_orders') }}
    GROUP BY customer_id
)

SELECT
    c.customer_unique_id,
    c.customer_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,

    -- Métricas de comportamento
    COALESCE(o.total_orders, 0)                             AS total_orders,
    COALESCE(o.lifetime_value, 0)                           AS lifetime_value,
    COALESCE(o.avg_order_value, 0)                          AS avg_order_value,
    COALESCE(o.delivered_orders, 0)                         AS delivered_orders,
    COALESCE(o.canceled_orders, 0)                          AS canceled_orders,

    -- Datas de atividade
    o.first_order_at,
    o.last_order_at,
    DATE_DIFF('day', o.last_order_at, CURRENT_TIMESTAMP)    AS days_since_last_order,

    -- Controle dbt
    CURRENT_TIMESTAMP                                       AS dbt_updated_at

FROM customers c
LEFT JOIN order_metrics o ON c.customer_id = o.customer_id
