{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'merge',
        unique_key           = 'seller_id',
        on_schema_change     = 'sync_all_columns',
        post_hook = [
            "ALTER TABLE {{ this }} EXECUTE optimize(file_size_threshold => '32MB')"
        ]
    )
}}

/*
  dim_sellers — Dimensão de Vendedores (Gold / Iceberg)

  Granularidade: 1 linha por seller_id.

  Métricas calculadas a partir de stg_order_items:
    - total_orders: pedidos distintos atendidos pelo vendedor
    - total_items_sold: unidades vendidas
    - total_revenue: receita bruta (sem frete)
    - total_freight: frete total cobrado
    - avg_item_price: ticket médio por item
    - distinct_products: variedade do catálogo ativo
*/

WITH sellers AS (
    SELECT * FROM {{ ref('stg_sellers') }}
),

item_metrics AS (
    SELECT
        seller_id,
        COUNT(DISTINCT order_id)                            AS total_orders,
        COUNT(*)                                            AS total_items_sold,
        CAST(SUM(price)         AS DECIMAL(12, 2))          AS total_revenue,
        CAST(SUM(freight_value) AS DECIMAL(12, 2))          AS total_freight,
        CAST(AVG(price)         AS DECIMAL(10, 2))          AS avg_item_price,
        COUNT(DISTINCT product_id)                          AS distinct_products
    FROM {{ ref('stg_order_items') }}
    WHERE seller_id IS NOT NULL
    GROUP BY seller_id
)

SELECT
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,

    -- Métricas de performance
    COALESCE(i.total_orders, 0)                             AS total_orders,
    COALESCE(i.total_items_sold, 0)                         AS total_items_sold,
    COALESCE(i.total_revenue, 0)                            AS total_revenue,
    COALESCE(i.total_freight, 0)                            AS total_freight,
    COALESCE(i.avg_item_price, 0)                           AS avg_item_price,
    COALESCE(i.distinct_products, 0)                        AS distinct_products,

    -- Controle dbt
    CURRENT_TIMESTAMP                                       AS dbt_updated_at

FROM sellers s
LEFT JOIN item_metrics i ON s.seller_id = i.seller_id
