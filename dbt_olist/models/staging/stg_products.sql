{{
    config(
        materialized = 'view',
        description  = 'Produtos enriquecidos com categoria em inglês — Camada Silver.'
    )
}}

/*
  Fonte: bronze_products + bronze_category_translation (Iceberg)

  Enriquecimento:
    LEFT JOIN com a tabela de tradução de categorias.
    COALESCE garante que produtos sem tradução mantêm o nome original em PT.

  Deduplicação por product_id via kafka_offset DESC.

  Dimensões físicas:
    Mantidas como DOUBLE para cálculos de frete e logística nos marts.
*/

WITH products AS (
    SELECT
        product_id,
        product_category_name,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        ingestion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY CAST(kafka_offset AS BIGINT) DESC
        )                                                   AS row_num
    FROM {{ source('iceberg_bronze', 'bronze_products') }}
    WHERE product_id IS NOT NULL
),

translations AS (
    SELECT
        product_category_name,
        product_category_name_english
    FROM (
        SELECT
            product_category_name,
            product_category_name_english,
            ROW_NUMBER() OVER (
                PARTITION BY product_category_name
                ORDER BY CAST(kafka_offset AS BIGINT) DESC
            ) AS row_num
        FROM {{ source('iceberg_bronze', 'bronze_category_translation') }}
        WHERE product_category_name IS NOT NULL
    )
    WHERE row_num = 1
)

SELECT
    p.product_id,
    p.product_category_name,
    COALESCE(
        t.product_category_name_english,
        p.product_category_name
    )                                                       AS product_category_name_en,
    CAST(p.product_name_lenght        AS INTEGER)           AS product_name_length,
    CAST(p.product_description_lenght AS INTEGER)           AS product_description_length,
    CAST(p.product_photos_qty         AS INTEGER)           AS product_photos_qty,
    CAST(p.product_weight_g           AS DOUBLE)            AS product_weight_g,
    CAST(p.product_length_cm          AS DOUBLE)            AS product_length_cm,
    CAST(p.product_height_cm          AS DOUBLE)            AS product_height_cm,
    CAST(p.product_width_cm           AS DOUBLE)            AS product_width_cm,
    p.ingestion_timestamp
FROM products p
LEFT JOIN translations t
    ON LOWER(TRIM(p.product_category_name)) = LOWER(TRIM(t.product_category_name))
WHERE p.row_num = 1
