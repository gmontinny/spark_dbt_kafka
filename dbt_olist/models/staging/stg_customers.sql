{{
    config(
        materialized = 'view',
        description  = 'Clientes desduplicados por customer_unique_id — Camada Silver.'
    )
}}

/*
  Fonte: bronze_customers (Iceberg)

  Deduplicação por customer_unique_id:
    Um mesmo cliente pode aparecer com customer_ids diferentes em pedidos distintos.
    A identidade real do cliente é customer_unique_id. Mantemos o registro com
    maior kafka_offset (versão mais recente no broker).

  Normalização:
    - customer_city: LOWER + TRIM (dados originais têm variações como "São Paulo" / "sao paulo")
    - customer_state: UPPER + TRIM (garantir UF em maiúsculas para joins com dim_customers)
*/

WITH source AS (
    SELECT * FROM {{ source('iceberg_bronze', 'bronze_customers') }}
),

deduped AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        LOWER(TRIM(customer_city))                  AS customer_city,
        UPPER(TRIM(customer_state))                 AS customer_state,
        ingestion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY CAST(kafka_offset AS BIGINT) DESC
        )                                           AS row_num
    FROM source
    WHERE customer_id IS NOT NULL
      AND customer_unique_id IS NOT NULL
)

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    ingestion_timestamp
FROM deduped
WHERE row_num = 1
