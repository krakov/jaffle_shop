-- Honeydew generated model

-- References
-- {{ ref('stg_orders') }}
-- {{ ref('stg_customers') }}
-- {{ ref('stg_payments') }}

{{ config(materialized="table")}}
{{ get_honeydew_entity_sql('payments') }}
