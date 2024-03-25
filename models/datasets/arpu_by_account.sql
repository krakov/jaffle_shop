-- Honeydew generated model

-- {{ ref('orders') }} {{ ref('customers') }}

{{ config(materialized="table")}}
{{ get_honeydew_dataset_sql('arpu_by_account') }}
