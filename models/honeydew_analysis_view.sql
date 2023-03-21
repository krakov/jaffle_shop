-- Query for Curated View analysis generated by Honeydew at 2023-02-20 16:19:03
WITH
-- Entity Node for customers
-- Bring attributes [customer_id] from dataset raw_customers
node_customers_1 AS (
SELECT
    ID AS customers__customer_id
FROM {{ ref('raw_customers') }}

),

-- Entity Node for orders
-- Bring attributes [customer_id,order_id] from dataset raw_orders
node_orders_4 AS (
SELECT
    USER_ID AS orders__customer_id,
    ID AS orders__order_id
FROM {{ ref('raw_orders') }}

),

-- Entity Node for orders
-- Join to entities [customers]
node_orders_5 AS (
SELECT
    node_orders_4.orders__customer_id,
    node_orders_4.orders__order_id,
    node_customers_1.customers__customer_id
FROM node_orders_4
INNER JOIN node_customers_1 ON (node_orders_4.orders__customer_id = node_customers_1.customers__customer_id)
),

-- Entity Node for payments
-- Bring attributes [amount,order_id,payment_id,payment_method] from dataset raw_payments
node_payments_8 AS (
SELECT
    AMOUNT AS payments__amount,
    ORDER_ID AS payments__order_id,
    ID AS payments__payment_id,
    PAYMENT_METHOD AS payments__payment_method
FROM {{ ref('raw_payments') }}

),

-- Entity Node for payments
-- Add attributes [amount_usd]
-- Join to entities [orders,customers]
node_payments_9 AS (
SELECT
    node_payments_8.payments__amount /100 AS payments__amount_usd,
    node_payments_8.payments__order_id,
    node_payments_8.payments__payment_id,
    node_payments_8.payments__payment_method,
    node_orders_5.orders__order_id,
    node_orders_5.customers__customer_id
FROM node_payments_8
INNER JOIN node_orders_5 ON (node_payments_8.payments__order_id = node_orders_5.orders__order_id)
),

-- Add metrics [payments.total_amount] on groups [customers.customer_id]
-- Aggregation granularity is on entity payments
metrics_payments_11 AS (
SELECT
    customers__customer_id,
    sum(node_payments_9.payments__amount_usd ) AS payments__total_amount__11
FROM node_payments_9
GROUP BY 1),

-- Entity Node for customers
-- Add attributes [payments__total_amount__by_customers_customer_id]
-- Join to entities []
node_customers_12 AS (
SELECT
    metrics_payments_11.payments__total_amount__11 AS customers__payments__total_amount__by_customers_customer_id,
    node_customers_1.customers__customer_id
FROM node_customers_1
LEFT JOIN metrics_payments_11 ON (node_customers_1.customers__customer_id IS NOT DISTINCT FROM metrics_payments_11.customers__customer_id)
),

-- Entity Node for customers (step #2)
-- Add attributes [total_rev]
node_customers_13 AS (
SELECT
    node_customers_12.customers__payments__total_amount__by_customers_customer_id AS customers__total_rev,
    node_customers_12.customers__customer_id
FROM node_customers_12
),

-- Entity Node for orders (step #2)
-- Join to entities [customers]
node_orders_15 AS (
SELECT
    node_orders_5.orders__order_id,
    node_customers_13.customers__customer_id,
    node_customers_13.customers__total_rev
FROM node_orders_5
INNER JOIN node_customers_13 ON (node_orders_5.orders__customer_id = node_customers_13.customers__customer_id)
),

-- Entity Node for payments (step #2)
-- Join to entities [orders,customers]
node_payments_17 AS (
SELECT
    node_payments_9.payments__payment_id,
    node_payments_9.payments__payment_method,
    node_payments_9.payments__amount_usd,
    node_orders_15.customers__customer_id,
    node_orders_15.customers__total_rev
FROM node_payments_9
INNER JOIN node_orders_15 ON (node_payments_9.payments__order_id = node_orders_15.orders__order_id)
),

-- Add metrics [customers.arpu] on groups [payments.payment_method]
-- Aggregation granularity is on entity customers
metrics_customers_19 AS (
SELECT
    payments__payment_method,
    avg(node_payments_17.customers__total_rev ) AS customers__arpu__19
FROM (
    SELECT
        payments__payment_method,
        ANY_VALUE(customers__total_rev) AS customers__total_rev,
        ANY_VALUE(customers__customer_id) AS customers__customer_id
    FROM node_payments_17
    GROUP BY customers__customer_id, 1) AS node_payments_17
GROUP BY 1),

-- Add metrics [payments.total_amount] on groups [payments.payment_method]
-- Aggregation granularity is on entity payments
metrics_payments_20 AS (
SELECT
    payments__payment_method,
    sum(node_payments_17.payments__amount_usd ) AS payments__total_amount__20
FROM node_payments_17
GROUP BY 1)

-- Final node
SELECT
    metrics_customers_19.payments__payment_method AS "payments.payment_method",
    metrics_customers_19.customers__arpu__19 AS "customers.arpu",
    metrics_payments_20.payments__total_amount__20 AS "payments.total_amount"
FROM metrics_customers_19
LEFT JOIN metrics_payments_20 ON (metrics_customers_19.payments__payment_method IS NOT DISTINCT FROM metrics_payments_20.payments__payment_method)

