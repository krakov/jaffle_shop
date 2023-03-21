with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),


customer_payments as (

    select
        orders.customer_id,
        sum(amount) as total_amount

    from payments

    left join orders on
         payments.order_id = orders.order_id

    group by orders.customer_id

),

-- note building here per each payment method the distinct customers in it
-- this is done to calc avg revenue per customer per payment methods
-- if just joining payments -> orders -> customer_payments then there is row duplication 
-- (same customer appears few times per payment method) if an order is split between payment methods, causing skew!
distinct_payment_method_customers as (
    select
        payment_method,
        orders.customer_id

    from payments

    left join orders on
         payments.order_id = orders.order_id

    group by payment_method, orders.customer_id
),

-- this is per payment method, deduplicated per customer (to allow avg rev per customer)
payments_arpu_aggregated as (

    select
        payments.payment_method,
        avg(customer_payments.total_amount) as arpu

    from payments

    left join distinct_payment_method_customers on
         payments.payment_method = distinct_payment_method_customers.payment_method

    left join customer_payments on
        distinct_payment_method_customers.customer_id = customer_payments.customer_id

    group by payments.payment_method

),

-- this is per payment method, not deduplicated per customer (to allow total revenue)
payments_revenue_aggregated as (
    select
        payment_method,
        sum(amount) as total_amount
    from payments
    group by payments.payment_method
),


final as (

    select
        payments_arpu_aggregated.payment_method,
        payments_revenue_aggregated.total_amount,
        payments_arpu_aggregated.arpu

    from payments_arpu_aggregated

    left join payments_revenue_aggregated 
    on payments_arpu_aggregated.payment_method = payments_revenue_aggregated.payment_method

)

select * from final
