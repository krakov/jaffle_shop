with orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),

final as (

    select
        orders.order_date,
        payments.payment_method,
        sum(payments.amount) as total_amount

    from payments

    left join orders on
         payments.order_id = orders.order_id

    group by orders.order_date, payments.payment_method

)

select * from final
