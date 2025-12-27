with source as (

    select

        id as inventory_id,
        transaction_type,
        to_date(transaction_created_date) as transaction_created_date,
        to_date(transaction_modified_date) as transaction_modified_date,
        product_id,
        quantity,
        purchase_order_id,
        customer_order_id,
        comments,
        current_timestamp as insertion_timestamp

    from {{ ref("stg_inventory_transactions")}}
),

unique_source as (

    select
        *,
        row_number() over(
            partition by inventory_id 
            order by product_id,
            quantity, 
            purchase_order_id,
            customer_order_id,
            transaction_type,
            comments
        ) as rn
    from source
)

select
    inventory_id,
    transaction_type,
    transaction_created_date,
    transaction_modified_date,
    product_id,
    quantity,
    purchase_order_id,
    customer_order_id,
    comments,
    insertion_timestamp
from unique_source
where rn = 1