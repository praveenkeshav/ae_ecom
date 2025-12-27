with source as (

    select

        od.order_id,
        od.product_id,
        o.customer_id,
        o.employee_id,
        o.shipper_id,
        od.quantity,
        od.unit_price,
        od.discount,
        od.status_id,
        od.date_allocated,
        od.purchase_order_id,
        od.inventory_id,
        to_date(o.order_date) as order_date,
        to_date(o.shipped_date) as shipped_date,
        to_date(o.paid_date) as paid_date,
        current_timestamp as insertion_timestamp
    from {{ ref('stg_orders') }} o
    left join {{ ref('stg_order_details') }} od
        on od.order_id = o.id
    where od.order_id is not null        

),

unique_source as (

    select
        *,
        row_number() over(
            partition by customer_id, employee_id, order_id, product_id,
            shipper_id, purchase_order_id, inventory_id, order_date
            order by insertion_timestamp) as rn
    from source
)

select
        order_id,
        product_id,
        customer_id,
        employee_id,
        shipper_id,
        quantity,
        unit_price,
        discount,
        status_id,
        date_allocated,
        purchase_order_id,
        inventory_id,
        order_date,
        shipped_date,
        paid_date,
        insertion_timestamp
from unique_source
where rn = 1