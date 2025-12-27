with source as (

    select
        c.id as customer_id,
        e.id as employee_id,
        pod.purchase_order_id,
        pod.product_id,
        pod.quantity,
        pod.unit_cost,
        to_date(pod.date_received) as date_received,
        pod.posted_to_inventory,
        pod.inventory_id,
        po.supplier_id,
        po.created_by,
        to_date(po.submitted_date) as submitted_date,
        to_date(po.creation_date) as creation_date,
        po.status_id,
        to_date(po.expected_date) as expected_date,
        po.shipping_fee,
        po.taxes,
        to_date(po.payment_date) as payment_date,
        po.payment_amount,
        po.payment_method,
        po.notes,
        po.approved_by,
        po.approved_date,
        po.submitted_by,
        current_timestamp as insertion_timestamp

    from {{ ref("stg_purchase_orders") }} po
    left join {{ ref("stg_purchase_order_details") }} pod
        on pod.purchase_order_id = po.id
    left join {{ ref("stg_products") }} p
        on p.id = pod.product_id
    left join {{ ref("stg_order_details") }} od
        on od.product_id = p.id
    left join {{ ref("stg_orders") }} o
        on o.id = od.order_id
    left join {{ ref("stg_employees") }} e
        on e.id = po.created_by
    left join {{ ref("stg_customer") }} c
        on c.id = o.customer_id
    where o.customer_id is not null                 
),

unique_source as (
    select
        *,
        row_number() over(
            partition by
            customer_id,
            employee_id,
            purchase_order_id,
            product_id,
            inventory_id,
            supplier_id,
            creation_date
        order by creation_date
        ) as rn
    from source    
)

select
        customer_id,
        employee_id,
        purchase_order_id,
        product_id,
        quantity,
        unit_cost,
        date_received,
        posted_to_inventory,
        inventory_id,
        supplier_id,
        created_by,
        submitted_date,
        creation_date,
        status_id,
        expected_date,
        shipping_fee,
        taxes,
        payment_date,
        payment_amount,
        payment_method,
        notes,
        approved_by,
        approved_date,
        submitted_by,
        insertion_timestamp
        
from unique_source
where  rn = 1