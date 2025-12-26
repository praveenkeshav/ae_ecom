with source as(    
    select
        p.id as product_id,
        p.product_code,
        p.product_name,
        p.description,
        s.company as supplier_company,
        p.standard_cost,
        p.list_price,
        p.reorder_level,
        p.target_level,
        p.quantity_per_unit,
        p.discontinued,
        p.minimum_reorder_quantity,
        p.category,
        p.attachments,
        current_timestamp as insertion_timestamp
    from {{ ref('stg_products') }} p
    left join {{ ref('stg_suppliers') }} s
        on s.id = p.supplier_id
),

deduped as (

    select 
        *,
        row_number() over(
            partition by product_id 
            order by product_id) 
        as rn
    from source
),

final as (

select
    product_id,
    product_code,
    product_name,
    description,
    supplier_company,
    standard_cost,
    list_price,
    reorder_level,
    target_level,
    quantity_per_unit,
    discontinued,
    minimum_reorder_quantity,
    category,
    attachments,
    insertion_timestamp

from deduped
where rn = 1

)

select * from final