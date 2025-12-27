with source as (

    select
        id as employee_id,
        company,
        last_name,
        first_name,
        email_address,
        job_title,
        business_phone,
        home_phone,
        mobile_phone,
        fax_number,
        address,
        city,
        state_province,
        zip_postal_code,
        country_region,
        web_page,
        notes,
        attachments,
        current_timestamp as insertion_timestamp

    from {{ ref('stg_employees') }}
),

deduped as (

    select 
        *,
        row_number() over(
            partition by employee_id 
            order by employee_id) as rn
    from source
),

final as (

    select
        employee_id,
        company,
        last_name,
        first_name,
        email_address,
        job_title,
        business_phone,
        home_phone,
        mobile_phone,
        fax_number,
        address,
        city,
        state_province,
        zip_postal_code,
        country_region,
        web_page,
        notes,
        attachments,
        current_timestamp as insertion_timestamp

    from deduped
where rn = 1

)

select * from final