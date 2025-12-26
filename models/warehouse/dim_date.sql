with date_spine as (

    {{ dbt_utils.date_spine(
        datepart   = "day",
        start_date = "to_date('2014-01-01')",
        end_date   = "to_date('2050-01-01')"
    ) }}
),

dim_date as (

    select

        to_varchar(date_day, 'YYYY-MM-DD') as id,
        date_day as full_day,
        year(date_day) as year,
        week(date_day) as year_week,
        dayofyear(date_day) as year_day,
        year(date_day) as fiscal_year,
        quarter(date_day) as fiscal_qtr,
        month(date_day) as month,
        monthname(date_day) as month_name,
        dayofweek(date_day) as week_day,
        dayname(date_day) as day_name,
        case
          when dayofweek(date_day) in (0, 6) then 0
          else 1
        end as day_is_weekday

    from date_spine
)

select * from dim_date
order by full_day