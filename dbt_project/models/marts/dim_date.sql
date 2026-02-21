
with date_spine as (
    select
        range as date_day
    from range(date '2010-01-01', date '2030-01-01', interval 1 day)
)
select
    cast(strftime(date_day, '%Y%m%d') as integer)   as date_key,
    date_day                                         as date,
    extract('year'    from date_day)::integer        as year,
    extract('month'   from date_day)::integer        as month,
    extract('quarter' from date_day)::integer        as quarter,
    isodow(date_day)::integer                        as day_of_week,  -- 1=Mon … 7=Sun
    isodow(date_day) in (6, 7)                       as is_weekend
from date_spine
