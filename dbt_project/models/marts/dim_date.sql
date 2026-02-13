
with date_spine as (
  select
    range as date_day
  from range(date '2010-01-01', date '2030-01-01', interval 1 day)
)
select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    extract('year' from date_day) as year,
    extract('month' from date_day) as month,
    extract('day' from date_day) as day,
    extract('quarter' from date_day) as quarter,
    dayname(date_day) as day_of_week
from date_spine
