set enable_incremental to off;
set tpch_updates to 'lineitem';
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;
set dbt_query to 'q9';
set enable_dbtoaster to on;

set tpch_delta_mode to uniform; 
set bd_prob to 0.9; 

select
    q9_name,
    q9_year,
    sum(q9_extendedprice * (1 - q9_discount) - q9_supplycost * q9_quantity) as sum_profit
from
    q9
group by
    q9_name,
    q9_year
--order by
--    q9_name,
--   q9_year desc;

