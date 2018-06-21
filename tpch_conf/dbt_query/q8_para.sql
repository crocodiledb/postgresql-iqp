set enable_incremental to off;
set tpch_updates to 'supplier,customer,part,orders,lineitem';
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;
set dbt_query to 'q8';
set enable_dbtoaster to on;

set tpch_delta_mode to :v_mode; 
set bd_prob to :v_prob; 

select
    q8_name,
    q8_year,
    sum(q8_extendedprice * (1-q8_discount))
from
    q8
group by
    q8_name,
    q8_year
order by
    q8_name,
    q8_year;
