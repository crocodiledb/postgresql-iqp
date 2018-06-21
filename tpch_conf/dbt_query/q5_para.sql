set enable_incremental to off;
set tpch_updates to 'supplier,customer,orders,lineitem';
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;
set dbt_query to 'q5';
set enable_dbtoaster to on;

set tpch_delta_mode to :v_mode; 
set bd_prob to :v_prob; 

select
    q5_name,
    sum(q5_extendedprice * (1 - q5_discount)) as revenue
from
    q5
group by
    q5_name
order by
    revenue desc;
