set enable_incremental to off;
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;
set enable_dbtoaster to on;

set tpch_updates to 'part,lineitem';
set dbt_query to 'q19';

set tpch_delta_mode to :v_mode; 
set bd_prob to :v_prob; 

select
    q19_linenumber, sum(q19_extendedprice * (1 - q19_discount) ) as revenue
from
    q19
group by 
    q19_linenumber;
