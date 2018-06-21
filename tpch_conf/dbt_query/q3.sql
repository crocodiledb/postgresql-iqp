set enable_incremental to off;
set tpch_updates to 'lineitem';
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;
set dbt_query to 'q3';
set enable_dbtoaster to on;

set tpch_delta_mode to uniform; 
set bd_prob to 0.9; 

select
	q3_orderkey, 
	sum(q3_extendedprice*(1-q3_discount)) as revenue, 
	q3_orderdate, 
	q3_shippriority
from
    q3
group by
	q3_orderkey,
	q3_orderdate,
	q3_shippriority
order by
	revenue desc,
	q3_orderdate; 
