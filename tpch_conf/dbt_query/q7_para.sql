set enable_incremental to off;
set tpch_updates to 'supplier,customer,orders,lineitem';
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;
set dbt_query to 'q7';
set enable_dbtoaster to on;

set tpch_delta_mode to :v_mode; 
set bd_prob to :v_prob; 

select
	q7_name1 as supp_nation,
	q7_name2 as cust_nation,
	q7_year, 
	sum(q7_extendedprice * (1 - q7_discount)) as revenue
from 
	q7
group by
	supp_nation,
	cust_nation,
	q7_year
order by
	supp_nation,
	cust_nation,
	q7_year;
