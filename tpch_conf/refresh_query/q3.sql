set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set memory_budget to 1500000;
set decision_method to dp;

set enable_incremental to off;
set tpch_updates to 'lineitem';
set iqp_query to 'q3';
set gen_mem_info to off; 

insert into q3_refresh 
select 	
	l_orderkey, 
	sum(l_extendedprice*(1-l_discount)) as revenue, 
	o_orderdate, 
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'BUILDING'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < '1995-03-15'
	and l_shipdate > '1995-03-15'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate; 
