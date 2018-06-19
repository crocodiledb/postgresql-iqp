set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set memory_budget to 1500000;
set decision_method to dp;
set tpch_updates to 'customer,orders,lineitem,supplier';

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;

set geqo to off;

set enable_incremental to on;
set iqp_query to 'q5';
set gen_mem_info to off; 


select
	i5_n_name,
	sum(i5_l_extendedprice * (1 - i5_l_discount)) as revenue
from
	i5_c,
	i5_o,
	i5_l,
	i5_s,
	i5_n,
	i5_r
where
	i5_c_custkey = i5_o_custkey
	and i5_l_orderkey = i5_o_orderkey
	and i5_l_suppkey = i5_s_suppkey
	and i5_c_nationkey = i5_s_nationkey
	and i5_s_nationkey = i5_n_nationkey
	and i5_n_regionkey = i5_r_regionkey
group by
	i5_n_name
order by
	revenue desc;
