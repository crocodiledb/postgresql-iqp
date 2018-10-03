set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set memory_budget to 1500000;
set decision_method to dp;

set tpch_delta_mode to uniform; 
set bd_prob to 0.99; 

set enable_incremental to on;
set tpch_updates to 'customer';
set iqp_query to 'q3';
set gen_mem_info to off; 

select
	i3_l_orderkey,
    sum(i3_l_extendedprice*(1-i3_l_discount)) as revenue,
	i3_o_orderdate, 
	i3_o_shippriority
from
    i3_c,
	i3_o,
	i3_l
where
	i3_c_custkey = i3_o_custkey
	and i3_l_orderkey = i3_o_orderkey
group by
	i3_l_orderkey,
	i3_o_orderdate,
	i3_o_shippriority
order by
	revenue desc,
	i3_o_orderdate;


