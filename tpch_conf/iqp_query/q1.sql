set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set memory_budget to 3000;
set decision_method to dp;

set tpch_delta_mode to uniform; 
set bd_prob to 0.99; 

set enable_incremental to on;
set tpch_updates to 'lineitem';
set iqp_query to 'q1';
set gen_mem_info to off; 

select
	i1_l_returnflag,
	i1_l_linestatus,
	sum(i1_l_quantity) as sum_qty,
	sum(i1_l_extendedprice) as sum_base_price,
	sum(i1_l_extendedprice*(1-i1_l_discount)) as sum_disc_price,
	sum(i1_l_extendedprice*(1-i1_l_discount)*(1+i1_l_tax)) as sum_charge,
	avg(i1_l_quantity) as avg_qty,
	avg(i1_l_extendedprice) as avg_price,
	avg(i1_l_discount) as avg_disc,
	count(*) as count_order
from
	i1_l
group by
	i1_l_returnflag,
	i1_l_linestatus
order by
	i1_l_returnflag,
	i1_l_linestatus;


