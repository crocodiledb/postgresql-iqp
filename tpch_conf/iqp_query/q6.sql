set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set memory_budget to 3000;
set decision_method to dp;

set tpch_delta_mode to uniform; 
set bd_prob to 0.99; 

set enable_incremental to on;
set tpch_updates to 'lineitem';
set iqp_query to 'q6';
set gen_mem_info to off; 

select
	i6_l_linenumber, sum(i6_l_extendedprice*i6_l_discount) as revenue
from
	i6_l
group by
    i6_l_linenumber;


