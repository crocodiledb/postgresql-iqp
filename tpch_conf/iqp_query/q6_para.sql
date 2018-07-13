set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set decision_method to dp;

set enable_incremental to on;
set tpch_updates to 'lineitem';
set iqp_query to 'q6';
set gen_mem_info to off; 

set memory_budget to :v_budget;
set tpch_delta_mode to :v_mode; 
set bd_prob to :v_prob;

select
	i6_l_linenumber, sum(i6_l_extendedprice*i6_l_discount) as revenue
from
	i6_l
group by
    i6_l_linenumber;


