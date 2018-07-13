set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set decision_method to dp;

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;

set enable_incremental to on;
set tpch_updates to 'part,lineitem';
set iqp_query to 'q14';
set gen_mem_info to off; 

set memory_budget to :v_budget;
set tpch_delta_mode to :v_mode; 
set bd_prob to :v_prob; 

select
	i14_l_linenumber, 100.00 * sum(case
		when i14_p_type like 'PROMO%'
			then i14_l_extendedprice * (1 - i14_l_discount)
		else 0
	end) / sum(i14_l_extendedprice * (1 - i14_l_discount)) as promo_revenue
from
	i14_l,
	i14_p
where
	i14_l_partkey = i14_p_partkey
group by
    	i14_l_linenumber;

