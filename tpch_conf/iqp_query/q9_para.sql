set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set decision_method to dp;
set tpch_updates to 'supplier,part,partsupp,orders,lineitem';

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;

set geqo to off;
set enable_sort to off;

set memory_budget to :v_budget;
set tpch_delta_mode to :v_mode; 
set bd_prob to :v_prob; 

set enable_incremental to on;
set iqp_query to 'q9';
set gen_mem_info to off; 

select
    nation,
    o_year,
    sum(i9_l_extendedprice * (1 - i9_l_discount) - i9_ps_supplycost * i9_l_quantity) as sum_profit
from (
    select
        i9_n_name as nation,
        extract(year from i9_o_orderdate) as o_year,
        i9_l_extendedprice,
	i9_l_discount,
	i9_ps_supplycost,
	i9_l_quantity
    from
        i9_p,
        i9_s,
        i9_l,
        i9_ps,
        i9_o,
        i9_n
    where
        i9_s_suppkey = i9_l_suppkey
        and i9_ps_suppkey = i9_l_suppkey
        and i9_ps_partkey = i9_l_partkey
        and i9_p_partkey = i9_l_partkey
        and i9_o_orderkey = i9_l_orderkey
        and i9_s_nationkey = i9_n_nationkey
    ) as profit
group by
    nation,
    o_year
--order by
--    nation,
--   o_year desc;
