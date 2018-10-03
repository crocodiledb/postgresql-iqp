set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set memory_budget to 0;
set decision_method to dp;

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;

set geqo to off;

set enable_incremental to on;
set tpch_updates to 'part,supplier,lineitem,orders,customer';
set iqp_query to 'q8';
set gen_mem_info to off; 

select
    nation,
    o_year,
    sum(i8_l_extendedprice * (1-i8_l_discount))
from (
	select
		extract(year from i8_o_orderdate) as o_year,
		i8_l_extendedprice,
		i8_l_discount,
		i8_n2_name as nation
	from
		i8_p,
		i8_s,
		i8_l,
		i8_o,
		i8_c,
		i8_r_n,
		i8_n2
	where
		i8_p_partkey = i8_l_partkey
		and i8_s_suppkey = i8_l_suppkey
		and i8_l_orderkey = i8_o_orderkey
		and i8_o_custkey = i8_c_custkey
		and i8_c_nationkey = i8_r_n_nationkey
		and i8_s_nationkey = i8_n2_nationkey
	) as all_nations
group by
	nation,
	o_year
order by
	nation,
	o_year;
