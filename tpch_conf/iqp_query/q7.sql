set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set memory_budget to 1500000;
set decision_method to dp;
set tpch_updates to 'supplier,lineitem,orders,customer';

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;

set geqo to off;

set enable_incremental to on;
set iqp_query to 'q7';
set gen_mem_info to off; 

select
	supp_nation,
	cust_nation,
	l_year, 
	sum(i7_l_extendedprice * (1 - i7_l_discount)) as revenue
from (
	select
		i7_n1_name1 as supp_nation,
		i7_n2_name2 as cust_nation,
		extract(year from i7_l_shipdate) as l_year,
		i7_l_extendedprice, i7_l_discount
	from
		i7_s,
		i7_l,
		i7_o,
		i7_c,
		i7_n1,
		i7_n2
	where
		i7_s_suppkey = i7_l_suppkey
		and i7_o_orderkey = i7_l_orderkey
		and i7_c_custkey = i7_o_custkey
		and i7_s_nationkey = i7_n1_nationkey
		and i7_c_nationkey = i7_n2_nationkey
    ) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year;
