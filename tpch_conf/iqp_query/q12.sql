set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set memory_budget to 1500000;
set decision_method to dp;

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;

set tpch_delta_mode to uniform; 
set bd_prob to 0.99; 

set enable_incremental to on;
set tpch_updates to 'orders,lineitem';
set iqp_query to 'q12';
set gen_mem_info to off; 

select
    i12_l_shipmode,
    sum(case
        when i12_o_orderpriority ='1-URGENT'
        or i12_o_orderpriority ='2-HIGH'
        then 1
        else 0
        end) as high_line_count,
    sum(case
        when i12_o_orderpriority <> '1-URGENT'
        and i12_o_orderpriority <> '2-HIGH'
        then 1
        else 0
        end) as low_line_count
from
    i12_o,
    i12_l
where
    i12_o_orderkey = i12_l_orderkey
group by
    i12_l_shipmode
order by
    i12_l_shipmode;
