set enable_incremental to off;
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;
set enable_dbtoaster to on;

set tpch_updates to 'orders,lineitem';
set dbt_query to 'q12';

set tpch_delta_mode to :v_mode; 
set bd_prob to :v_prob; 

select
    q12_shipmode,
    sum(case
        when q12_orderpriority ='1-URGENT'
        or q12_orderpriority ='2-HIGH'
        then 1
        else 0
        end) as high_line_count,
    sum(case
        when q12_orderpriority <> '1-URGENT'
        and q12_orderpriority <> '2-HIGH'
        then 1
        else 0
        end) as low_line_count
from
    q12
group by
    q12_shipmode
order by
    q12_shipmode;
