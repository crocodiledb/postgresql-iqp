insert into i3_c select c_custkey from customer_full where c_mktsegment = 'BUILDING';

insert into i3_o select o_custkey, o_orderkey, o_orderdate, o_shippriority from orders_full where o_orderdate < '1995-03-15';

insert into i3_l select l_orderkey, l_extendedprice, l_discount from lineitem_full where l_shipdate > '1995-03-15';



