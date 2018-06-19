Insert into i8_c select c_nationkey, c_custkey from customer; 	

Insert into i8_s select s_nationkey, s_suppkey from supplier; 

Insert into i8_p select p_partkey from part where p_type = 'ECONOMY ANODIZED STEEL'; 	

Insert into i8_o select o_custkey, o_orderkey, o_orderdate from orders where o_orderdate between date '1995-01-01' and date '1996-12-31';

Insert into i8_l select l_suppkey, l_partkey, l_orderkey, l_extendedprice, l_discount from lineitem;

Insert into i8_r_n select n_nationkey from region r, nation n where r.r_regionkey = n.n_regionkey and r.r_name = 'AMERICA';

Insert into i8_n2 select n_nationkey, n_name from nation;
