Insert into i9_s select s_nationkey, s_suppkey from supplier; 

Insert into i9_p select p_partkey from part where p_name like '%green%';	

Insert into i9_ps select ps_partkey, ps_suppkey, ps_supplycost from partsupp; 	

Insert into i9_o select o_orderkey, o_orderdate from orders; 

Insert into i9_l select l_suppkey, l_partkey, l_orderkey, l_quantity, l_extendedprice, l_discount from lineitem; 	

Insert into i9_n select n_nationkey, n_name from nation; 
