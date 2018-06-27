delete from i7_c;
Insert into i7_c select c_nationkey, c_custkey from customer; 

delete from i7_s;
Insert into i7_s select s_nationkey, s_suppkey from supplier;	

delete from i7_o;
Insert into i7_o select o_custkey, o_orderkey from orders; 	

delete from i7_l;
Insert into i7_l select l_suppkey, l_orderkey, l_shipdate, l_extendedprice, l_discount from lineitem where l_shipdate between date '1995-01-01' and date '1996-12-31';

delete from i7_n2;
Insert into i7_n2 select n_nationkey, n_name from nation where n_name = 'FRANCE' or n_name = 'GERMANY';

delete from i7_n1;
Insert into i7_n1 select n_nationkey, n_name from nation where n_name = 'FRANCE' or n_name = 'GERMANY';
