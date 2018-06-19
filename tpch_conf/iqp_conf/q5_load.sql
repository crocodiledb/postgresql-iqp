insert into i5_c select c_nationkey, c_custkey from customer;	

insert into i5_s select s_nationkey, s_suppkey from supplier;

insert into i5_o select o_custkey, o_orderkey from orders where o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year; 

insert into i5_l select l_orderkey, l_suppkey, l_extendedprice, l_discount from lineitem; 

insert into i5_n select n_regionkey, n_nationkey, n_name from nation; 

insert into i5_r select r_regionkey from region where r_name = 'ASIA';
