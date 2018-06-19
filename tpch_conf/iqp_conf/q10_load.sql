Insert into i10_c select c_nationkey, c_custkey, c_name as custname, c_acctbal, c_address, c_phone, c_comment from customer; 

Insert into i10_o select o_custkey, o_orderkey from orders where o_orderdate >= date '1993-10-01' and o_orderdate < date '1993-10-01' + interval '3' month;

Insert into i10_l select l_orderkey, l_extendedprice, l_discount from lineitem where l_returnflag = 'R';

Insert into i10_n select n_nationkey, n_name from nation; 
