DROP TABLE IF EXISTS i7_c CASCADE;
CREATE TABLE i7_c (
    i7_c_nationkey BIGINT, 
    i7_c_custkey    BIGINT
);

DROP TABLE IF EXISTS i7_s CASCADE;
CREATE TABLE i7_s (
    i7_s_nationkey  BIGINT,
	i7_s_suppkey	BIGINT
);

DROP TABLE IF EXISTS i7_o CASCADE;
CREATE TABLE i7_o (
	i7_o_custkey	BIGINT,
	i7_o_orderkey	BIGINT
);

DROP TABLE IF EXISTS i7_l CASCADE;
CREATE TABLE i7_l (
	i7_l_suppkey	BIGINT,
	i7_l_orderkey	BIGINT,
	i7_l_shipdate	date,
	i7_l_extendedprice	decimal,
	i7_l_discount	decimal
);

DROP TABLE IF EXISTS i7_n2 CASCADE;
CREATE TABLE i7_n2 (
    i7_n2_nationkey BIGINT, 
    i7_n2_name2      char(25) 
);

DROP TABLE IF EXISTS i7_n1 CASCADE;
CREATE TABLE i7_n1 (
    i7_n1_nationkey  BIGINT,
	i7_n1_name1     char(25)
);


