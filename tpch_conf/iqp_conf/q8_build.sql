DROP TABLE IF EXISTS i8_c CASCADE;
CREATE TABLE i8_c (
    i8_c_nationkey  BIGINT,
	i8_c_custkey	BIGINT
);

DROP TABLE IF EXISTS i8_s CASCADE;
CREATE TABLE i8_s (
    i8_s_nationkey  BIGINT,
	i8_s_suppkey	BIGINT
);

DROP TABLE IF EXISTS i8_p CASCADE;
CREATE TABLE i8_p (
	i8_p_partkey	BIGINT
);

DROP TABLE IF EXISTS i8_o CASCADE;
CREATE TABLE i8_o (
	i8_o_custkey	BIGINT,
	i8_o_orderkey	BIGINT,
    i8_o_orderdate  date
);

DROP TABLE IF EXISTS i8_l CASCADE;
CREATE TABLE i8_l (
	i8_l_suppkey	BIGINT,
	i8_l_partkey	BIGINT,
	i8_l_orderkey	BIGINT,
	i8_l_extendedprice	decimal,
	i8_l_discount	decimal
);

DROP TABLE IF EXISTS i8_r_n CASCADE;
CREATE TABLE i8_r_n (
	i8_r_n_nationkey	BIGINT
);

DROP TABLE IF EXISTS i8_n2 CASCADE;
CREATE TABLE i8_n2 (
	i8_n2_nationkey	BIGINT,
	i8_n2_name	char(25)
);

