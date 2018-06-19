DROP TABLE IF EXISTS i9_s CASCADE;
CREATE TABLE i9_s (
	i9_s_nationkey	BIGINT,
	i9_s_suppkey	BIGINT
);

DROP TABLE IF EXISTS i9_p CASCADE;
CREATE TABLE i9_p (
	i9_p_partkey	BIGINT
);

DROP TABLE IF EXISTS i9_ps CASCADE;
CREATE TABLE i9_ps (
	i9_ps_partkey	BIGINT,
	i9_ps_suppkey	BIGINT,
	i9_ps_supplycost	decimal
);

DROP TABLE IF EXISTS i9_o CASCADE;
CREATE TABLE i9_o (
	i9_o_orderkey	BIGINT,
	i9_o_orderdate	date
);

DROP TABLE IF EXISTS i9_l CASCADE;
CREATE TABLE i9_l (
	i9_l_suppkey	BIGINT,
	i9_l_partkey	BIGINT,
	i9_l_orderkey	BIGINT,
	i9_l_quantity	decimal,
	i9_l_extendedprice	decimal,
	i9_l_discount	decimal
);

DROP TABLE IF EXISTS i9_n CASCADE;
CREATE TABLE i9_n (
	i9_n_nationkey	BIGINT,
	i9_n_name	char(25)
);




