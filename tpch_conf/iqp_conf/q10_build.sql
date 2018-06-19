DROP TABLE IF EXISTS i10_c CASCADE;
CREATE TABLE i10_c (
	i10_c_nationkey BIGINT,
	i10_c_custkey	BIGINT,
	i10_c_custname	varchar(25),
	i10_c_acctbal	decimal,
	i10_c_address	varchar(40),
	i10_c_phone	char(15),
	i10_c_comment	varchar(117)
);

DROP TABLE IF EXISTS i10_o CASCADE;
CREATE TABLE i10_o (
	i10_o_custkey	BIGINT,
	i10_o_orderkey	BIGINT
);

DROP TABLE IF EXISTS i10_l CASCADE;
CREATE TABLE i10_l (
	i10_l_orderkey	BIGINT,
	i10_l_extendedprice	decimal,
	i10_l_discount	decimal
);

DROP TABLE IF EXISTS i10_n CASCADE;
CREATE TABLE i10_n (
	i10_n_nationkey		BIGINT,
	i10_n_nationname	char(25)
);

