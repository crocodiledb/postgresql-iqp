DROP TABLE IF EXISTS q10 CASCADE;
CREATE TABLE q10 (
	q10_custname	varchar(25),
	q10_acctbal	decimal,
	q10_nationname	char(25),
	q10_address	varchar(40),
	q10_phone	char(15),
	q10_comment	varchar(117),
	q10_extendedprice	decimal,
	q10_discount	decimal
);

DROP TABLE IF EXISTS q10_n_c_o CASCADE;
CREATE TABLE q10_n_c_o (
	q10_n_c_o_orderkey	BIGINT,
	q10_n_c_o_custname	varchar(25),
	q10_n_c_o_acctbal	decimal,
	q10_n_c_o_nationname	char(25),
	q10_n_c_o_address	varchar(40),
	q10_n_c_o_phone	char(15),
	q10_n_c_o_comment	varchar(117)
);

DROP TABLE IF EXISTS q10_o_l CASCADE;
CREATE TABLE q10_o_l (
	q10_o_l_custkey	BIGINT,
	q10_o_l_extendedprice	decimal,
	q10_o_l_discount	decimal
);

DROP TABLE IF EXISTS q10_n_c CASCADE;
CREATE TABLE q10_n_c (
	q10_n_c_custkey	BIGINT,
	q10_n_c_custname	varchar(25),
	q10_n_c_acctbal	decimal,
	q10_n_c_nationname	char(25),
	q10_n_c_address	varchar(40),
	q10_n_c_phone	char(15),
	q10_n_c_comment	varchar(117)
);

DROP TABLE IF EXISTS q10_c CASCADE;
CREATE TABLE q10_c (
	q10_c_nationkey BIGINT,
	q10_c_custkey	BIGINT,
	q10_c_custname	varchar(25),
	q10_c_acctbal	decimal,
	q10_c_address	varchar(40),
	q10_c_phone	char(15),
	q10_c_comment	varchar(117)
);

DROP TABLE IF EXISTS q10_o CASCADE;
CREATE TABLE q10_o (
	q10_o_custkey	BIGINT,
	q10_o_orderkey	BIGINT
);

DROP TABLE IF EXISTS q10_l CASCADE;
CREATE TABLE q10_l (
	q10_l_orderkey	BIGINT,
	q10_l_extendedprice	decimal,
	q10_l_discount	decimal
);

DROP TABLE IF EXISTS q10_n CASCADE;
CREATE TABLE q10_n (
	q10_n_nationkey		BIGINT,
	q10_n_nationname	char(25)
);

Insert into q10_n select n_nationkey, n_name from nation; 

