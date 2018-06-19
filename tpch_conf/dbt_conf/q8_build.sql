
DROP TABLE IF EXISTS q8 CASCADE;
CREATE TABLE q8 (
	q8_name	char(25),
	q8_year	double precision,
	q8_extendedprice	decimal,
	q8_discount	decimal
);

DROP TABLE IF EXISTS q8_r_n_c_n2_s_o_l CASCADE;
CREATE TABLE q8_r_n_c_n2_s_o_l (
	q8_r_n_c_n2_s_o_l_partkey	BIGINT,
	q8_r_n_c_n2_s_o_l_name	char(25),
	q8_r_n_c_n2_s_o_l_year	double precision,
	q8_r_n_c_n2_s_o_l_extendedprice	decimal,
	q8_r_n_c_n2_s_o_l_discount	decimal
);

DROP TABLE IF EXISTS q8_r_n_c_p_o_l CASCADE;
CREATE TABLE q8_r_n_c_p_o_l (
	q8_r_n_c_p_o_l_suppkey	BIGINT,
	q8_r_n_c_p_o_l_year	double precision,
	q8_r_n_c_p_o_l_extendedprice	decimal,
	q8_r_n_c_p_o_l_discount	decimal
);

DROP TABLE IF EXISTS q8_n2_s_p_o_l CASCADE;
CREATE TABLE q8_n2_s_p_o_l (
	q8_n2_s_p_o_l_custkey	BIGINT,
	q8_n2_s_p_o_l_name	char(25),
	q8_n2_s_p_o_l_year	double precision,
	q8_n2_s_p_o_l_extendedprice	decimal,
	q8_n2_s_p_o_l_discount	decimal
);

DROP TABLE IF EXISTS q8_r_n_c_o_l CASCADE;
CREATE TABLE q8_r_n_c_o_l (
	q8_r_n_c_o_l_suppkey	BIGINT,
	q8_r_n_c_o_l_partkey	BIGINT,
	q8_r_n_c_o_l_year	double precision,
	q8_r_n_c_o_l_extendedprice	decimal,
	q8_r_n_c_o_l_discount	decimal
);

DROP TABLE IF EXISTS q8_n2_s_p_l CASCADE;
CREATE TABLE q8_n2_s_p_l (
	q8_n2_s_p_l_orderkey	BIGINT,
	q8_n2_s_p_l_name	char(25),
	q8_n2_s_p_l_extendedprice	decimal,
	q8_n2_s_p_l_discount	decimal
);

DROP TABLE IF EXISTS q8_n2_s_o_l CASCADE;
CREATE TABLE q8_n2_s_o_l (
	q8_n2_s_o_l_custkey	BIGINT,
	q8_n2_s_o_l_partkey	BIGINT,
	q8_n2_s_o_l_name	char(25),
	q8_n2_s_o_l_year	double precision,
	q8_n2_s_o_l_extendedprice	decimal,
	q8_n2_s_o_l_discount	decimal
);

DROP TABLE IF EXISTS q8_p_o_l CASCADE;
CREATE TABLE q8_p_o_l (
	q8_p_o_l_custkey	BIGINT,
	q8_p_o_l_suppkey	BIGINT,
	q8_p_o_l_year	double precision,
	q8_p_o_l_extendedprice	decimal,
	q8_p_o_l_discount	decimal
);

DROP TABLE IF EXISTS q8_r_n_c_o CASCADE;
CREATE TABLE q8_r_n_c_o (
	q8_r_n_c_o_orderkey	BIGINT,
	q8_r_n_c_o_year	double precision
);

DROP TABLE IF EXISTS q8_n2_s_l CASCADE;
CREATE TABLE q8_n2_s_l (
	q8_n2_s_l_partkey	BIGINT,
	q8_n2_s_l_orderkey	BIGINT,
	q8_n2_s_l_name	char(25),
	q8_n2_s_l_extendedprice	decimal,
	q8_n2_s_l_discount	decimal
);

DROP TABLE IF EXISTS q8_p_l CASCADE;
CREATE TABLE q8_p_l (
	q8_p_l_suppkey	BIGINT,
	q8_p_l_orderkey	BIGINT,
	q8_p_l_extendedprice	decimal,
	q8_p_l_discount	decimal
);

DROP TABLE IF EXISTS q8_o_l CASCADE;
CREATE TABLE q8_o_l (
	q8_o_l_custkey	BIGINT,
	q8_o_l_suppkey	BIGINT,
	q8_o_l_partkey	BIGINT,
	q8_o_l_year	double precision,
	q8_o_l_extendedprice	decimal,
	q8_o_l_discount	decimal
);

DROP TABLE IF EXISTS q8_r_n_c CASCADE;
CREATE TABLE q8_r_n_c (
	q8_r_n_c_custkey	BIGINT
);

DROP TABLE IF EXISTS q8_n2_s CASCADE;
CREATE TABLE q8_n2_s (
	q8_n2_s_suppkey	BIGINT,
	q8_n2_s_name	char(25)
);


DROP TABLE IF EXISTS q8_c CASCADE;
CREATE TABLE q8_c (
    q8_c_nationkey  BIGINT,
	q8_c_custkey	BIGINT
);

DROP TABLE IF EXISTS q8_s CASCADE;
CREATE TABLE q8_s (
    q8_s_nationkey  BIGINT,
	q8_s_suppkey	BIGINT
);

DROP TABLE IF EXISTS q8_p CASCADE;
CREATE TABLE q8_p (
	q8_p_partkey	BIGINT
);

DROP TABLE IF EXISTS q8_o CASCADE;
CREATE TABLE q8_o (
	q8_o_custkey	BIGINT,
	q8_o_orderkey	BIGINT,
	q8_o_year	double precision
);

DROP TABLE IF EXISTS q8_l CASCADE;
CREATE TABLE q8_l (
	q8_l_suppkey	BIGINT,
	q8_l_partkey	BIGINT,
	q8_l_orderkey	BIGINT,
	q8_l_extendedprice	decimal,
	q8_l_discount	decimal
);


DROP TABLE IF EXISTS q8_r_n CASCADE;
CREATE TABLE q8_r_n (
	q8_r_n_nationkey	BIGINT
);

DROP TABLE IF EXISTS q8_n2 CASCADE;
CREATE TABLE q8_n2 (
	q8_n2_nationkey	BIGINT,
	q8_n2_name	char(25)
);


Insert into q8_r_n select n_nationkey from region r, nation n where r.r_regionkey = n.n_regionkey and r.r_name = 'AMERICA';
Insert into q8_n2 select n_nationkey, n_name from nation;

