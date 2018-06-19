DROP TABLE IF EXISTS q9 CASCADE;
CREATE TABLE q9 (
	q9_name	char(25),
	q9_supplycost	decimal,
	q9_year	double precision,
	q9_quantity	decimal,
	q9_extendedprice	decimal,
	q9_discount	decimal
);

DROP TABLE IF EXISTS q9_n_s_p_ps_l CASCADE;
CREATE TABLE q9_n_s_p_ps_l (
	q9_n_s_p_ps_l_orderkey	BIGINT,
	q9_n_s_p_ps_l_name	char(25),
	q9_n_s_p_ps_l_supplycost	decimal,
	q9_n_s_p_ps_l_quantity	decimal,
	q9_n_s_p_ps_l_extendedprice	decimal,
	q9_n_s_p_ps_l_discount	decimal
);

DROP TABLE IF EXISTS q9_n_s_p_o_l CASCADE;
CREATE TABLE q9_n_s_p_o_l (
	q9_n_s_p_o_l_suppkey	BIGINT,
	q9_n_s_p_o_l_partkey	BIGINT,
	q9_n_s_p_o_l_name	char(25),
	q9_n_s_p_o_l_year	double precision,
	q9_n_s_p_o_l_quantity	decimal,
	q9_n_s_p_o_l_extendedprice	decimal,
	q9_n_s_p_o_l_discount	decimal
);

DROP TABLE IF EXISTS q9_n_s_ps_o_l CASCADE;
CREATE TABLE q9_n_s_ps_o_l (
	q9_n_s_ps_o_l_partkey	BIGINT,
	q9_n_s_ps_o_l_name	char(25),
	q9_n_s_ps_o_l_supplycost	decimal,
	q9_n_s_ps_o_l_year	double precision,
	q9_n_s_ps_o_l_quantity	decimal,
	q9_n_s_ps_o_l_extendedprice	decimal,
	q9_n_s_ps_o_l_discount	decimal
);

DROP TABLE IF EXISTS q9_p_ps_o_l CASCADE;
CREATE TABLE q9_p_ps_o_l (
	q9_p_ps_o_l_suppkey	BIGINT,
	q9_p_ps_o_l_supplycost	decimal,
	q9_p_ps_o_l_year	double precision,
	q9_p_ps_o_l_quantity	decimal,
	q9_p_ps_o_l_extendedprice	decimal,
	q9_p_ps_o_l_discount	decimal
);

DROP TABLE IF EXISTS q9_n_s_p_l CASCADE;
CREATE TABLE q9_n_s_p_l (
	q9_n_s_p_l_suppkey	BIGINT,
	q9_n_s_p_l_partkey	BIGINT,
	q9_n_s_p_l_orderkey	BIGINT,
	q9_n_s_p_l_name	char(25),
	q9_n_s_p_l_quantity	decimal,
	q9_n_s_p_l_extendedprice	decimal,
	q9_n_s_p_l_discount	decimal
);

DROP TABLE IF EXISTS q9_n_s_ps_l CASCADE;
CREATE TABLE q9_n_s_ps_l (
	q9_n_s_ps_l_partkey	BIGINT,
	q9_n_s_ps_l_orderkey	BIGINT,
	q9_n_s_ps_l_name	char(25),
	q9_n_s_ps_l_supplycost	decimal,
	q9_n_s_ps_l_quantity	decimal,
	q9_n_s_ps_l_extendedprice	decimal,
	q9_n_s_ps_l_discount	decimal
);

DROP TABLE IF EXISTS q9_n_s_o_l CASCADE;
CREATE TABLE q9_n_s_o_l (
	q9_n_s_o_l_suppkey	BIGINT,
	q9_n_s_o_l_partkey	BIGINT,
	q9_n_s_o_l_name	char(25),
	q9_n_s_o_l_year	double precision,
	q9_n_s_o_l_quantity	decimal,
	q9_n_s_o_l_extendedprice	decimal,
	q9_n_s_o_l_discount	decimal
);

DROP TABLE IF EXISTS q9_p_ps_l CASCADE;
CREATE TABLE q9_p_ps_l (
	q9_p_ps_l_suppkey	BIGINT,
	q9_p_ps_l_orderkey	BIGINT,
	q9_p_ps_l_supplycost	decimal,
	q9_p_ps_l_quantity	decimal,
	q9_p_ps_l_extendedprice	decimal,
	q9_p_ps_l_discount	decimal
);

DROP TABLE IF EXISTS q9_p_o_l CASCADE;
CREATE TABLE q9_p_o_l (
	q9_p_o_l_suppkey	BIGINT,
	q9_p_o_l_partkey	BIGINT,
	q9_p_o_l_year	double precision,
	q9_p_o_l_quantity	decimal,
	q9_p_o_l_extendedprice	decimal,
	q9_p_o_l_discount	decimal
);

DROP TABLE IF EXISTS q9_ps_o_l CASCADE;
CREATE TABLE q9_ps_o_l (
	q9_ps_o_l_suppkey	BIGINT,
	q9_ps_o_l_partkey	BIGINT,
	q9_ps_o_l_supplycost	decimal,
	q9_ps_o_l_year	double precision,
	q9_ps_o_l_quantity	decimal,
	q9_ps_o_l_extendedprice	decimal,
	q9_ps_o_l_discount	decimal
);

DROP TABLE IF EXISTS q9_n_s_l CASCADE;
CREATE TABLE q9_n_s_l (
	q9_n_s_l_suppkey	BIGINT,
	q9_n_s_l_partkey	BIGINT,
	q9_n_s_l_orderkey	BIGINT,
	q9_n_s_l_name	char(25),
	q9_n_s_l_quantity	decimal,
	q9_n_s_l_extendedprice	decimal,
	q9_n_s_l_discount	decimal
);

DROP TABLE IF EXISTS q9_p_l CASCADE;
CREATE TABLE q9_p_l (
	q9_p_l_suppkey	BIGINT,
	q9_p_l_partkey	BIGINT,
	q9_p_l_orderkey	BIGINT,
	q9_p_l_quantity	decimal,
	q9_p_l_extendedprice	decimal,
	q9_p_l_discount	decimal
);

DROP TABLE IF EXISTS q9_ps_l CASCADE;
CREATE TABLE q9_ps_l (
	q9_ps_l_suppkey	BIGINT,
	q9_ps_l_partkey	BIGINT,
	q9_ps_l_orderkey	BIGINT,
	q9_ps_l_supplycost	decimal,
	q9_ps_l_quantity	decimal,
	q9_ps_l_extendedprice	decimal,
	q9_ps_l_discount	decimal
);

DROP TABLE IF EXISTS q9_o_l CASCADE;
CREATE TABLE q9_o_l (
	q9_o_l_suppkey	BIGINT,
	q9_o_l_partkey	BIGINT,
	q9_o_l_year	double precision,
	q9_o_l_quantity	decimal,
	q9_o_l_extendedprice	decimal,
	q9_o_l_discount	decimal
);

DROP TABLE IF EXISTS q9_n_s CASCADE;
CREATE TABLE q9_n_s (
	q9_n_s_suppkey	BIGINT,
	q9_n_s_name	char(25)
);

DROP TABLE IF EXISTS q9_s CASCADE;
CREATE TABLE q9_s (
	q9_s_nationkey	BIGINT,
	q9_s_suppkey	BIGINT
);

DROP TABLE IF EXISTS q9_p CASCADE;
CREATE TABLE q9_p (
	q9_p_partkey	BIGINT
);

DROP TABLE IF EXISTS q9_ps CASCADE;
CREATE TABLE q9_ps (
	q9_ps_partkey	BIGINT,
	q9_ps_suppkey	BIGINT,
	q9_ps_supplycost	decimal
);

DROP TABLE IF EXISTS q9_o CASCADE;
CREATE TABLE q9_o (
	q9_o_orderkey	BIGINT,
	q9_o_year	double precision
);

DROP TABLE IF EXISTS q9_l CASCADE;
CREATE TABLE q9_l (
	q9_l_suppkey	BIGINT,
	q9_l_partkey	BIGINT,
	q9_l_orderkey	BIGINT,
	q9_l_quantity	decimal,
	q9_l_extendedprice	decimal,
	q9_l_discount	decimal
);

DROP TABLE IF EXISTS q9_n CASCADE;
CREATE TABLE q9_n (
	q9_n_nationkey	BIGINT,
	q9_n_name	char(25)
);

Insert into q9_n select n_nationkey, n_name from nation; 


