DROP TABLE IF EXISTS q7 CASCADE;
CREATE TABLE q7 (
	q7_name2	char(25),
	q7_name1	char(25),
	q7_year		double precision,
	q7_extendedprice	decimal,
	q7_discount	decimal
);

DROP TABLE IF EXISTS q7_n2_c_o_l CASCADE;
CREATE TABLE q7_n2_c_o_l (
	q7_n2_c_o_l_suppkey	BIGINT,
	q7_n2_c_o_l_name2	char(25),
	q7_n2_c_o_l_year	double precision,
	q7_n2_c_o_l_extendedprice	decimal,
	q7_n2_c_o_l_discount	decimal
);

DROP TABLE IF EXISTS q7_n1_s_o_l CASCADE;
CREATE TABLE q7_n1_s_o_l (
	q7_n1_s_o_l_custkey	BIGINT,
	q7_n1_s_o_l_name1	char(25),
	q7_n1_s_o_l_year	double precision,
	q7_n1_s_o_l_extendedprice	decimal,
	q7_n1_s_o_l_discount	decimal
);

DROP TABLE IF EXISTS q7_n2_c_o CASCADE;
CREATE TABLE q7_n2_c_o (
	q7_n2_c_o_orderkey	BIGINT,
	q7_n2_c_o_name2	char(25)
);

DROP TABLE IF EXISTS q7_n1_s_l CASCADE;
CREATE TABLE q7_n1_s_l (
	q7_n1_s_l_orderkey	BIGINT,
	q7_n1_s_l_name1	char(25),
	q7_n1_s_l_year	double precision,
	q7_n1_s_l_extendedprice	decimal,
	q7_n1_s_l_discount	decimal
);

DROP TABLE IF EXISTS q7_o_l CASCADE;
CREATE TABLE q7_o_l (
	q7_o_l_custkey	BIGINT,
	q7_o_l_suppkey	BIGINT,
	q7_o_l_year	double precision,
	q7_o_l_extendedprice	decimal,
	q7_o_l_discount	decimal
);

DROP TABLE IF EXISTS q7_n2_c CASCADE;
CREATE TABLE q7_n2_c (
	q7_n2_c_custkey	BIGINT,
	q7_n2_c_name2	char(25)
);

DROP TABLE IF EXISTS q7_n1_s CASCADE;
CREATE TABLE q7_n1_s (
	q7_n1_s_suppkey	BIGINT,
	q7_n1_s_name1	char(25)
);

DROP TABLE IF EXISTS q7_c CASCADE;
CREATE TABLE q7_c (
    q7_c_nationkey BIGINT, 
    q7_c_custkey    BIGINT
);

DROP TABLE IF EXISTS q7_s CASCADE;
CREATE TABLE q7_s (
    q7_s_nationkey  BIGINT,
	q7_s_suppkey	BIGINT
);

DROP TABLE IF EXISTS q7_o CASCADE;
CREATE TABLE q7_o (
	q7_o_custkey	BIGINT,
	q7_o_orderkey	BIGINT
);

DROP TABLE IF EXISTS q7_l CASCADE;
CREATE TABLE q7_l (
	q7_l_suppkey	BIGINT,
	q7_l_orderkey	BIGINT,
	q7_l_year	double precision,
	q7_l_extendedprice	decimal,
	q7_l_discount	decimal
);

DROP TABLE IF EXISTS q7_n2 CASCADE;
CREATE TABLE q7_n2 (
    q7_n2_nationkey BIGINT, 
    q7_n2_name2      char(25) 
);

DROP TABLE IF EXISTS q7_n1 CASCADE;
CREATE TABLE q7_n1 (
    q7_n1_nationkey  BIGINT,
	q7_n1_name1     char(25)
);

Insert into q7_n2 select n_nationkey, n_name from nation where n_name = 'FRANCE' or n_name = 'GERMANY';
Insert into q7_n1 select n_nationkey, n_name from nation where n_name = 'FRANCE' or n_name = 'GERMANY';
