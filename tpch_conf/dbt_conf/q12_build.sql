DROP TABLE IF EXISTS q12 CASCADE;
CREATE TABLE q12 (
	q12_orderpriority	CHAR(15),
	q12_shipmode	CHAR(10)
);

DROP TABLE IF EXISTS q12_o CASCADE;
CREATE TABLE q12_o (
	q12_o_orderkey	BIGINT,
	q12_o_orderpriority	CHAR(15)
);

DROP TABLE IF EXISTS q12_l CASCADE;
CREATE TABLE q12_l (
	q12_l_orderkey	BIGINT,
	q12_l_shipmode	CHAR(10)
);

