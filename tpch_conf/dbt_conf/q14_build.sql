DROP TABLE IF EXISTS q14 CASCADE;
CREATE TABLE q14 (
	q14_type	VARCHAR(25),
    	q14_linenumber INTEGER,
	q14_extendedprice	DECIMAL,
	q14_discount	DECIMAL
);

DROP TABLE IF EXISTS q14_p CASCADE;
CREATE TABLE q14_p (
	q14_p_partkey	BIGINT,
	q14_p_type	VARCHAR(25)
);

DROP TABLE IF EXISTS q14_l CASCADE;
CREATE TABLE q14_l (
	q14_l_partkey	BIGINT,
    	q14_l_linenumber INTEGER,
	q14_l_extendedprice	DECIMAL,
	q14_l_discount	DECIMAL
);

