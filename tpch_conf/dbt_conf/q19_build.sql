DROP TABLE IF EXISTS q19 CASCADE;
CREATE TABLE q19 (
    q19_linenumber		INTEGER,
	q19_extendedprice	DECIMAL,
	q19_discount	DECIMAL
);

DROP TABLE IF EXISTS q19_p CASCADE;
CREATE TABLE q19_p (
	q19_p_partkey	BIGINT
);

DROP TABLE IF EXISTS q19_l CASCADE;
CREATE TABLE q19_l (
	q19_l_partkey	BIGINT,
    q19_l_linenumber INTEGER,
	q19_l_extendedprice	DECIMAL,
	q19_l_discount	DECIMAL
);


