DROP TABLE IF EXISTS i5_c CASCADE;
CREATE TABLE i5_c (
	i5_c_nationkey	BIGINT,
	i5_c_custkey    BIGINT
);

DROP TABLE IF EXISTS i5_s CASCADE;
CREATE TABLE i5_s (
	i5_s_nationkey	BIGINT,
	i5_s_suppkey	BIGINT
);

DROP TABLE IF EXISTS i5_o CASCADE;
CREATE TABLE i5_o (
	i5_o_custkey	BIGINT,
	i5_o_orderkey	BIGINT
);

DROP TABLE IF EXISTS i5_l CASCADE;
CREATE TABLE i5_l (
	i5_l_orderkey	BIGINT,
	i5_l_suppkey	BIGINT,
	i5_l_extendedprice	decimal,
	i5_l_discount	decimal
);

DROP TABLE IF EXISTS i5_n CASCADE;
CREATE TABLE i5_n (
	i5_n_regionkey      BIGINT,
	i5_n_nationkey	BIGINT,
	i5_n_name	char(25)
);

DROP TABLE IF EXISTS i5_r CASCADE;
CREATE TABLE i5_r (
    i5_r_regionkey  bigint
);

