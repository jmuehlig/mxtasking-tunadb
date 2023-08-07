create table customer (
	c_custkey int PRIMARY KEY,
	c_name char(25),
	c_address char(40),
	c_nationkey int,
	c_phone char(15),
	c_acctbal decimal,
	c_mktsegment char(10),
	c_comment char(117)
);

create table lineitem (
	l_orderkey int PRIMARY KEY,
	l_partkey int,
	l_suppkey int,
	l_linenumber int PRIMARY KEY,
	l_quantity decimal,
	l_extendedprice decimal,
	l_discount decimal,
	l_tax decimal,
	l_returnflag char(1),
	l_linestatus char(1),
	l_shipdate date,
	l_commitdate date,
	l_receiptdate date,
	l_shipinstruct char(25),
	l_shipmode char(10),
	l_comment char(44)
);

create table nation (
	n_nationkey int PRIMARY KEY,
	n_name char(25),
	n_regionkey int,
	n_comment char(152)
);

create table orders (
	o_orderkey int PRIMARY KEY,
	o_custkey int,
	o_orderstatus char(1),
	o_totalprice decimal,
	o_orderdate date,
	o_orderpriority char(15),
	o_clerk char(15),
	o_shippriority int,
	o_comment char(79)
);

create table part (
	p_partkey int PRIMARY KEY,
	p_name char(55),
	p_mfgr char(25),
	p_brand char(10),
	p_type char(25),
	p_size int,
	p_container char(10),
	p_retailprice decimal,
	p_comment char(23)
);

create table partsupp (
	ps_partkey int PRIMARY KEY,
	ps_suppkey int PRIMARY KEY,
	ps_availqty int,
	ps_supplycost decimal,
	ps_comment char(199)
);


create table region (
	r_regionkey int PRIMARY KEY,
	r_name char(55),
	r_comment char (152)
);

create table supplier (
	s_suppkey int PRIMARY KEY,
	s_name char(24),
	s_address char(40),
	s_nationkey int,
	s_phone char(15),
	s_acctbal decimal,
	s_comment char(101)
);

