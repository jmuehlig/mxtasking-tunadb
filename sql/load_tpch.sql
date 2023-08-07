.load file 'sql/tpch-schema.sql';

copy customer from 'sql/data/tpch/customer.tbl' (delimiter '|');
copy lineitem from 'sql/data/tpch/lineitem.tbl' (delimiter '|');
copy nation from 'sql/data/tpch/nation.tbl' (delimiter '|');
copy orders from 'sql/data/tpch/orders.tbl' (delimiter '|');
copy part from 'sql/data/tpch/part.tbl' (delimiter '|');
copy partsupp from 'sql/data/tpch/partsupp.tbl' (delimiter '|');
copy region from 'sql/data/tpch/region.tbl' (delimiter '|');
copy supplier from 'sql/data/tpch/supplier.tbl' (delimiter '|');

.load file 'sql/tpch-update-statistics.sql';
