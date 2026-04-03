create external table plan_ext (
    date date,
    region varchar(20),
    matdirec varchar(20),
    quantity integer,
    distr_chan varchar(100)
    )
location ('pxf://gp.plan?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern')
on all
format 'custom' (formatter = 'pxfwritable_import')
encoding 'UTF8';

create external table sales_ext (
    date       date,
    region     varchar(20),
    material   varchar(20),
    distr_chan varchar(100),
    quantity   integer,
    check_nm   varchar(100),
    check_pos  varchar(100)
    )
location ('pxf://gp.sales?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern')
on all
format 'custom' (formatter = 'pxfwritable_import')
encoding 'UTF8';

create external table chanel_ext (
    distr_chan varchar(20),
    txtsh text
    )
location (
    'gpfdist://172.16.128.202:8080/chanel.csv'
    )
on all
format 'CSV' (delimiter ';' null '' escape'"' quote '"' header)
encoding 'UTF8'
segment reject limit 10 rows ;

create external table price_ext (
	material   varchar(20),
    region     varchar(20),
    distr_chan varchar(100),
    price      integer
)
location (
	'gpfdist://172.16.128.202:8080/price.csv'
)
format 'CSV' (delimiter ';' null '' escape'"' quote '"' header)
encoding 'UTF8'
segment reject limit 10 rows ;

create external table product_ext (
	material varchar(20),
    asgrp    integer,
    brand    integer,
    matcateg varchar(4),
    matdirec integer,
    txt      text
)
location (
	'gpfdist://172.16.128.202:8080/product.csv'
)
format 'CSV' (delimiter ';' null '' escape'"' quote '"' header)
encoding 'UTF8'
segment reject limit 10 rows ;

create external table region_ext (
	region varchar(4),
    txt    text
)
location ('gpfdist://172.16.128.202:8080/region.csv')
format 'CSV' (delimiter ';' null '' escape'"' quote '"' header)
encoding 'UTF8'
segment reject limit 10 rows ;

drop external table
    region_ext;

select count(*) from region_ext;
