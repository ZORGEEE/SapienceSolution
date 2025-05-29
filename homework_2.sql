create table plan(
    date date not null ,
    region varchar(20) not null ,
    matdirec varchar(20) not null,
    quantity integer,
    distr_chan varchar(100) not null
)
    with (appendonly = true, orientation = column , compresstype = zstd, compresslevel = 5)
    distributed randomly
    partition by range (date) (default partition plan,
        start ('2021-01-01'::date) inclusive
        end ('2021-12-31'::date) exclusive
        every ('1 mon'::interval));

create table sales(
    date date not null,
    region varchar(20) not null,
    material varchar(20) not null,
    distr_chan varchar(100) not null,
    quantity integer,
    check_nm varchar(100) not null ,
    check_pos varchar(100) not null
)
    with (appendonly = true, orientation = column, compresstype = zstd, compresslevel = 5)
    distributed randomly
    partition by range (date) (default partition sales,
        start ('2021-01-01'::date) inclusive
        end ('2021-12-31'::date) exclusive
        every ('1 mon'::interval));

create table price (
    material varchar(20) not null,
    region varchar(20) not null,
    distr_chan varchar(100) not null,
    price integer
)
    with (appendonly = true, orientation = row, compresstype = zstd, compresslevel = 5)
    distributed replicated;

create table product (
    material varchar(20) not null,
    asgrp integer not null,
    brand integer not null,
    matcateg varchar(4) not null,
    matdirec integer,
    txt text
)
    with (appendonly = true, orientation = row, compresstype = zstd, compresslevel = 5)
    distributed replicated;

create table chanel (
    distr_chan varchar(1) not null,
    txtsh text
)
    with (appendonly = true, orientation = row, compresstype = zstd, compresslevel = 5)
    distributed replicated;

create table region (
    region varchar(4),
    txt text
)
    with (appendonly = true, orientation = row, compresstype = zstd, compresslevel = 5)
    distributed replicated;

