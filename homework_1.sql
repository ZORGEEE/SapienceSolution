create schema std12_50;

create table std12_50.table1(
    field1 int,
    field2 text
)
distributed by (field1);

insert into std12_50.table1
select a, md5(a::text)
from generate_series(1, 1000) a;

select gp_segment_id, count(1) from std12_50.table1 group by 1 order by 1;

select (gp_toolkit.gp_skew_coefficient('std12_50.table1'::regclass)).skccoeff;