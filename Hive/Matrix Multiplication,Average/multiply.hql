drop table A;
drop table B;
drop table temp;

create table A (
  i int,
  j int,
  v double)
row format delimited fields terminated by ',' stored as textfile;

create table B (
  x int,
  y int,
  z double)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:M}' overwrite into table A;

load data local inpath '${hiveconf:N}' overwrite into table B;

CREATE TABLE temp AS
SELECT a.i,b.y,SUM(a.v*b.z)
from A as a join B as b on a.j = b.x
GROUP BY a.i,b.y;

select COUNT(*),avg(c2)
from temp;
