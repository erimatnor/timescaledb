-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ON_ERROR_STOP 0

--negative tests for query validation
create table mat_t1( a integer, b integer,c TEXT);

CREATE TABLE conditions (
      timec        TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature integer  NULL,
      humidity    DOUBLE PRECISION  NULL,
      timemeasure TIMESTAMPTZ,
      timeinterval INTERVAL
    );
select table_name from create_hypertable( 'conditions', 'timec');

CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous, timescaledb.myfill = 1)
as
select location , min(temperature)
from conditions
group by time_bucket('1d', timec), location WITH NO DATA;

--valid PG option
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous, check_option = LOCAL )
as
select * from conditions , mat_t1 WITH NO DATA;

-- join multiple tables
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
as
select location, count(*) from conditions , mat_t1
where conditions.location = mat_t1.c
group by location WITH NO DATA;

-- join multiple tables WITH explicit JOIN
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
as
select location, count(*) from conditions JOIN mat_t1 ON true
where conditions.location = mat_t1.c
group by location WITH NO DATA;

-- LATERAL multiple tables
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
as
select location, count(*) from conditions,
LATERAL (Select * from mat_t1 where c = conditions.location) q
group by location WITH NO DATA;


--non-hypertable
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
as
select a, count(*) from mat_t1
group by a WITH NO DATA;


-- no group by
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
as
select count(*) from conditions  WITH NO DATA;

-- no time_bucket in group by
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
as
select count(*) from conditions group by location WITH NO DATA;

-- with valid query in a CTE
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
with m1 as (
Select location, count(*) from conditions
 group by time_bucket('1week', timec) , location)
select * from m1 WITH NO DATA;

--with DISTINCT ON
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
as
 select distinct on ( location ) count(*)  from conditions group by location, time_bucket('1week', timec)  WITH NO DATA;

--aggregate with DISTINCT
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select time_bucket('1week', timec),
 count(location) , sum(distinct temperature) from conditions
 group by time_bucket('1week', timec) , location WITH NO DATA;

--aggregate with FILTER
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select time_bucket('1week', timec),
 sum(temperature) filter ( where humidity > 20 ) from conditions
 group by time_bucket('1week', timec) , location WITH NO DATA;

-- aggregate with filter in having clause
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select time_bucket('1week', timec), max(temperature)
from conditions
 group by time_bucket('1week', timec) , location
 having sum(temperature) filter ( where humidity > 20 ) > 50 WITH NO DATA;

-- time_bucket on non partitioning column of hypertable
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select max(temperature)
from conditions
 group by time_bucket('1week', timemeasure) , location WITH NO DATA;

--time_bucket on expression
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select max(temperature)
from conditions
 group by time_bucket('1week', timec+ '10 minutes'::interval) , location WITH NO DATA;

--multiple time_bucket functions
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select max(temperature)
from conditions
 group by time_bucket('1week', timec) , time_bucket('1month', timec), location WITH NO DATA;

--time_bucket using additional args
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select max(temperature)
from conditions
 group by time_bucket( INTERVAL '5 minutes', timec, INTERVAL '-2.5 minutes') , location WITH NO DATA;

--time_bucket using non-const for first argument
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select max(temperature)
from conditions
 group by time_bucket( timeinterval, timec) , location WITH NO DATA;

-- ordered set aggr
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select mode() within group( order by humidity)
from conditions
 group by time_bucket('1week', timec)  WITH NO DATA;

--window function
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select avg(temperature) over( order by humidity)
from conditions
 WITH NO DATA;

--aggregate without combine function
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select json_agg(location)
from conditions
 group by time_bucket('1week', timec) , location WITH NO DATA;
;

CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature), array_agg(location)
from conditions
 group by time_bucket('1week', timec) , location WITH NO DATA;
;

-- userdefined aggregate without combine function
CREATE AGGREGATE newavg (
   sfunc = int4_avg_accum, basetype = int4, stype = _int8,
   finalfunc = int8_avg,
   initcond1 = '{0,0}'
);
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), newavg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location WITH NO DATA;
;

-- using subqueries
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from
( select humidity, temperature, location, timec
from conditions ) q
 group by time_bucket('1week', timec) , location  WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
select * from
( Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location )  q WITH NO DATA;

--using limit /limit offset
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
limit 10  WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
offset 10 WITH NO DATA;

--using ORDER BY in view defintion
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
ORDER BY 1 WITH NO DATA;

--using FETCH
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
fetch first 10 rows only WITH NO DATA;

--using locking clauses FOR clause
--all should be disabled. we cannot guarntee locks on the hypertable
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
FOR KEY SHARE WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
FOR SHARE WITH NO DATA;


CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
FOR UPDATE WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
FOR NO KEY UPDATE WITH NO DATA;

--tablesample clause
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions tablesample bernoulli(0.2)
 group by time_bucket('1week', timec) , location
 WITH NO DATA;

-- ONLY in from clause
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from ONLY conditions
 group by time_bucket('1week', timec) , location  WITH NO DATA;

--grouping sets and variants
CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by grouping sets(time_bucket('1week', timec) , location )  WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
group by rollup(time_bucket('1week', timec) , location )  WITH NO DATA;

--NO immutable functions -- check all clauses
CREATE FUNCTION test_stablefunc(int) RETURNS int LANGUAGE 'sql'
       STABLE AS 'SELECT $1 + 10';

CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), max(timec + INTERVAL '1h')
from conditions
group by time_bucket('1week', timec) , location   WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), min(location)
from conditions
group by time_bucket('1week', timec)
having  max(timec + INTERVAL '1h') > '2010-01-01 09:00:00-08' WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum( test_stablefunc(humidity::int) ), min(location)
from conditions
group by time_bucket('1week', timec) WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum( temperature ), min(location)
from conditions
group by time_bucket('1week', timec), test_stablefunc(humidity::int) WITH NO DATA;

-- Should use CREATE MATERIALIZED VIEW to create continuous aggregates
CREATE VIEW continuous_aggs_errors_tbl1 WITH (timescaledb.continuous) AS
SELECT time_bucket('1 week', timec)
  FROM conditions
GROUP BY time_bucket('1 week', timec);

-- row security on table
create table rowsec_tab( a bigint, b integer, c integer);
select table_name from create_hypertable( 'rowsec_tab', 'a', chunk_time_interval=>10);
CREATE OR REPLACE FUNCTION integer_now_test() returns bigint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(a), 0)::bigint FROM rowsec_tab $$;
SELECT set_integer_now_func('rowsec_tab', 'integer_now_test');
alter table rowsec_tab ENABLE ROW LEVEL SECURITY;
create policy rowsec_tab_allview ON rowsec_tab FOR SELECT USING(true);

CREATE MATERIALIZED VIEW mat_m1 WITH ( timescaledb.continuous)
AS
Select sum( b), min(c)
from rowsec_tab
group by time_bucket('1', a) WITH NO DATA;

drop table conditions cascade;

--negative tests for WITH options
CREATE TABLE conditions (
      timec       TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null,
      allnull     double precision null
    );

select table_name from create_hypertable( 'conditions', 'timec');

create materialized view mat_with_test( timec, minl, sumt , sumh)
WITH (timescaledb.continuous)
as
select time_bucket('1day', timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket('1day', timec) WITH NO DATA;

SELECT  h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'mat_with_test'
\gset

\set ON_ERROR_STOP 0
ALTER MATERIALIZED VIEW mat_with_test SET(timescaledb.create_group_indexes = 'false');
ALTER MATERIALIZED VIEW mat_with_test SET(timescaledb.create_group_indexes = 'true');
ALTER MATERIALIZED VIEW mat_with_test ALTER timec DROP default;
\set ON_ERROR_STOP 1

DROP TABLE conditions CASCADE;


--test WITH using a hypertable with an integer time dimension
CREATE TABLE conditions (
      timec       SMALLINT       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null,
      allnull     double precision null
    );

select table_name from create_hypertable( 'conditions', 'timec', chunk_time_interval=> 100);
CREATE OR REPLACE FUNCTION integer_now_test_s() returns smallint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(timec), 0)::smallint FROM conditions $$;
SELECT set_integer_now_func('conditions', 'integer_now_test_s');

\set ON_ERROR_STOP 0
create materialized view mat_with_test( timec, minl, sumt , sumh)
WITH (timescaledb.continuous)
as
select time_bucket(100, timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket(100, timec) WITH NO DATA;

create materialized view mat_with_test( timec, minl, sumt , sumh)
WITH (timescaledb.continuous)
as
select time_bucket(100, timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket(100, timec) WITH NO DATA;

ALTER TABLE conditions ALTER timec type int;

create materialized view mat_with_test( timec, minl, sumt , sumh)
WITH (timescaledb.continuous)
as
select time_bucket(100, timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket(100, timec) WITH NO DATA;

\set ON_ERROR_STOP 0
DROP TABLE conditions cascade;

CREATE TABLE conditions (
      timec       BIGINT       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null,
      allnull     double precision null
    );

select table_name from create_hypertable( 'conditions', 'timec', chunk_time_interval=> 100);
CREATE OR REPLACE FUNCTION integer_now_test_b() returns bigint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(timec), 0)::bigint FROM conditions $$;
SELECT set_integer_now_func('conditions', 'integer_now_test_b');

create materialized view mat_with_test( timec, minl, sumt , sumh)
WITH (timescaledb.continuous)
as
select time_bucket(BIGINT '100', timec), min(location), sum(temperature),sum(humidity)
from conditions
group by 1 WITH NO DATA;

-- custom time partition functions are not supported with invalidations
CREATE FUNCTION text_part_func(TEXT) RETURNS BIGINT
    AS $$ SELECT length($1)::BIGINT $$
    LANGUAGE SQL IMMUTABLE;

CREATE TABLE text_time(time TEXT);
    SELECT create_hypertable('text_time', 'time', chunk_time_interval => 10, time_partitioning_func => 'text_part_func');

\set ON_ERROR_STOP 0
CREATE MATERIALIZED VIEW text_view
    WITH (timescaledb.continuous)
    AS SELECT time_bucket('5', text_part_func(time)), COUNT(time)
        FROM text_time
        GROUP BY 1 WITH NO DATA;
\set ON_ERROR_STOP 1

-- Check that we get an error when mixing normal materialized views
-- and continuous aggregates.
CREATE MATERIALIZED VIEW normal_mat_view AS
SELECT time_bucket('5', text_part_func(time)), COUNT(time)
  FROM text_time
GROUP BY 1 WITH NO DATA;

\set ON_ERROR_STOP 0
DROP MATERIALIZED VIEW normal_mat_view, mat_with_test;
\set ON_ERROR_STOP 1

DROP TABLE text_time CASCADE;
