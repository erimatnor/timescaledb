-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--
-- Test calendar-based chunking with various partitioning types
--
-- Calendar-based chunking aligns chunks with date_trunc() boundaries
-- (e.g., start of day, week, month, year) based on the current timezone.
--

\set VERBOSITY terse

-- Enable calendar-based chunking
SET timescaledb.enable_calendar_chunking = true;

---------------------------------------------------------------
-- SECTION 1: UUID v7 PARTITIONING WITH CALENDAR ALIGNMENT
---------------------------------------------------------------

CREATE TABLE uuid_cal_events(
    id uuid PRIMARY KEY,
    device_id int,
    temp float
);

-- Test with daily interval (compatible with date_trunc('day'))
SELECT create_hypertable('uuid_cal_events', 'id', chunk_time_interval => interval '1 day');

-- Verify dimension configuration
SELECT time_interval, integer_interval
FROM timescaledb_information.dimensions
WHERE hypertable_name = 'uuid_cal_events';

-- Insert UUIDs at specific timestamps (generated from known timestamps)
-- These UUIDs correspond to:
-- 2025-01-01 09:00:00 UTC -> 0194214e-cd00-7000-a9a7-63f1416dab45
-- 2025-01-01 02:00:00 UTC -> 01942117-de80-7000-8121-f12b2b69dd96
-- 2025-01-02 03:00:00 UTC -> 0194263e-3a80-7000-8f40-82c987b1bc1f
-- 2025-01-02 04:00:00 UTC -> 01942675-2900-7000-8db1-a98694b18785
-- 2025-01-03 05:00:00 UTC -> 01942bd2-7380-7000-9bc4-5f97443907b8
-- 2025-01-03 12:00:00 UTC -> 01942d52-f900-7000-866e-07d6404d53c1

INSERT INTO uuid_cal_events VALUES
    ('01942117-de80-7000-8121-f12b2b69dd96', 1, 1.0),
    ('0194214e-cd00-7000-a9a7-63f1416dab45', 2, 2.0),
    ('0194263e-3a80-7000-8f40-82c987b1bc1f', 3, 3.0),
    ('01942675-2900-7000-8db1-a98694b18785', 4, 4.0),
    ('01942bd2-7380-7000-9bc4-5f97443907b8', 5, 5.0),
    ('01942d52-f900-7000-866e-07d6404d53c1', 6, 6.0);

-- Show chunks and verify calendar alignment
SELECT * FROM show_chunks('uuid_cal_events');

-- Show chunk constraints to verify alignment
SELECT (test.show_constraints(ch)).* FROM show_chunks('uuid_cal_events') ch;

-- Verify data with extracted timestamps
SELECT uuid_timestamp(id), device_id, temp
FROM uuid_cal_events ORDER BY id;

-- Test UUID v7 boundary values

-- Min UUID v7 time: 00000000-0000-7000-8000-000000000000 (1970-01-01 00:00:00 UTC)
BEGIN;
INSERT INTO uuid_cal_events VALUES ('00000000-0000-7000-8000-000000000000', 0, 0.0);
SELECT (test.show_constraints(ch)).* FROM show_chunks('uuid_cal_events') ch
WHERE ch::text LIKE '%_1_chunk';
ROLLBACK;

-- Max UUID v7 time: ffffffff-ffff-7000-8000-000000000000 (far future)
BEGIN;
INSERT INTO uuid_cal_events VALUES ('ffffffff-ffff-7000-8000-000000000000', 99, 99.0);
SELECT (test.show_constraints(ch)).* FROM show_chunks('uuid_cal_events') ch
ORDER BY ch DESC LIMIT 1;
ROLLBACK;

DROP TABLE uuid_cal_events;

-- Test UUID with monthly interval
CREATE TABLE uuid_monthly(
    id uuid PRIMARY KEY,
    data text
);

SELECT create_hypertable('uuid_monthly', 'id', chunk_time_interval => interval '1 month');

-- Insert data spanning multiple months
INSERT INTO uuid_monthly VALUES
    ('01942117-de80-7000-8121-f12b2b69dd96', 'jan'),  -- 2025-01-01
    ('0195f6e7-d400-7000-8000-000000000000', 'feb'),  -- ~2025-02-15
    ('01978b69-8000-7000-8000-000000000000', 'mar');  -- ~2025-03-15

SELECT * FROM show_chunks('uuid_monthly');
SELECT (test.show_constraints(ch)).* FROM show_chunks('uuid_monthly') ch;

DROP TABLE uuid_monthly;

---------------------------------------------------------------
-- SECTION 2: TIMESTAMPTZ PARTITIONING WITH CALENDAR ALIGNMENT
---------------------------------------------------------------

-- Test with various date_trunc compatible intervals

-- 2.1: Daily chunks
CREATE TABLE tz_daily(
    time timestamptz NOT NULL,
    device_id int,
    value float
);

SELECT create_hypertable('tz_daily', 'time', chunk_time_interval => interval '1 day');

-- Insert data spanning multiple days
INSERT INTO tz_daily VALUES
    ('2025-01-01 00:00:00 UTC', 1, 1.0),
    ('2025-01-01 23:59:59.999999 UTC', 1, 1.5),
    ('2025-01-02 00:00:00 UTC', 2, 2.0),
    ('2025-01-02 12:00:00 UTC', 2, 2.5),
    ('2025-01-03 00:00:00 UTC', 3, 3.0);

SELECT * FROM show_chunks('tz_daily');
SELECT (test.show_constraints(ch)).* FROM show_chunks('tz_daily') ch;

DROP TABLE tz_daily;

-- 2.2: Weekly chunks
CREATE TABLE tz_weekly(
    time timestamptz NOT NULL,
    device_id int,
    value float
);

SELECT create_hypertable('tz_weekly', 'time', chunk_time_interval => interval '1 week');

-- Insert data spanning multiple weeks (Monday start)
INSERT INTO tz_weekly VALUES
    ('2025-01-06 00:00:00 UTC', 1, 1.0),  -- Monday
    ('2025-01-12 23:59:59 UTC', 1, 1.5),  -- Sunday
    ('2025-01-13 00:00:00 UTC', 2, 2.0),  -- Monday (next week)
    ('2025-01-20 12:00:00 UTC', 3, 3.0);  -- Monday (week after)

SELECT * FROM show_chunks('tz_weekly');
SELECT (test.show_constraints(ch)).* FROM show_chunks('tz_weekly') ch;

DROP TABLE tz_weekly;

-- 2.3: Monthly chunks
CREATE TABLE tz_monthly(
    time timestamptz NOT NULL,
    device_id int,
    value float
);

SELECT create_hypertable('tz_monthly', 'time', chunk_time_interval => interval '1 month');

-- Insert data spanning multiple months including boundary conditions
INSERT INTO tz_monthly VALUES
    ('2025-01-01 00:00:00 UTC', 1, 1.0),
    ('2025-01-31 23:59:59.999999 UTC', 1, 1.5),
    ('2025-02-01 00:00:00 UTC', 2, 2.0),
    ('2025-02-28 12:00:00 UTC', 2, 2.5),  -- Feb (non-leap year 2025)
    ('2025-03-01 00:00:00 UTC', 3, 3.0);

SELECT * FROM show_chunks('tz_monthly');
SELECT (test.show_constraints(ch)).* FROM show_chunks('tz_monthly') ch;

DROP TABLE tz_monthly;

-- 2.4: Yearly chunks
CREATE TABLE tz_yearly(
    time timestamptz NOT NULL,
    device_id int,
    value float
);

SELECT create_hypertable('tz_yearly', 'time', chunk_time_interval => interval '1 year');

INSERT INTO tz_yearly VALUES
    ('2024-01-01 00:00:00 UTC', 1, 1.0),
    ('2024-12-31 23:59:59 UTC', 1, 1.5),
    ('2025-01-01 00:00:00 UTC', 2, 2.0),
    ('2026-06-15 12:00:00 UTC', 3, 3.0);

SELECT * FROM show_chunks('tz_yearly');
SELECT (test.show_constraints(ch)).* FROM show_chunks('tz_yearly') ch;

DROP TABLE tz_yearly;

-- 2.5: Hourly chunks
CREATE TABLE tz_hourly(
    time timestamptz NOT NULL,
    device_id int,
    value float
);

SELECT create_hypertable('tz_hourly', 'time', chunk_time_interval => interval '1 hour');

INSERT INTO tz_hourly VALUES
    ('2025-01-01 00:00:00 UTC', 1, 1.0),
    ('2025-01-01 00:59:59.999999 UTC', 1, 1.5),
    ('2025-01-01 01:00:00 UTC', 2, 2.0),
    ('2025-01-01 02:30:00 UTC', 3, 3.0);

SELECT * FROM show_chunks('tz_hourly');
SELECT (test.show_constraints(ch)).* FROM show_chunks('tz_hourly') ch;

DROP TABLE tz_hourly;

---------------------------------------------------------------
-- SECTION 3: TIMESTAMP (WITHOUT TIME ZONE) PARTITIONING
---------------------------------------------------------------

CREATE TABLE ts_daily(
    time timestamp NOT NULL,
    device_id int,
    value float
);

SELECT create_hypertable('ts_daily', 'time', chunk_time_interval => interval '1 day');

-- Insert data spanning multiple days
INSERT INTO ts_daily VALUES
    ('2025-01-01 00:00:00', 1, 1.0),
    ('2025-01-01 23:59:59.999999', 1, 1.5),
    ('2025-01-02 00:00:00', 2, 2.0),
    ('2025-01-02 12:00:00', 2, 2.5),
    ('2025-01-03 00:00:00', 3, 3.0);

SELECT * FROM show_chunks('ts_daily');
SELECT (test.show_constraints(ch)).* FROM show_chunks('ts_daily') ch;

-- Test timestamp boundary values
BEGIN;
INSERT INTO ts_daily VALUES ('4714-11-24 00:00:00 BC', -1, -1.0);  -- Min timestamp
SELECT (test.show_constraints(ch)).* FROM show_chunks('ts_daily') ch
ORDER BY 1 DESC LIMIT 1;
ROLLBACK;

DROP TABLE ts_daily;

-- Test monthly chunks with timestamp
CREATE TABLE ts_monthly(
    time timestamp NOT NULL,
    device_id int,
    value float
);

SELECT create_hypertable('ts_monthly', 'time', chunk_time_interval => interval '1 month');

INSERT INTO ts_monthly VALUES
    ('2025-01-15 12:00:00', 1, 1.0),
    ('2025-02-15 12:00:00', 2, 2.0),
    ('2025-03-15 12:00:00', 3, 3.0);

SELECT * FROM show_chunks('ts_monthly');
SELECT (test.show_constraints(ch)).* FROM show_chunks('ts_monthly') ch;

DROP TABLE ts_monthly;

---------------------------------------------------------------
-- SECTION 4: DATE PARTITIONING
---------------------------------------------------------------

CREATE TABLE date_daily(
    day date NOT NULL,
    device_id int,
    value float
);

SELECT create_hypertable('date_daily', 'day', chunk_time_interval => interval '1 day');

INSERT INTO date_daily VALUES
    ('2025-01-01', 1, 1.0),
    ('2025-01-02', 2, 2.0),
    ('2025-01-03', 3, 3.0);

SELECT * FROM show_chunks('date_daily');
SELECT (test.show_constraints(ch)).* FROM show_chunks('date_daily') ch;

DROP TABLE date_daily;

-- Test weekly chunks with date
CREATE TABLE date_weekly(
    day date NOT NULL,
    device_id int,
    value float
);

SELECT create_hypertable('date_weekly', 'day', chunk_time_interval => interval '1 week');

INSERT INTO date_weekly VALUES
    ('2025-01-06', 1, 1.0),  -- Monday
    ('2025-01-12', 1, 1.5),  -- Sunday
    ('2025-01-13', 2, 2.0),  -- Monday (next week)
    ('2025-01-20', 3, 3.0);  -- Monday (week after)

SELECT * FROM show_chunks('date_weekly');
SELECT (test.show_constraints(ch)).* FROM show_chunks('date_weekly') ch;

DROP TABLE date_weekly;

-- Test monthly chunks with date
CREATE TABLE date_monthly(
    day date NOT NULL,
    device_id int,
    value float
);

SELECT create_hypertable('date_monthly', 'day', chunk_time_interval => interval '1 month');

INSERT INTO date_monthly VALUES
    ('2025-01-15', 1, 1.0),
    ('2025-02-15', 2, 2.0),
    ('2025-03-15', 3, 3.0),
    ('2025-04-15', 4, 4.0);

SELECT * FROM show_chunks('date_monthly');
SELECT (test.show_constraints(ch)).* FROM show_chunks('date_monthly') ch;

DROP TABLE date_monthly;

-- Test date boundary values
CREATE TABLE date_boundaries(
    day date NOT NULL,
    value int
);

SELECT create_hypertable('date_boundaries', 'day', chunk_time_interval => interval '1 year');

-- Test far past and future dates
BEGIN;
INSERT INTO date_boundaries VALUES ('0001-01-01', 1);
INSERT INTO date_boundaries VALUES ('9999-12-31', 2);
SELECT * FROM show_chunks('date_boundaries');
ROLLBACK;

DROP TABLE date_boundaries;

---------------------------------------------------------------
-- SECTION 5: TIMEZONE AND DAYLIGHT SAVING TIME (DST) TESTS
---------------------------------------------------------------

-- Save and set known timezone for consistent results
SET timezone = 'UTC';

CREATE TABLE tz_dst_test(
    time timestamptz NOT NULL,
    device_id int,
    value float
);

SELECT create_hypertable('tz_dst_test', 'time', chunk_time_interval => interval '1 day');

-- Insert data around US DST transitions (America/New_York)
-- Spring forward: March 9, 2025 2:00 AM -> 3:00 AM
-- Fall back: November 2, 2025 2:00 AM -> 1:00 AM

-- Data around spring forward (March 9, 2025)
INSERT INTO tz_dst_test VALUES
    ('2025-03-08 23:00:00 America/New_York', 1, 1.0),  -- Day before DST
    ('2025-03-09 01:00:00 America/New_York', 2, 2.0),  -- Before spring forward
    ('2025-03-09 03:00:00 America/New_York', 3, 3.0),  -- After spring forward (2 AM doesn't exist)
    ('2025-03-09 12:00:00 America/New_York', 4, 4.0),  -- Middle of day
    ('2025-03-10 00:00:00 America/New_York', 5, 5.0);  -- Day after DST

-- Data around fall back (November 2, 2025)
INSERT INTO tz_dst_test VALUES
    ('2025-11-01 23:00:00 America/New_York', 6, 6.0),  -- Day before DST
    ('2025-11-02 00:30:00 America/New_York', 7, 7.0),  -- Before fall back
    ('2025-11-02 01:30:00 EST', 8, 8.0),               -- After fall back (1:30 AM happens twice)
    ('2025-11-02 03:00:00 America/New_York', 9, 9.0),  -- After fall back
    ('2025-11-03 00:00:00 America/New_York', 10, 10.0); -- Day after DST

-- Show chunks in UTC
SELECT * FROM show_chunks('tz_dst_test');

-- Verify chunk boundaries
SELECT (test.show_constraints(ch)).* FROM show_chunks('tz_dst_test') ch;

-- Query data with different timezone settings
SET timezone = 'America/New_York';
SELECT time, device_id, value FROM tz_dst_test
WHERE time >= '2025-03-08' AND time < '2025-03-11'
ORDER BY time;

SET timezone = 'UTC';
SELECT time, device_id, value FROM tz_dst_test
WHERE time >= '2025-03-08' AND time < '2025-03-11'
ORDER BY time;

DROP TABLE tz_dst_test;

-- Test with European DST (Europe/Berlin)
-- Spring forward: March 30, 2025 2:00 AM -> 3:00 AM
-- Fall back: October 26, 2025 3:00 AM -> 2:00 AM

CREATE TABLE tz_eu_dst_test(
    time timestamptz NOT NULL,
    device_id int,
    value float
);

SELECT create_hypertable('tz_eu_dst_test', 'time', chunk_time_interval => interval '1 day');

-- Data around European spring forward (March 30, 2025)
INSERT INTO tz_eu_dst_test VALUES
    ('2025-03-29 23:00:00 Europe/Berlin', 1, 1.0),  -- Day before DST
    ('2025-03-30 01:00:00 Europe/Berlin', 2, 2.0),  -- Before spring forward
    ('2025-03-30 03:00:00 Europe/Berlin', 3, 3.0),  -- After spring forward
    ('2025-03-30 12:00:00 Europe/Berlin', 4, 4.0),  -- Middle of day
    ('2025-03-31 00:00:00 Europe/Berlin', 5, 5.0);  -- Day after DST

-- Data around European fall back (October 26, 2025)
INSERT INTO tz_eu_dst_test VALUES
    ('2025-10-25 23:00:00 Europe/Berlin', 6, 6.0),  -- Day before DST
    ('2025-10-26 01:30:00 Europe/Berlin', 7, 7.0),  -- Before fall back
    ('2025-10-26 02:30:00 CET', 8, 8.0),            -- After fall back (2:30 happens twice)
    ('2025-10-26 04:00:00 Europe/Berlin', 9, 9.0),  -- After fall back
    ('2025-10-27 00:00:00 Europe/Berlin', 10, 10.0); -- Day after DST

SELECT * FROM show_chunks('tz_eu_dst_test');

SET timezone = 'Europe/Berlin';
SELECT time, device_id, value FROM tz_eu_dst_test
WHERE time >= '2025-03-29' AND time < '2025-04-01'
ORDER BY time;

SET timezone = 'UTC';

DROP TABLE tz_eu_dst_test;

---------------------------------------------------------------
-- SECTION 6: CHUNK ALIGNMENT WITH DIFFERENT TIMEZONES
---------------------------------------------------------------

-- Test that chunks align properly when created in different timezones

CREATE TABLE tz_align_test(
    time timestamptz NOT NULL,
    device_id int,
    value float
);

SELECT create_hypertable('tz_align_test', 'time', chunk_time_interval => interval '1 day');

-- Insert data with explicit timezone
SET timezone = 'America/Los_Angeles';
INSERT INTO tz_align_test VALUES
    ('2025-01-01 00:00:00 America/Los_Angeles', 1, 1.0),
    ('2025-01-01 23:59:59 America/Los_Angeles', 2, 2.0),
    ('2025-01-02 00:00:00 America/Los_Angeles', 3, 3.0);

-- Switch timezone and verify chunk boundaries
SET timezone = 'Asia/Tokyo';
INSERT INTO tz_align_test VALUES
    ('2025-01-01 00:00:00 Asia/Tokyo', 4, 4.0),
    ('2025-01-01 23:59:59 Asia/Tokyo', 5, 5.0);

SET timezone = 'UTC';

-- Show all chunks
SELECT * FROM show_chunks('tz_align_test');
SELECT (test.show_constraints(ch)).* FROM show_chunks('tz_align_test') ch;

-- Verify data distribution across chunks
SELECT ch.chunk_name, count(*) as rows
FROM (SELECT tableoid::regclass as chunk_name FROM tz_align_test) ch
GROUP BY ch.chunk_name
ORDER BY ch.chunk_name;

DROP TABLE tz_align_test;

---------------------------------------------------------------
-- SECTION 7: BOUNDARY VALUE TESTS FOR ALL TYPES
---------------------------------------------------------------

-- 7.1: TIMESTAMPTZ boundaries
CREATE TABLE tz_boundary(
    time timestamptz NOT NULL,
    value int
);

SELECT create_hypertable('tz_boundary', 'time', chunk_time_interval => interval '1 day');

-- Test exact day boundaries
INSERT INTO tz_boundary VALUES
    ('2025-01-01 00:00:00+00', 1),        -- Exact start of day
    ('2025-01-01 23:59:59.999999+00', 2), -- Just before midnight
    ('2025-01-02 00:00:00+00', 3);        -- Exact start of next day

SELECT time, value FROM tz_boundary ORDER BY time;
SELECT (test.show_constraints(ch)).* FROM show_chunks('tz_boundary') ch;

DROP TABLE tz_boundary;

-- 7.2: Test microsecond precision at boundaries
CREATE TABLE tz_microsec(
    time timestamptz NOT NULL,
    value int
);

SELECT create_hypertable('tz_microsec', 'time', chunk_time_interval => interval '1 second');

INSERT INTO tz_microsec VALUES
    ('2025-01-01 00:00:00.000000+00', 1),
    ('2025-01-01 00:00:00.999999+00', 2),
    ('2025-01-01 00:00:01.000000+00', 3),
    ('2025-01-01 00:00:01.000001+00', 4);

SELECT * FROM show_chunks('tz_microsec');
SELECT time, value FROM tz_microsec ORDER BY time;

DROP TABLE tz_microsec;

-- 7.3: Test minute-level chunks
CREATE TABLE tz_minute(
    time timestamptz NOT NULL,
    value int
);

SELECT create_hypertable('tz_minute', 'time', chunk_time_interval => interval '1 minute');

INSERT INTO tz_minute VALUES
    ('2025-01-01 00:00:00+00', 1),
    ('2025-01-01 00:00:59+00', 2),
    ('2025-01-01 00:01:00+00', 3),
    ('2025-01-01 00:02:00+00', 4);

SELECT * FROM show_chunks('tz_minute');

DROP TABLE tz_minute;

---------------------------------------------------------------
-- SECTION 8: MULTI-INTERVAL TESTS (2 days, 2 weeks, etc.)
---------------------------------------------------------------

-- Test 2-day chunks
CREATE TABLE tz_2day(
    time timestamptz NOT NULL,
    value int
);

\set ON_ERROR_STOP 0
-- Note: 2 day interval may not be compatible with date_trunc
SELECT create_hypertable('tz_2day', 'time', chunk_time_interval => interval '2 days');
\set ON_ERROR_STOP 1

DROP TABLE IF EXISTS tz_2day;

-- Test 2-month chunks (should work with date_trunc logic)
CREATE TABLE tz_2month(
    time timestamptz NOT NULL,
    value int
);

\set ON_ERROR_STOP 0
SELECT create_hypertable('tz_2month', 'time', chunk_time_interval => interval '2 months');
\set ON_ERROR_STOP 1

DROP TABLE IF EXISTS tz_2month;

---------------------------------------------------------------
-- SECTION 9: COMPARISON WITH NON-CALENDAR CHUNKING
---------------------------------------------------------------

-- Disable calendar chunking for comparison
SET timescaledb.enable_calendar_chunking = false;

CREATE TABLE tz_no_cal(
    time timestamptz NOT NULL,
    value int
);

SELECT create_hypertable('tz_no_cal', 'time', chunk_time_interval => interval '1 day');

INSERT INTO tz_no_cal VALUES
    ('2025-01-01 12:00:00+00', 1),
    ('2025-01-02 12:00:00+00', 2),
    ('2025-01-03 12:00:00+00', 3);

-- Show non-calendar aligned chunks
SELECT * FROM show_chunks('tz_no_cal');
SELECT (test.show_constraints(ch)).* FROM show_chunks('tz_no_cal') ch;

DROP TABLE tz_no_cal;

-- Re-enable for calendar-aligned comparison
SET timescaledb.enable_calendar_chunking = true;

CREATE TABLE tz_cal(
    time timestamptz NOT NULL,
    value int
);

SELECT create_hypertable('tz_cal', 'time', chunk_time_interval => interval '1 day');

INSERT INTO tz_cal VALUES
    ('2025-01-01 12:00:00+00', 1),
    ('2025-01-02 12:00:00+00', 2),
    ('2025-01-03 12:00:00+00', 3);

-- Show calendar-aligned chunks
SELECT * FROM show_chunks('tz_cal');
SELECT (test.show_constraints(ch)).* FROM show_chunks('tz_cal') ch;

DROP TABLE tz_cal;

---------------------------------------------------------------
-- SECTION 10: EDGE CASES AND ERROR HANDLING
---------------------------------------------------------------

\set ON_ERROR_STOP 0

-- Test invalid interval type
CREATE TABLE invalid_interval(id uuid PRIMARY KEY);
SELECT create_hypertable('invalid_interval', 'id', chunk_time_interval => true);
DROP TABLE IF EXISTS invalid_interval;

-- Test inserting non-v7 UUID should fail
CREATE TABLE uuid_v7_only(id uuid PRIMARY KEY, value int);
SELECT create_hypertable('uuid_v7_only', 'id', chunk_time_interval => interval '1 day');
INSERT INTO uuid_v7_only VALUES ('a8961135-cd89-4c4b-aa05-79df642407dd', 1);  -- v4 UUID
DROP TABLE IF EXISTS uuid_v7_only;

\set ON_ERROR_STOP 1

---------------------------------------------------------------
-- SECTION 11: LEAP YEAR BOUNDARY TESTS
---------------------------------------------------------------

CREATE TABLE leap_year_test(
    time timestamptz NOT NULL,
    value int
);

SELECT create_hypertable('leap_year_test', 'time', chunk_time_interval => interval '1 month');

-- Test February boundaries in leap year (2024)
INSERT INTO leap_year_test VALUES
    ('2024-02-28 12:00:00+00', 1),  -- Feb 28 in leap year
    ('2024-02-29 12:00:00+00', 2),  -- Feb 29 in leap year
    ('2024-03-01 00:00:00+00', 3);  -- March 1

SELECT * FROM show_chunks('leap_year_test');
SELECT (test.show_constraints(ch)).* FROM show_chunks('leap_year_test') ch;

-- Test February boundaries in non-leap year (2025)
INSERT INTO leap_year_test VALUES
    ('2025-02-28 12:00:00+00', 4),  -- Feb 28 in non-leap year
    ('2025-03-01 00:00:00+00', 5);  -- March 1

SELECT * FROM show_chunks('leap_year_test');

DROP TABLE leap_year_test;

---------------------------------------------------------------
-- SECTION 12: YEAR BOUNDARY TESTS
---------------------------------------------------------------

CREATE TABLE year_boundary_test(
    time timestamptz NOT NULL,
    value int
);

SELECT create_hypertable('year_boundary_test', 'time', chunk_time_interval => interval '1 year');

-- Test year boundaries
INSERT INTO year_boundary_test VALUES
    ('2024-12-31 23:59:59.999999+00', 1),  -- Last moment of 2024
    ('2025-01-01 00:00:00+00', 2),          -- First moment of 2025
    ('2025-12-31 23:59:59.999999+00', 3),  -- Last moment of 2025
    ('2026-01-01 00:00:00+00', 4);          -- First moment of 2026

SELECT * FROM show_chunks('year_boundary_test');
SELECT (test.show_constraints(ch)).* FROM show_chunks('year_boundary_test') ch;

DROP TABLE year_boundary_test;

---------------------------------------------------------------
-- SECTION 13: TIMEZONE OFFSET BOUNDARY TESTS
---------------------------------------------------------------

-- Test with positive and negative timezone offsets at day boundaries

CREATE TABLE offset_boundary_test(
    time timestamptz NOT NULL,
    device_id int,
    value float
);

SELECT create_hypertable('offset_boundary_test', 'time', chunk_time_interval => interval '1 day');

-- Insert at midnight in various timezones
-- These will fall into different chunks based on UTC alignment
INSERT INTO offset_boundary_test VALUES
    ('2025-01-01 00:00:00+00', 1, 1.0),    -- UTC midnight
    ('2025-01-01 00:00:00+05:30', 2, 2.0), -- India (IST) midnight = Dec 31 18:30 UTC
    ('2025-01-01 00:00:00-08', 3, 3.0),    -- Pacific midnight = Jan 1 08:00 UTC
    ('2025-01-01 00:00:00+12', 4, 4.0),    -- New Zealand midnight = Dec 31 12:00 UTC
    ('2025-01-01 00:00:00-12', 5, 5.0);    -- Baker Island midnight = Jan 1 12:00 UTC

SELECT * FROM show_chunks('offset_boundary_test');

-- Display times in different zones to verify
SET timezone = 'UTC';
SELECT time AT TIME ZONE 'UTC' as utc_time,
       time AT TIME ZONE 'Asia/Kolkata' as ist_time,
       time AT TIME ZONE 'America/Los_Angeles' as pst_time,
       device_id, value
FROM offset_boundary_test
ORDER BY time;

DROP TABLE offset_boundary_test;

---------------------------------------------------------------
-- SECTION 14: CHUNK EXCLUSION WITH CALENDAR CHUNKS
---------------------------------------------------------------

CREATE TABLE chunk_excl_test(
    time timestamptz NOT NULL,
    device_id int,
    value float
);

SELECT create_hypertable('chunk_excl_test', 'time', chunk_time_interval => interval '1 day');

-- Insert data across multiple days
INSERT INTO chunk_excl_test
SELECT ts, (extract(epoch from ts)::int % 10), random()
FROM generate_series('2025-01-01'::timestamptz, '2025-01-10'::timestamptz, '6 hours') ts;

SELECT * FROM show_chunks('chunk_excl_test');

-- Test chunk exclusion with different predicates
EXPLAIN (COSTS OFF) SELECT * FROM chunk_excl_test WHERE time >= '2025-01-05' AND time < '2025-01-07';

-- Verify data in excluded range
SELECT count(*) FROM chunk_excl_test WHERE time >= '2025-01-05' AND time < '2025-01-07';

DROP TABLE chunk_excl_test;

---------------------------------------------------------------
-- CLEANUP
---------------------------------------------------------------

RESET timezone;
RESET timescaledb.enable_calendar_chunking;

\set VERBOSITY default
