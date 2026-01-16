-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--
-- Compare calc_range() output with time_bucket() to verify consistency
--
-- This test explores whether time_bucket can generate the same ranges as
-- calc_range for various intervals and corner cases including:
-- - Different time units (hours, days, weeks, months, years)
-- - Time zone handling
-- - Daylight saving time transitions
-- - Leap years
-- - Month boundary variations
--

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION calc_range(ts TIMESTAMPTZ, chunk_interval INTERVAL, origin TIMESTAMPTZ DEFAULT NULL, force_general BOOL DEFAULT NULL)
RETURNS TABLE(start_ts TIMESTAMPTZ, end_ts TIMESTAMPTZ) AS :MODULE_PATHNAME, 'ts_dimension_calculate_open_range_calendar' LANGUAGE C;
SET ROLE :ROLE_DEFAULT_PERM_USER;

SET timescaledb.enable_calendar_chunking = true;

---------------------------------------------------------------
-- COMPARISON HELPER FUNCTION
-- Compare calc_range output with time_bucket-based ranges
---------------------------------------------------------------

-- Note: time_bucket has two modes:
--   1. time_bucket(interval, ts, origin) - UTC-based bucketing
--   2. time_bucket(interval, ts, timezone, origin) - timezone-aware bucketing
--
-- For day/month intervals, calc_range does timezone conversion internally.
-- Both functions expect the origin in UTC format and convert it internally.
-- So we pass the origin directly without pre-conversion.
CREATE OR REPLACE FUNCTION compare_range_methods(
    ts TIMESTAMPTZ,
    bucket_interval INTERVAL,
    origin TIMESTAMPTZ DEFAULT '2000-01-01 00:00:00 UTC'::timestamptz,
    tz TEXT DEFAULT current_setting('timezone')
) RETURNS TABLE(
    calc_start TIMESTAMPTZ,
    calc_end TIMESTAMPTZ,
    calc_size INTERVAL,
    bucket_start TIMESTAMPTZ,
    bucket_end TIMESTAMPTZ,
    bucket_size INTERVAL,
    start_match BOOLEAN,
    size_match BOOLEAN
) AS $$
    SELECT
        cr.start_ts,
        cr.end_ts,
        cr.end_ts - cr.start_ts AS calc_size,
        time_bucket(bucket_interval, ts, tz, origin) AS tb_start,
        time_bucket(bucket_interval, ts, tz, origin) + bucket_interval AS tb_end,
        (time_bucket(bucket_interval, ts, tz, origin) + bucket_interval) - time_bucket(bucket_interval, ts, tz, origin) AS bucket_size,
        cr.start_ts = time_bucket(bucket_interval, ts, tz, origin),
        (cr.end_ts - cr.start_ts) = ((time_bucket(bucket_interval, ts, tz, origin) + bucket_interval) - time_bucket(bucket_interval, ts, tz, origin))
    FROM calc_range(ts, bucket_interval, origin) cr;
$$ LANGUAGE SQL;

-- For sub-day intervals, also compare with PostgreSQL's date_bin()
-- Both calc_range (for sub-day) and date_bin work in UTC microsecond space
-- without timezone conversion. Using the same raw origin should produce matching results.
CREATE OR REPLACE FUNCTION compare_subday_methods(
    ts TIMESTAMPTZ,
    bucket_interval INTERVAL,
    origin TIMESTAMPTZ DEFAULT '2000-01-01 00:00:00 UTC'::timestamptz,
    tz TEXT DEFAULT current_setting('timezone')
) RETURNS TABLE(
    calc_start TIMESTAMPTZ,
    tb_start TIMESTAMPTZ,
    datebin_start TIMESTAMPTZ,
    calc_vs_tb BOOLEAN,
    calc_vs_datebin BOOLEAN,
    tb_vs_datebin BOOLEAN
) AS $$
    SELECT
        cr.start_ts,
        time_bucket(bucket_interval, ts, tz, origin),
        date_bin(bucket_interval, ts, origin),
        cr.start_ts = time_bucket(bucket_interval, ts, tz, origin),
        cr.start_ts = date_bin(bucket_interval, ts, origin),
        time_bucket(bucket_interval, ts, tz, origin) = date_bin(bucket_interval, ts, origin)
    FROM calc_range(ts, bucket_interval, origin) cr;
$$ LANGUAGE SQL;

---------------------------------------------------------------
-- SECTION 1: SUB-DAY INTERVALS
-- Compare calc_range, time_bucket (with timezone), and date_bin
---------------------------------------------------------------

\echo '=== SUB-DAY INTERVALS (3-way comparison) ==='

-- Compare all three: calc_range, time_bucket (with timezone), and date_bin
SELECT
    test_name,
    ts,
    inv,
    (compare_subday_methods(ts, inv)).*
FROM (
    VALUES
        ('1 hour',     '2024-06-15 14:30:00 UTC'::timestamptz, '1 hour'::interval),
        ('2 hours',    '2024-06-15 14:30:00 UTC'::timestamptz, '2 hours'::interval),
        ('6 hours',    '2024-06-15 14:30:00 UTC'::timestamptz, '6 hours'::interval),
        ('12 hours',   '2024-06-15 14:30:00 UTC'::timestamptz, '12 hours'::interval),
        ('15 minutes', '2024-06-15 14:37:22 UTC'::timestamptz, '15 minutes'::interval),
        ('30 minutes', '2024-06-15 14:37:22 UTC'::timestamptz, '30 minutes'::interval)
) AS t(test_name, ts, inv);

---------------------------------------------------------------
-- SECTION 1b: SUB-DAY INTERVALS WITH LOCAL ORIGIN (3-way comparison)
-- Test with origin specified in local timezone
---------------------------------------------------------------

\echo '=== SUB-DAY INTERVALS WITH LOCAL ORIGIN (3-way comparison) ==='

-- Test in a non-UTC timezone with a local origin
SET timezone TO 'America/Los_Angeles';

SELECT
    test_name,
    ts,
    inv,
    origin,
    (compare_subday_methods(ts, inv, origin)).*
FROM (
    VALUES
        ('1 hour local',  '2024-06-15 14:30:00 UTC'::timestamptz, '1 hour'::interval,  '2000-01-01 00:00:00 America/Los_Angeles'::timestamptz),
        ('2 hours local', '2024-06-15 14:30:00 UTC'::timestamptz, '2 hours'::interval, '2000-01-01 00:00:00 America/Los_Angeles'::timestamptz),
        ('6 hours local', '2024-06-15 14:30:00 UTC'::timestamptz, '6 hours'::interval, '2000-01-01 00:00:00 America/Los_Angeles'::timestamptz),
        ('12 hours local','2024-06-15 14:30:00 UTC'::timestamptz, '12 hours'::interval,'2000-01-01 00:00:00 America/Los_Angeles'::timestamptz)
) AS t(test_name, ts, inv, origin);

SET timezone TO 'UTC';

---------------------------------------------------------------
-- SECTION 2: DAY INTERVALS
---------------------------------------------------------------

\echo '=== DAY INTERVALS ==='

SELECT
    test_name,
    ts,
    inv,
    (compare_range_methods(ts, inv)).*
FROM (
    VALUES
        ('1 day - mid day',    '2024-06-15 14:30:00 UTC'::timestamptz, '1 day'::interval),
        ('1 day - midnight',   '2024-06-15 00:00:00 UTC'::timestamptz, '1 day'::interval),
        ('1 day - end of day', '2024-06-15 23:59:59 UTC'::timestamptz, '1 day'::interval),
        ('7 days',             '2024-06-15 14:30:00 UTC'::timestamptz, '7 days'::interval),
        ('14 days',            '2024-06-15 14:30:00 UTC'::timestamptz, '14 days'::interval),
        ('30 days',            '2024-06-15 14:30:00 UTC'::timestamptz, '30 days'::interval)
) AS t(test_name, ts, inv);

---------------------------------------------------------------
-- SECTION 3: MONTH INTERVALS
-- This is where differences may appear due to variable month lengths
---------------------------------------------------------------

\echo '=== MONTH INTERVALS ==='

SELECT
    test_name,
    ts,
    inv,
    (compare_range_methods(ts, inv)).*
FROM (
    VALUES
        ('January',              '2024-01-15 12:00:00 UTC'::timestamptz, '1 month'::interval),
        ('February (leap)',      '2024-02-15 12:00:00 UTC'::timestamptz, '1 month'::interval),
        ('February (non-leap)',  '2023-02-15 12:00:00 UTC'::timestamptz, '1 month'::interval),
        ('March',                '2024-03-15 12:00:00 UTC'::timestamptz, '1 month'::interval),
        ('April (30 days)',      '2024-04-15 12:00:00 UTC'::timestamptz, '1 month'::interval),
        ('3 months (quarter)',   '2024-05-15 12:00:00 UTC'::timestamptz, '3 months'::interval),
        ('6 months (half year)', '2024-05-15 12:00:00 UTC'::timestamptz, '6 months'::interval)
) AS t(test_name, ts, inv);

---------------------------------------------------------------
-- SECTION 4: YEAR INTERVALS
---------------------------------------------------------------

\echo '=== YEAR INTERVALS ==='

SELECT
    test_name,
    ts,
    inv,
    (compare_range_methods(ts, inv)).*
FROM (
    VALUES
        ('regular',            '2024-06-15 12:00:00 UTC'::timestamptz, '1 year'::interval),
        ('leap year',          '2024-02-29 12:00:00 UTC'::timestamptz, '1 year'::interval),
        ('near year boundary', '2024-12-31 23:59:59 UTC'::timestamptz, '1 year'::interval)
) AS t(test_name, ts, inv);

---------------------------------------------------------------
-- SECTION 5: DAYLIGHT SAVING TIME TRANSITIONS
-- US Eastern: DST starts 2nd Sunday of March, ends 1st Sunday of November
---------------------------------------------------------------

\echo '=== DAYLIGHT SAVING TIME (America/New_York) ==='

SET timezone TO 'America/New_York';

SELECT
    test_name,
    ts,
    inv,
    (compare_range_methods(ts, inv)).*
FROM (
    VALUES
        -- Spring forward (March 10, 2024 at 2:00 AM EST -> 3:00 AM EDT)
        ('spring forward - before',    '2024-03-10 01:30:00 America/New_York'::timestamptz, '1 day'::interval),
        ('spring forward - after',     '2024-03-10 03:30:00 America/New_York'::timestamptz, '1 day'::interval),
        ('spring forward - day before','2024-03-09 12:00:00 America/New_York'::timestamptz, '1 day'::interval),
        ('spring forward - day after', '2024-03-11 12:00:00 America/New_York'::timestamptz, '1 day'::interval),
        -- Fall back (November 3, 2024 at 2:00 AM EDT -> 1:00 AM EST)
        ('fall back - before',         '2024-11-03 00:30:00 America/New_York'::timestamptz, '1 day'::interval),
        ('fall back - after',          '2024-11-03 03:30:00 America/New_York'::timestamptz, '1 day'::interval),
        -- Hour buckets during DST
        ('1h during spring forward',   '2024-03-10 01:30:00 America/New_York'::timestamptz, '1 hour'::interval),
        ('1h during fall back',        '2024-11-03 01:30:00 America/New_York'::timestamptz, '1 hour'::interval),
        -- Month buckets spanning DST
        ('1m spanning spring forward', '2024-03-15 12:00:00 America/New_York'::timestamptz, '1 month'::interval),
        ('1m spanning fall back',      '2024-11-15 12:00:00 America/New_York'::timestamptz, '1 month'::interval)
) AS t(test_name, ts, inv);

---------------------------------------------------------------
-- SECTION 5b: DST with LOCAL TIMEZONE ORIGIN
-- Using origin in local timezone instead of UTC
---------------------------------------------------------------

\echo '=== DST WITH LOCAL TIMEZONE ORIGIN (America/New_York) ==='

-- Use a local midnight origin instead of UTC origin
SELECT
    test_name,
    ts,
    inv,
    origin,
    (compare_range_methods(ts, inv, origin)).*
FROM (
    VALUES
        -- Spring forward with local origin
        ('spring fwd - local origin',  '2024-03-10 01:30:00 America/New_York'::timestamptz, '1 day'::interval,   '2000-01-01 00:00:00 America/New_York'::timestamptz),
        ('spring fwd - local origin',  '2024-03-10 03:30:00 America/New_York'::timestamptz, '1 day'::interval,   '2000-01-01 00:00:00 America/New_York'::timestamptz),
        ('spring fwd - 1h local',      '2024-03-10 01:30:00 America/New_York'::timestamptz, '1 hour'::interval,  '2000-01-01 00:00:00 America/New_York'::timestamptz),
        ('spring fwd - 1m local',      '2024-03-15 12:00:00 America/New_York'::timestamptz, '1 month'::interval, '2000-01-01 00:00:00 America/New_York'::timestamptz),
        -- Fall back with local origin
        ('fall back - local origin',   '2024-11-03 00:30:00 America/New_York'::timestamptz, '1 day'::interval,   '2000-01-01 00:00:00 America/New_York'::timestamptz),
        ('fall back - local origin',   '2024-11-03 03:30:00 America/New_York'::timestamptz, '1 day'::interval,   '2000-01-01 00:00:00 America/New_York'::timestamptz),
        ('fall back - 1h local',       '2024-11-03 01:30:00 America/New_York'::timestamptz, '1 hour'::interval,  '2000-01-01 00:00:00 America/New_York'::timestamptz),
        ('fall back - 1m local',       '2024-11-15 12:00:00 America/New_York'::timestamptz, '1 month'::interval, '2000-01-01 00:00:00 America/New_York'::timestamptz)
) AS t(test_name, ts, inv, origin);

SET timezone TO 'UTC';

---------------------------------------------------------------
-- SECTION 6: LEAP YEAR EDGE CASES
---------------------------------------------------------------

\echo '=== LEAP YEAR EDGE CASES ==='

SELECT
    test_name,
    ts,
    inv,
    (compare_range_methods(ts, inv)).*
FROM (
    VALUES
        -- February 29 in leap years
        ('Leap day - 1 day',           '2024-02-29 12:00:00 UTC'::timestamptz, '1 day'::interval),
        ('Leap day - 1 month',         '2024-02-29 12:00:00 UTC'::timestamptz, '1 month'::interval),
        ('Leap day - 1 year',          '2024-02-29 12:00:00 UTC'::timestamptz, '1 year'::interval),
        -- Day before and after leap day
        ('Feb 28 leap year',           '2024-02-28 12:00:00 UTC'::timestamptz, '1 day'::interval),
        ('Mar 1 leap year',            '2024-03-01 12:00:00 UTC'::timestamptz, '1 day'::interval),
        -- Non-leap year February
        ('Feb 28 non-leap - 1 day',    '2023-02-28 12:00:00 UTC'::timestamptz, '1 day'::interval),
        ('Feb 28 non-leap - 1 month',  '2023-02-28 12:00:00 UTC'::timestamptz, '1 month'::interval)
) AS t(test_name, ts, inv);

---------------------------------------------------------------
-- SECTION 7: MONTH BOUNDARY VARIATIONS
-- Different months have 28, 29, 30, or 31 days
---------------------------------------------------------------

\echo '=== MONTH BOUNDARY VARIATIONS ==='

SELECT
    test_name,
    ts,
    inv,
    (compare_range_methods(ts, inv)).*
FROM (
    VALUES
        -- Last day of various months
        ('Jan 31', '2024-01-31 12:00:00 UTC'::timestamptz, '1 month'::interval),
        ('Mar 31', '2024-03-31 12:00:00 UTC'::timestamptz, '1 month'::interval),
        ('Apr 30', '2024-04-30 12:00:00 UTC'::timestamptz, '1 month'::interval),
        ('May 31', '2024-05-31 12:00:00 UTC'::timestamptz, '1 month'::interval),
        -- First day of various months
        ('Jan 1',  '2024-01-01 00:00:00 UTC'::timestamptz, '1 month'::interval),
        ('Feb 1',  '2024-02-01 00:00:00 UTC'::timestamptz, '1 month'::interval),
        ('Mar 1',  '2024-03-01 00:00:00 UTC'::timestamptz, '1 month'::interval)
) AS t(test_name, ts, inv);

---------------------------------------------------------------
-- SECTION 8: YEAR BOUNDARY
---------------------------------------------------------------

\echo '=== YEAR BOUNDARY ==='

SELECT
    test_name,
    ts,
    inv,
    (compare_range_methods(ts, inv)).*
FROM (
    VALUES
        ('Dec 31 - 1 day',   '2024-12-31 12:00:00 UTC'::timestamptz, '1 day'::interval),
        ('Jan 1 - 1 day',    '2024-01-01 00:00:00 UTC'::timestamptz, '1 day'::interval),
        ('Dec 31 - 1 month', '2024-12-31 12:00:00 UTC'::timestamptz, '1 month'::interval),
        ('Dec 31 - 1 year',  '2024-12-31 12:00:00 UTC'::timestamptz, '1 year'::interval),
        ('New Year - 1 day', '2024-12-31 23:59:59.999999 UTC'::timestamptz, '1 day'::interval),
        ('New Year - 1 hour','2025-01-01 00:00:00 UTC'::timestamptz, '1 hour'::interval)
) AS t(test_name, ts, inv);

---------------------------------------------------------------
-- SECTION 9: CUSTOM ORIGINS
---------------------------------------------------------------

\echo '=== CUSTOM ORIGINS ==='

SELECT
    test_name,
    ts,
    inv,
    origin,
    (compare_range_methods(ts, inv, origin)).*
FROM (
    VALUES
        -- Fiscal year origin (April 1)
        ('Fiscal Apr 1 - 1m',  '2024-05-15 12:00:00 UTC'::timestamptz, '1 month'::interval,  '2024-04-01 00:00:00 UTC'::timestamptz),
        ('Fiscal Apr 1 - 3m',  '2024-05-15 12:00:00 UTC'::timestamptz, '3 months'::interval, '2024-04-01 00:00:00 UTC'::timestamptz),
        -- Mid-week origin (Wednesday)
        ('Wednesday - 7 days', '2024-06-17 12:00:00 UTC'::timestamptz, '7 days'::interval,   '2024-01-03 00:00:00 UTC'::timestamptz),
        -- Non-midnight origin
        ('Noon origin - 1 day','2024-06-15 14:00:00 UTC'::timestamptz, '1 day'::interval,    '2024-01-01 12:00:00 UTC'::timestamptz),
        ('Noon origin - 1 hour','2024-06-15 14:30:00 UTC'::timestamptz,'1 hour'::interval,   '2024-01-01 12:30:00 UTC'::timestamptz)
) AS t(test_name, ts, inv, origin);

---------------------------------------------------------------
-- SECTION 10: DIFFERENT TIMEZONES (UTC origin)
---------------------------------------------------------------

\echo '=== DIFFERENT TIMEZONES (UTC origin) ==='

-- Europe (CET/CEST)
SET timezone TO 'Europe/Berlin';

SELECT
    test_name,
    ts,
    inv,
    (compare_range_methods(ts, inv)).*
FROM (
    VALUES
        ('Berlin - 1 day',         '2024-06-15 14:00:00 Europe/Berlin'::timestamptz, '1 day'::interval),
        ('Berlin - 1 month',       '2024-06-15 14:00:00 Europe/Berlin'::timestamptz, '1 month'::interval),
        ('Berlin DST spring',      '2024-03-31 03:00:00 Europe/Berlin'::timestamptz, '1 day'::interval)
) AS t(test_name, ts, inv);

SET timezone TO 'UTC';

-- Asia (no DST)
SET timezone TO 'Asia/Tokyo';

SELECT
    test_name,
    ts,
    inv,
    (compare_range_methods(ts, inv)).*
FROM (
    VALUES
        ('Tokyo - 1 day',   '2024-06-15 14:00:00 Asia/Tokyo'::timestamptz, '1 day'::interval),
        ('Tokyo - 1 month', '2024-06-15 14:00:00 Asia/Tokyo'::timestamptz, '1 month'::interval)
) AS t(test_name, ts, inv);

SET timezone TO 'UTC';

-- Australia (opposite DST)
SET timezone TO 'Australia/Sydney';

SELECT
    test_name,
    ts,
    inv,
    (compare_range_methods(ts, inv)).*
FROM (
    VALUES
        ('Sydney - 1 day',      '2024-06-15 14:00:00 Australia/Sydney'::timestamptz, '1 day'::interval),
        ('Sydney DST start',    '2024-10-06 03:00:00 Australia/Sydney'::timestamptz, '1 day'::interval)
) AS t(test_name, ts, inv);

SET timezone TO 'UTC';

---------------------------------------------------------------
-- SECTION 10b: DIFFERENT TIMEZONES (local origin)
---------------------------------------------------------------

\echo '=== DIFFERENT TIMEZONES (local origin) ==='

-- Europe (CET/CEST) with local origin
SET timezone TO 'Europe/Berlin';

SELECT
    test_name,
    ts,
    inv,
    origin,
    (compare_range_methods(ts, inv, origin)).*
FROM (
    VALUES
        ('Berlin local - 1 day',    '2024-06-15 14:00:00 Europe/Berlin'::timestamptz, '1 day'::interval,   '2000-01-01 00:00:00 Europe/Berlin'::timestamptz),
        ('Berlin local - 1 month',  '2024-06-15 14:00:00 Europe/Berlin'::timestamptz, '1 month'::interval, '2000-01-01 00:00:00 Europe/Berlin'::timestamptz),
        ('Berlin DST local',        '2024-03-31 03:00:00 Europe/Berlin'::timestamptz, '1 day'::interval,   '2000-01-01 00:00:00 Europe/Berlin'::timestamptz)
) AS t(test_name, ts, inv, origin);

SET timezone TO 'UTC';

-- Asia (no DST) with local origin
SET timezone TO 'Asia/Tokyo';

SELECT
    test_name,
    ts,
    inv,
    origin,
    (compare_range_methods(ts, inv, origin)).*
FROM (
    VALUES
        ('Tokyo local - 1 day',   '2024-06-15 14:00:00 Asia/Tokyo'::timestamptz, '1 day'::interval,   '2000-01-01 00:00:00 Asia/Tokyo'::timestamptz),
        ('Tokyo local - 1 month', '2024-06-15 14:00:00 Asia/Tokyo'::timestamptz, '1 month'::interval, '2000-01-01 00:00:00 Asia/Tokyo'::timestamptz)
) AS t(test_name, ts, inv, origin);

SET timezone TO 'UTC';

-- Australia (opposite DST) with local origin
SET timezone TO 'Australia/Sydney';

SELECT
    test_name,
    ts,
    inv,
    origin,
    (compare_range_methods(ts, inv, origin)).*
FROM (
    VALUES
        ('Sydney local - 1 day',   '2024-06-15 14:00:00 Australia/Sydney'::timestamptz, '1 day'::interval,   '2000-01-01 00:00:00 Australia/Sydney'::timestamptz),
        ('Sydney DST local',       '2024-10-06 03:00:00 Australia/Sydney'::timestamptz, '1 day'::interval,   '2000-01-01 00:00:00 Australia/Sydney'::timestamptz)
) AS t(test_name, ts, inv, origin);

SET timezone TO 'UTC';

---------------------------------------------------------------
-- SECTION 10c: UNUSUAL TIMEZONES (local origin)
---------------------------------------------------------------

\echo '=== UNUSUAL TIMEZONES (local origin) ==='

-- India (UTC+5:30, no DST, half-hour offset)
SET timezone TO 'Asia/Kolkata';

SELECT
    test_name,
    ts,
    inv,
    origin,
    (compare_range_methods(ts, inv, origin)).*
FROM (
    VALUES
        ('Kolkata - 1 day',   '2024-06-15 14:00:00 Asia/Kolkata'::timestamptz, '1 day'::interval,   '2000-01-01 00:00:00 Asia/Kolkata'::timestamptz),
        ('Kolkata - 1 month', '2024-06-15 14:00:00 Asia/Kolkata'::timestamptz, '1 month'::interval, '2000-01-01 00:00:00 Asia/Kolkata'::timestamptz),
        ('Kolkata - 1 hour',  '2024-06-15 14:30:00 Asia/Kolkata'::timestamptz, '1 hour'::interval,  '2000-01-01 00:00:00 Asia/Kolkata'::timestamptz)
) AS t(test_name, ts, inv, origin);

SET timezone TO 'UTC';

-- Nepal (UTC+5:45, no DST, 45-minute offset)
SET timezone TO 'Asia/Kathmandu';

SELECT
    test_name,
    ts,
    inv,
    origin,
    (compare_range_methods(ts, inv, origin)).*
FROM (
    VALUES
        ('Kathmandu - 1 day',   '2024-06-15 14:00:00 Asia/Kathmandu'::timestamptz, '1 day'::interval,   '2000-01-01 00:00:00 Asia/Kathmandu'::timestamptz),
        ('Kathmandu - 1 month', '2024-06-15 14:00:00 Asia/Kathmandu'::timestamptz, '1 month'::interval, '2000-01-01 00:00:00 Asia/Kathmandu'::timestamptz),
        ('Kathmandu - 1 hour',  '2024-06-15 14:30:00 Asia/Kathmandu'::timestamptz, '1 hour'::interval,  '2000-01-01 00:00:00 Asia/Kathmandu'::timestamptz)
) AS t(test_name, ts, inv, origin);

SET timezone TO 'UTC';

-- Pacific/Auckland (New Zealand, DST opposite to northern hemisphere)
SET timezone TO 'Pacific/Auckland';

SELECT
    test_name,
    ts,
    inv,
    origin,
    (compare_range_methods(ts, inv, origin)).*
FROM (
    VALUES
        -- NZ winter (no DST)
        ('Auckland winter - 1 day',   '2024-06-15 14:00:00 Pacific/Auckland'::timestamptz, '1 day'::interval,   '2000-01-01 00:00:00 Pacific/Auckland'::timestamptz),
        ('Auckland winter - 1 month', '2024-06-15 14:00:00 Pacific/Auckland'::timestamptz, '1 month'::interval, '2000-01-01 00:00:00 Pacific/Auckland'::timestamptz),
        -- NZ DST transition (last Sunday of September)
        ('Auckland DST start',        '2024-09-29 03:00:00 Pacific/Auckland'::timestamptz, '1 day'::interval,   '2000-01-01 00:00:00 Pacific/Auckland'::timestamptz),
        -- NZ DST end (first Sunday of April)
        ('Auckland DST end',          '2024-04-07 03:00:00 Pacific/Auckland'::timestamptz, '1 day'::interval,   '2000-01-01 00:00:00 Pacific/Auckland'::timestamptz)
) AS t(test_name, ts, inv, origin);

SET timezone TO 'UTC';

-- America/Los_Angeles (Pacific Time, different DST dates than Eastern)
SET timezone TO 'America/Los_Angeles';

SELECT
    test_name,
    ts,
    inv,
    origin,
    (compare_range_methods(ts, inv, origin)).*
FROM (
    VALUES
        ('LA summer - 1 day',   '2024-06-15 14:00:00 America/Los_Angeles'::timestamptz, '1 day'::interval,   '2000-01-01 00:00:00 America/Los_Angeles'::timestamptz),
        ('LA summer - 1 month', '2024-06-15 14:00:00 America/Los_Angeles'::timestamptz, '1 month'::interval, '2000-01-01 00:00:00 America/Los_Angeles'::timestamptz),
        ('LA DST spring',       '2024-03-10 03:00:00 America/Los_Angeles'::timestamptz, '1 day'::interval,   '2000-01-01 00:00:00 America/Los_Angeles'::timestamptz),
        ('LA DST fall',         '2024-11-03 01:30:00 America/Los_Angeles'::timestamptz, '1 day'::interval,   '2000-01-01 00:00:00 America/Los_Angeles'::timestamptz)
) AS t(test_name, ts, inv, origin);

SET timezone TO 'UTC';

---------------------------------------------------------------
-- SECTION 11: EXTREME TIMESTAMPS
---------------------------------------------------------------

\echo '=== EXTREME TIMESTAMPS ==='

SELECT
    test_name,
    ts,
    inv,
    (compare_range_methods(ts, inv)).*
FROM (
    VALUES
        ('Year 1000 - 1 day',   '1000-06-15 12:00:00 UTC'::timestamptz, '1 day'::interval),
        ('Year 1000 - 1 month', '1000-06-15 12:00:00 UTC'::timestamptz, '1 month'::interval),
        ('Year 3000 - 1 day',   '3000-06-15 12:00:00 UTC'::timestamptz, '1 day'::interval),
        ('Year 3000 - 1 month', '3000-06-15 12:00:00 UTC'::timestamptz, '1 month'::interval)
) AS t(test_name, ts, inv);

---------------------------------------------------------------
-- SECTION 12: SUMMARY - All mismatches (using timezone-aware time_bucket)
---------------------------------------------------------------

\echo '=== SUMMARY: ALL MISMATCHES ==='

-- Test across many timestamps and intervals, show only mismatches
-- Using 'UTC' timezone for time_bucket to match session timezone
SELECT
    ts,
    inv,
    cr.start_ts as calc_start,
    cr.end_ts as calc_end,
    time_bucket(inv, ts, 'UTC', '2000-01-01 UTC'::timestamptz) as bucket_start,
    time_bucket(inv, ts, 'UTC', '2000-01-01 UTC'::timestamptz) + inv as bucket_end,
    cr.start_ts - time_bucket(inv, ts, 'UTC', '2000-01-01 UTC'::timestamptz) as start_diff,
    cr.end_ts - (time_bucket(inv, ts, 'UTC', '2000-01-01 UTC'::timestamptz) + inv) as end_diff
FROM (
    VALUES
        ('2024-01-15 12:00:00 UTC'::timestamptz),
        ('2024-02-29 12:00:00 UTC'::timestamptz),
        ('2024-06-15 00:00:00 UTC'::timestamptz),
        ('2024-12-31 23:59:59 UTC'::timestamptz)
) AS timestamps(ts)
CROSS JOIN (
    VALUES
        ('1 hour'::interval),
        ('1 day'::interval),
        ('7 days'::interval),
        ('1 month'::interval),
        ('3 months'::interval),
        ('1 year'::interval)
) AS intervals(inv)
CROSS JOIN LATERAL calc_range(ts, inv, '2000-01-01 UTC'::timestamptz) cr
WHERE cr.start_ts != time_bucket(inv, ts, 'UTC', '2000-01-01 UTC'::timestamptz)
   OR cr.end_ts != time_bucket(inv, ts, 'UTC', '2000-01-01 UTC'::timestamptz) + inv;

---------------------------------------------------------------
-- SECTION 13: DETAILED MONTH COMPARISON
-- Month intervals for every month in 2024
---------------------------------------------------------------

\echo '=== DETAILED MONTH COMPARISON (2024) ==='

SELECT
    month_start,
    cr.start_ts as calc_start,
    cr.end_ts as calc_end,
    time_bucket('1 month', month_start, 'UTC', '2000-01-01 UTC'::timestamptz) as bucket_start,
    time_bucket('1 month', month_start, 'UTC', '2000-01-01 UTC'::timestamptz) + '1 month'::interval as bucket_end,
    cr.end_ts - cr.start_ts as calc_duration,
    cr.start_ts = time_bucket('1 month', month_start, 'UTC', '2000-01-01 UTC'::timestamptz) as start_match,
    cr.end_ts = time_bucket('1 month', month_start, 'UTC', '2000-01-01 UTC'::timestamptz) + '1 month'::interval as end_match
FROM generate_series('2024-01-15'::timestamptz, '2024-12-15'::timestamptz, '1 month'::interval) as month_start
CROSS JOIN LATERAL calc_range(month_start, '1 month'::interval, '2000-01-01 UTC'::timestamptz) cr;

---------------------------------------------------------------
-- SECTION 14: TIME_BUCKET SUB-DAY UNEXPECTED BEHAVIOR
-- Demonstrates how timezone-aware time_bucket produces unexpected
-- results for sub-day intervals due to DST transitions
---------------------------------------------------------------

\echo '=== TIME_BUCKET SUB-DAY UNEXPECTED BEHAVIOR ==='

SET timezone TO 'America/New_York';

-- Example 1: DST offset causes bucket misalignment
-- Origin is in EST (UTC-5), timestamp is in EDT (UTC-4)
-- time_bucket shifts buckets by 1 hour relative to UTC-based methods
\echo '--- Example 1: DST offset causes 1-hour bucket shift ---'

SELECT
    'Winter (EST)' as period,
    ts,
    time_bucket('2 hours', ts, 'America/New_York', origin) as tb_start,
    date_bin('2 hours', ts, origin) as datebin_start,
    time_bucket('2 hours', ts, 'America/New_York', origin) = date_bin('2 hours', ts, origin) as match
FROM (VALUES
    ('2024-01-15 14:30:00 America/New_York'::timestamptz, '2000-01-01 00:00:00 America/New_York'::timestamptz)
) AS t(ts, origin)
UNION ALL
SELECT
    'Summer (EDT)' as period,
    ts,
    time_bucket('2 hours', ts, 'America/New_York', origin) as tb_start,
    date_bin('2 hours', ts, origin) as datebin_start,
    time_bucket('2 hours', ts, 'America/New_York', origin) = date_bin('2 hours', ts, origin) as match
FROM (VALUES
    ('2024-07-15 14:30:00 America/New_York'::timestamptz, '2000-01-01 00:00:00 America/New_York'::timestamptz)
) AS t(ts, origin);

-- Example 2: Same UTC timestamp, different bucket due to DST
-- Shows that time_bucket's result depends on whether timestamp is in DST or not
\echo '--- Example 2: Same UTC time, different local bucket ---'

SELECT
    label,
    ts,
    ts AT TIME ZONE 'America/New_York' as local_time,
    time_bucket('6 hours', ts, 'America/New_York', '2000-01-01 00:00:00 America/New_York'::timestamptz) as tb_start,
    date_bin('6 hours', ts, '2000-01-01 00:00:00 America/New_York'::timestamptz) as datebin_start
FROM (VALUES
    ('January (EST)', '2024-01-15 12:00:00 UTC'::timestamptz),
    ('July (EDT)',    '2024-07-15 12:00:00 UTC'::timestamptz)
) AS t(label, ts);

-- Example 3: Bucket boundaries near DST spring forward
-- 2:00 AM local doesn't exist on March 10, 2024
\echo '--- Example 3: Buckets near DST spring forward (2:00 AM gap) ---'

SELECT
    label,
    ts,
    time_bucket('1 hour', ts, 'America/New_York', '2000-01-01 00:00:00 America/New_York'::timestamptz) as tb_start,
    time_bucket('1 hour', ts, 'America/New_York', '2000-01-01 00:00:00 America/New_York'::timestamptz) + '1 hour'::interval as tb_end,
    date_bin('1 hour', ts, '2000-01-01 00:00:00 America/New_York'::timestamptz) as datebin_start,
    date_bin('1 hour', ts, '2000-01-01 00:00:00 America/New_York'::timestamptz) + '1 hour'::interval as datebin_end
FROM (VALUES
    ('1:30 AM EST (before gap)', '2024-03-10 01:30:00 America/New_York'::timestamptz),
    ('3:30 AM EDT (after gap)',  '2024-03-10 03:30:00 America/New_York'::timestamptz)
) AS t(label, ts);

-- Example 4: Bucket boundaries near DST fall back
-- 1:00-2:00 AM local happens twice on November 3, 2024
\echo '--- Example 4: Buckets near DST fall back (1:00 AM repeated) ---'

SELECT
    label,
    ts,
    time_bucket('1 hour', ts, 'America/New_York', '2000-01-01 00:00:00 America/New_York'::timestamptz) as tb_start,
    date_bin('1 hour', ts, '2000-01-01 00:00:00 America/New_York'::timestamptz) as datebin_start
FROM (VALUES
    ('12:30 AM EDT (before)',     '2024-11-03 00:30:00 America/New_York'::timestamptz),
    ('1:30 AM EDT (first)',       '2024-11-03 01:30:00-04'::timestamptz),  -- explicit EDT
    ('1:30 AM EST (second)',      '2024-11-03 01:30:00-05'::timestamptz),  -- explicit EST
    ('2:30 AM EST (after)',       '2024-11-03 02:30:00 America/New_York'::timestamptz)
) AS t(label, ts);

-- Example 5: Consistency check - date_bin always produces fixed-size buckets
-- time_bucket alignment varies based on DST state
\echo '--- Example 5: Bucket size consistency across DST ---'

SELECT
    label,
    ts,
    time_bucket('3 hours', ts, 'America/New_York', '2000-01-01 00:00:00 America/New_York'::timestamptz) as tb_start,
    (time_bucket('3 hours', ts, 'America/New_York', '2000-01-01 00:00:00 America/New_York'::timestamptz) + '3 hours'::interval)
        - time_bucket('3 hours', ts, 'America/New_York', '2000-01-01 00:00:00 America/New_York'::timestamptz) as tb_size,
    date_bin('3 hours', ts, '2000-01-01 00:00:00 America/New_York'::timestamptz) as datebin_start,
    (date_bin('3 hours', ts, '2000-01-01 00:00:00 America/New_York'::timestamptz) + '3 hours'::interval)
        - date_bin('3 hours', ts, '2000-01-01 00:00:00 America/New_York'::timestamptz) as datebin_size
FROM (VALUES
    ('Normal day',        '2024-06-15 14:30:00 America/New_York'::timestamptz),
    ('Spring forward day','2024-03-10 14:30:00 America/New_York'::timestamptz),
    ('Fall back day',     '2024-11-03 14:30:00 America/New_York'::timestamptz)
) AS t(label, ts);

-- Example 6: Timestamp lands in bucket AFTER its own time
-- This violates the fundamental invariant: bucket_start <= timestamp < bucket_end
\echo '--- Example 6: Timestamp before its own bucket (BROKEN INVARIANT) ---'

SELECT
    ts,
    ts AT TIME ZONE 'UTC' as ts_utc,
    time_bucket('1 hour', ts, 'America/New_York',
                '2000-01-01 00:00:00 America/New_York'::timestamptz) as bucket_start,
    time_bucket('1 hour', ts, 'America/New_York',
                '2000-01-01 00:00:00 America/New_York'::timestamptz) AT TIME ZONE 'UTC' as bucket_utc,
    ts < time_bucket('1 hour', ts, 'America/New_York',
                     '2000-01-01 00:00:00 America/New_York'::timestamptz) as ts_before_bucket
FROM (VALUES ('2024-11-03 01:30:00-04'::timestamptz)) AS t(ts);

-- Example 7: Fall back creates effective 2-hour bucket
-- All timestamps from 05:00-07:00 UTC land in the same bucket
\echo '--- Example 7: Fall back creates 2-hour bucket ---'

SELECT
    label,
    ts AT TIME ZONE 'UTC' as ts_utc,
    time_bucket('1 hour', ts, 'America/New_York',
                '2000-01-01 00:00:00 America/New_York'::timestamptz) AT TIME ZONE 'UTC' as bucket_utc
FROM (VALUES
    ('00:30 EDT',             '2024-11-03 00:30:00-04'::timestamptz),
    ('01:00 EDT (first)',     '2024-11-03 01:00:00-04'::timestamptz),
    ('01:59 EDT (first)',     '2024-11-03 01:59:00-04'::timestamptz),
    ('01:00 EST (second)',    '2024-11-03 01:00:00-05'::timestamptz),
    ('01:59 EST (second)',    '2024-11-03 01:59:00-05'::timestamptz),
    ('02:00 EST',             '2024-11-03 02:00:00-05'::timestamptz)
) AS t(label, ts);

SET timezone TO 'UTC';

---------------------------------------------------------------
-- CLEANUP
---------------------------------------------------------------

DROP FUNCTION compare_range_methods(TIMESTAMPTZ, INTERVAL, TIMESTAMPTZ, TEXT);
DROP FUNCTION compare_subday_methods(TIMESTAMPTZ, INTERVAL, TIMESTAMPTZ, TEXT);

\c :TEST_DBNAME :ROLE_SUPERUSER
DROP FUNCTION calc_range(TIMESTAMPTZ, INTERVAL, TIMESTAMPTZ, BOOL);

RESET timescaledb.enable_calendar_chunking;
