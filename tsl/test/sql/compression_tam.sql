-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE readings(time timestamptz, location int, device int, temp float, humidity float);

SELECT create_hypertable('readings', 'time');

-- INSERT INTO readings (time, location, device, temp, humidity)
-- SELECT t, ceil(random()*10), ceil(random()*30), random()*40, random()*100
-- FROM generate_series('2022-06-01'::timestamptz, '2022-07-01', '5s') t;

INSERT INTO readings(time, location, device, temp, humidity)
   VALUES
	('2022-06-01', 1, 15, 20, 47),
	('2022-06-02', 1, 16, 22, 49);

ALTER TABLE readings SET (
      timescaledb.compress,
      timescaledb.compress_orderby = 'time',
      timescaledb.compress_segmentby = 'device'
);

SELECT format('%I.%I', chunk_schema, chunk_name)::regclass AS chunk
  FROM timescaledb_information.chunks
 WHERE format('%I.%I', hypertable_schema, hypertable_name)::regclass = 'readings'::regclass
 LIMIT 1 \gset

SELECT device, count(*) FROM readings GROUP BY device ORDER BY device;

-- We should be able to set the table access method for a chunk, which
-- will automatically compress the chunk.
ALTER TABLE :chunk SET ACCESS METHOD tscompression;

-- This should compress the chunk
SELECT chunk_name FROM chunk_compression_stats('readings') WHERE compression_status='Compressed';

-- Should give the same result as above
SELECT device, count(*) FROM readings GROUP BY device ORDER BY device;

-- We should be able to change it back to heap.
ALTER TABLE :chunk SET ACCESS METHOD heap;

-- Should give the same result as above
SELECT device, count(*) FROM readings GROUP BY device ORDER BY device;
