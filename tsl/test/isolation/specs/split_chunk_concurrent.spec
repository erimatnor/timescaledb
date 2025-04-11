# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup
{
    create table readings (time timestamptz, device int, temp float);
    select create_hypertable('readings', 'time', chunk_time_interval => interval '1 week');
    insert into readings values ('2024-01-01 01:00', 1, 1.0), ('2024-01-08 01:01', 2, 2.0), ('2024-01-08 02:00', 3, 3.0);
    alter table readings set (timescaledb.compress_orderby='time', timescaledb.compress_segmentby='device');
           
    create or replace procedure drop_one_chunk(hypertable regclass) as $$
    declare
        chunk regclass;
    begin
        select cl.oid into chunk
           from pg_class cl
           join pg_inherits inh
           on (cl.oid = inh.inhrelid)
           where inh.inhparent = hypertable
           limit 1;
        execute format('drop table %s cascade', chunk);
    end;
    $$ LANGUAGE plpgsql;

    create or replace procedure split_first_chunk(hypertable regclass) as $$
    declare
        chunk regclass;
    begin
        select cl.oid into chunk
           from pg_class cl
           join pg_inherits inh
           on (cl.oid = inh.inhrelid)
           where inh.inhparent = hypertable
           limit 1;
        execute format('call split_chunk(%L)', chunk);
    end;
    $$ LANGUAGE plpgsql;

    create or replace procedure lock_one_chunk(hypertable regclass) as $$
    declare
        chunk regclass;
    begin
        select ch into chunk from show_chunks(hypertable) ch offset 1 limit 1;
        execute format('lock %s in row exclusive mode', chunk);
    end;
    $$ LANGUAGE plpgsql;
}

teardown {
    drop table readings;
}

session "s1"
setup	{
    set local lock_timeout = '5000ms';
    set local deadlock_timeout = '10ms';
}

# The transaction will not "pick" a snapshot until the first query, so
# do a simple select on pg_class to pick one for the transaction. We
# don't want to query any tables involved in the test since that will
# grab locks on them.
step "s1_begin" {
    start transaction isolation level repeatable read;
    select count(*) > 0 from pg_class;
}

step "s1_show_chunks" { select count(*) from show_chunks('readings'); }
step "s1_show_data" {
    select * from readings order by time desc, device;
    select count(*) as num_device_all, count(*) filter (where device=1) as num_device_1, count(*) filter (where device=5) as num_device_5 from readings;
}
step "s1_row_exclusive_lock" { call lock_one_chunk('readings'); }
step "s1_commit" { commit; }

step "s1_insert_into_splitting_chunk" {
    insert into readings values ('2024-01-01 01:05', 4, 4.0);
}

step "s1_insert_into_existing_chunk" {
    insert into readings values ('2024-01-08 01:05', 5, 5.0);
}

step "s1_insert_into_new_chunk" {
    insert into readings values ('2024-01-11 10:00', 6, 6.0);
}

session "s2"
setup	{
    set local lock_timeout = '500ms';
    set local deadlock_timeout = '100ms';
    reset timescaledb.merge_chunks_lock_upgrade_mode;
}

step "s2_show_chunks" { select count(*) from show_chunks('readings'); }
step "s2_merge_chunks" {
    call merge_all_chunks('readings');
}

session "s3"
setup	{
    set local lock_timeout = '500ms';
    set local deadlock_timeout = '100ms';
}

step "s3_begin" {
    start transaction isolation level read committed;
    select count(*) > 0 from pg_class;
}
step "s3_show_data" {
    select * from readings order by time desc, device;
    select count(*) as num_device_all, count(*) filter (where device=1) as num_device_1, count(*) filter (where device=5) as num_device_5 from readings;
}
step "s3_show_chunks" {    
    select chunk_name, range_start, range_end from timescaledb_information.chunks where hypertable_name = 'readings';
}
step "s3_split_chunk" {
    call split_first_chunk('readings');
}

step "s3_commit" { commit; }

session "s4"
setup	{
    set local lock_timeout = '500ms';
    set local deadlock_timeout = '100ms';
}

step "s4_show_chunks" {    
    select chunk_name, range_start, range_end
           from timescaledb_information.chunks
           where hypertable_name = 'readings'
           order by range_start::timestamptz, chunk_name;
}

step "s4_query_data" {    
    select * from readings order by time;
}

step "s4_wp_before_routing_on" { SELECT debug_waitpoint_enable('split_chunk_before_tuple_routing'); }
step "s4_wp_before_routing_off" { SELECT debug_waitpoint_release('split_chunk_before_tuple_routing'); }

step "s4_wp_at_end_on" { SELECT debug_waitpoint_enable('split_chunk_at_end'); }
step "s4_wp_at_end_off" { SELECT debug_waitpoint_release('split_chunk_at_end'); }


# Concurrent insert into existing chunk while another chunk is being split
permutation "s4_show_chunks" "s4_wp_at_end_on" "s3_split_chunk" "s1_insert_into_existing_chunk" "s4_wp_at_end_off" "s4_show_chunks" "s4_query_data"

# Concurrent insert into new chunk while another chunk is being split
permutation "s4_show_chunks" "s4_wp_at_end_on" "s3_split_chunk" "s1_insert_into_new_chunk" "s4_wp_at_end_off" "s4_show_chunks" "s4_query_data"
