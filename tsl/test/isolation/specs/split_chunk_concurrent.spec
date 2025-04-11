# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup
{
    create table readings (time timestamptz, device int, temp float);
    select create_hypertable('readings', 'time', chunk_time_interval => interval '1 week');
    insert into readings values ('2024-02-08 01:00', 1, 1.0), ('2024-02-13 01:00', 1, 1.0), ('2024-02-15 01:01', 2, 2.0), ('2024-02-18 02:00', 3, 3.0);
    alter table readings set (timescaledb.compress_orderby='time', timescaledb.compress_segmentby='device');
  
    create or replace procedure show_per_chunk_data(hypertable regclass) as $$
    declare
        row   record;
        chunk_info timescaledb_information.chunks;
        i int;
    begin
        i = 1;
    
        for chunk_info in
            select * 
            from timescaledb_information.chunks chs
            where format('%I.%I', chs.hypertable_schema, chs.hypertable_name)::regclass = hypertable
            order by chs.range_start::timestamptz, chs.range_end::timestamptz
        loop
            raise notice '------ %. chunk ------', i;
            for row in
                 execute format('select * from %I.%I order by time, device', chunk_info.chunk_schema, chunk_info.chunk_name)
            loop
                raise notice '[% - %]: %', chunk_info.range_start, chunk_info.range_end, row;
            end loop;
            i = i + 1;    
        end loop;
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
}

teardown {
    drop table readings;
}

session "s1"
setup	{
    set local lock_timeout = '5000ms';
    set local deadlock_timeout = '10ms';
}

step "s1_insert_into_splitting_chunk_range1" {
    insert into readings values ('2024-02-09 01:05', 4, 4.0);
}
step "s1_insert_into_splitting_chunk_range2" {
    insert into readings values ('2024-02-12 01:05', 5, 5.0);
}

step "s1_insert_into_existing_chunk" {
    insert into readings values ('2024-02-17 01:05', 6, 6.0);
}

step "s1_insert_into_new_chunk" {
    insert into readings values ('2024-02-23 10:00', 7, 7.0);
}

session "s2"
setup	{
    set local lock_timeout = '500ms';
    set local deadlock_timeout = '100ms';
}

step "s2_split_chunk" {
    call split_first_chunk('readings');
}

session "s3"
setup	{
    set local lock_timeout = '500ms';
    set local deadlock_timeout = '100ms';
}

step "s3_query_data" {    
    call show_per_chunk_data('readings');
}

step "s3_wp_at_end_on" { SELECT debug_waitpoint_enable('split_chunk_at_end'); }
step "s3_wp_at_end_off" { SELECT debug_waitpoint_release('split_chunk_at_end'); }


# Concurrent insert into chunk being split
permutation "s3_query_data" "s3_wp_at_end_on" "s2_split_chunk" "s1_insert_into_splitting_chunk_range1" "s3_wp_at_end_off" "s3_query_data"

permutation "s3_query_data" "s3_wp_at_end_on" "s2_split_chunk" "s1_insert_into_splitting_chunk_range2" "s3_wp_at_end_off" "s3_query_data"

# Concurrent insert into existing chunk while another chunk is being split
permutation "s3_query_data" "s3_wp_at_end_on" "s2_split_chunk" "s1_insert_into_existing_chunk" "s3_wp_at_end_off" "s3_query_data"

# Concurrent insert into new chunk while another chunk is being split
permutation "s3_query_data" "s3_wp_at_end_on" "s2_split_chunk" "s1_insert_into_new_chunk" "s3_wp_at_end_off" "s3_query_data"
