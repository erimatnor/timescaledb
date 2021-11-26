# This file and its contents are licensed under the Apache License 2.0.
# Please see the included NOTICE for copyright information and
# LICENSE-APACHE for a copy of the license.

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 2;

my $node = PostgresNode->get_new_node('loader');
$node->init();
$node->start();

$node->safe_psql(
	'postgres',
	qq[
	SET timescaledb.allow_install_without_preload = 'on';
    LOAD 'timescaledb-2.6.0-dev';
	CREATE EXTENSION timescaledb;
	CREATE TABLE foo (time timestamptz, value int);
	SELECT create_hypertable('foo', 'time');
	INSERT INTO foo VALUES ('2021-02-01 21:30', 1);
    ]);

my ($psql_rc, $psql_out, $psql_err) = $node->psql('postgers', 'SELECT * FROM foo');
is($psql_out, q['2021-02-01 21:30'  1], "psql output check");
