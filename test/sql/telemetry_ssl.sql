\c single :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_status(int) RETURNS TEXT
    AS :MODULE_PATHNAME, 'test_status' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_status_ssl(int) RETURNS TEXT
    AS :MODULE_PATHNAME, 'test_status_ssl' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_status_mock(text) RETURNS TEXT
    AS :MODULE_PATHNAME, 'test_status_mock' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_telemetry() RETURNS VOID
    AS :MODULE_PATHNAME, 'test_telemetry' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
\c single :ROLE_DEFAULT_PERM_USER
SELECT _timescaledb_internal.test_status_ssl(200);
SELECT _timescaledb_internal.test_status_ssl(201);
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.test_status_ssl(304);
SELECT _timescaledb_internal.test_status_ssl(400);
SELECT _timescaledb_internal.test_status_ssl(401);
SELECT _timescaledb_internal.test_status_ssl(404);
SELECT _timescaledb_internal.test_status_ssl(500);
SELECT _timescaledb_internal.test_status_ssl(503);
\set ON_ERROR_STOP 1
SELECT _timescaledb_internal.test_status(200);
SELECT _timescaledb_internal.test_status(201);
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.test_status(304);
SELECT _timescaledb_internal.test_status(400);
SELECT _timescaledb_internal.test_status(401);
SELECT _timescaledb_internal.test_status(404);
SELECT _timescaledb_internal.test_status(500);
SELECT _timescaledb_internal.test_status(503);
\set ON_ERROR_STOP 1

-- This function runs the test 5 times, because each time the internal C function is choosing a random length to send from the server on each socket read. We hit many cases this way.
CREATE OR REPLACE FUNCTION mocker(TEXT) RETURNS SETOF TEXT AS
$BODY$
DECLARE
r TEXT;
BEGIN
FOR i in 1..5 LOOP
SELECT _timescaledb_internal.test_status_mock($1) INTO r;
RETURN NEXT r;
END LOOP;
RETURN;
END
$BODY$
LANGUAGE 'plpgsql';

select * from mocker(
        E'HTTP/1.1 200 OK\r\n'
        'Content-Type: application/json; charset=utf-8\r\n'
        'Date: Thu, 12 Jul 2018 18:33:04 GMT\r\n'
        'ETag: W/\"e-upYEWCL+q6R/++2nWHz5b76hBgo\"\r\n'
        'Server: nginx\r\n'
        'Vary: Accept-Encoding\r\n'
        'Content-Length: 14\r\n'
        'Connection: Close\r\n\r\n'
        '{\"status\":200}');
select * from mocker(
        E'HTTP/1.1 201 OK\r\n'
        'Content-Type: application/json; charset=utf-8\r\n'
        'Vary: Accept-Encoding\r\n'
        'Content-Length: 14\r\n'
        'Connection: Close\r\n\r\n'
        '{\"status\":201}');

\set ON_ERROR_STOP 0
\set test_string 'HTTP/1.1 404 Not Found\r\nContent-Length: 14\r\nConnection: Close\r\n\r\n{\"status\":404}';
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
\set test_string 'Content-Length: 14\r\nConnection: Close\r\n\r\n{\"status\":404}';
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
\set test_string 'HTTP/1.1 404 Not Found\r\nContent-Length: 14\r\nConnection: Close\r\n{\"status\":404}';
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
SELECT _timescaledb_internal.test_status_mock(:'test_string');
\set ON_ERROR_STOP 1
SELECT _timescaledb_internal.test_telemetry();
