\c single :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_http_parsing(int) RETURNS VOID
    AS :MODULE_PATHNAME, 'test_http_parsing' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_http_parsing_full() RETURNS VOID
    AS :MODULE_PATHNAME, 'test_http_parsing_full' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_http_request_build() RETURNS VOID
    AS :MODULE_PATHNAME, 'test_http_request_build' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_conn() RETURNS VOID
    AS :MODULE_PATHNAME, 'test_conn' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_parse_uri(text) RETURNS
    TABLE(scheme text, host text, port int, path text)
    AS :MODULE_PATHNAME, 'test_parse_uri' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

\c single :ROLE_DEFAULT_PERM_USER
SELECT _timescaledb_internal.test_http_parsing(10000);
SELECT _timescaledb_internal.test_http_parsing_full();
SELECT _timescaledb_internal.test_http_request_build();
SELECT _timescaledb_internal.test_conn();

SELECT _timescaledb_internal.test_parse_uri('http://foo.bar.com');
SELECT _timescaledb_internal.test_parse_uri('http://foo.bar.com:8080');
SELECT _timescaledb_internal.test_parse_uri('https://foo.bar.com');
SELECT _timescaledb_internal.test_parse_uri('https://foo.bar.com:8443');
SELECT _timescaledb_internal.test_parse_uri('https://foo.bar.com/v1/telemetry');
SELECT _timescaledb_internal.test_parse_uri('https://foo.bar.com/v1/telemetry/');
\set ON_ERROR_STOP false
SELECT _timescaledb_internal.test_parse_uri('https://foo.bar.com:8443?');
SELECT _timescaledb_internal.test_parse_uri('foo://foo.bar.com');
SELECT _timescaledb_internal.test_parse_uri('http//foo.bar.com');
SELECT _timescaledb_internal.test_parse_uri('http:/foo.bar.com');
