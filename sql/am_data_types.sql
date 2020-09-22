--
-- Test loading and reading different data types to/from cstore_fdw foreign tables.
--


-- Settings to make the result deterministic
SET datestyle = "ISO, YMD";
SET timezone to 'GMT';
SET intervalstyle TO 'POSTGRES_VERBOSE';


-- Test array types
CREATE TABLE test_array_types (int_array int[], bigint_array bigint[],
	text_array text[]) USING cstore_tableam;

COPY test_array_types FROM '/Users/jefdavi/wd/cstore2/data/array_types.csv' WITH CSV;

SELECT * FROM test_array_types;


-- Test date/time types
CREATE TABLE test_datetime_types (timestamp timestamp,
	timestamp_with_timezone timestamp with time zone, date date, time time,
	interval interval) USING cstore_tableam;

COPY test_datetime_types FROM '/Users/jefdavi/wd/cstore2/data/datetime_types.csv' WITH CSV;

SELECT * FROM test_datetime_types;


-- Test enum and composite types
CREATE TYPE enum_type AS ENUM ('a', 'b', 'c');
CREATE TYPE composite_type AS (a int, b text);

CREATE TABLE test_enum_and_composite_types (enum enum_type,
	composite composite_type) USING cstore_tableam;

COPY test_enum_and_composite_types FROM
	'/Users/jefdavi/wd/cstore2/data/enum_and_composite_types.csv' WITH CSV;

SELECT * FROM test_enum_and_composite_types;


-- Test range types
CREATE TABLE test_range_types (int4range int4range, int8range int8range,
	numrange numrange, tsrange tsrange) USING cstore_tableam;

COPY test_range_types FROM '/Users/jefdavi/wd/cstore2/data/range_types.csv' WITH CSV;

SELECT * FROM test_range_types;


-- Test other types
CREATE TABLE test_other_types (bool boolean, bytea bytea, money money,
	inet inet, bitstring bit varying(5), uuid uuid, json json) USING cstore_tableam;

COPY test_other_types FROM '/Users/jefdavi/wd/cstore2/data/other_types.csv' WITH CSV;

SELECT * FROM test_other_types;


-- Test null values
CREATE TABLE test_null_values (a int, b int[], c composite_type)
	USING cstore_tableam;

COPY test_null_values FROM '/Users/jefdavi/wd/cstore2/data/null_values.csv' WITH CSV;

SELECT * FROM test_null_values;
