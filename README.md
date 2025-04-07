# Parser Tools

An experimental DuckDB extension that exposes functionality from DuckDB's native SQL parser.

## Overview

`parser_tools` is a DuckDB extension designed to provide SQL parsing capabilities within the database. It allows you to analyze SQL queries and extract structural information directly in SQL. This extension provides one table function and two scalar functions for parsing SQL and extracting referenced tables: `parse_tables` (table function and scalar function), and `parse_table_names` (see [Functions](#functions) below). Future versions may expose additional aspects of the parsed query structure.

## Features

- Extract table references from a SQL query
- See the **context** in which each table is used (e.g. `FROM`, `JOIN`, etc.)
- Includes **schema**, **table**, and **context** information
- Built on DuckDB's native SQL parser
- Simple SQL interface — no external tooling required


## Known Limitations
- Only `SELECT` statements are supported
- Only returns table references (the full parse tree is not exposed)

## Installation

```sql
INSTALL 'parser_tools';
LOAD 'parser_tools';
```

## Usage

### Parse table references from a query
#### Simple example

```sql
SELECT * FROM parse_tables('SELECT * FROM MyTable');
```

##### Output

```
┌─────────┬─────────┬─────────┐
│ schema  │  table  │ context │
│ varchar │ varchar │ varchar │
├─────────┼─────────┼─────────┤
│ main    │ MyTable │ from    │
└─────────┴─────────┴─────────┘
```

This tells you that `MyTable` in the `main` schema was used in the `FROM` clause of the query.

#### CTE Example
```sql
select * from parse_tables('with EarlyAdopters as (select * from Users where id < 10) select * from EarlyAdopters;');
```

##### Output
```
┌─────────┬───────────────┬──────────┐
│ schema  │     table     │ context  │
│ varchar │    varchar    │ varchar  │
├─────────┼───────────────┼──────────┤
│         │ EarlyAdopters │ cte      │
│ main    │ Users         │ from     │
│ main    │ EarlyAdopters │ from_cte │
└─────────┴───────────────┴──────────┘
```
This tells us a few things: 
* `EarlyAdopters` was defined as a CTE. 
* The `Users` table was referenced in a from clause.
* `EarlyAdopters` was referenced in a from clause (but it's a cte, not a table).

## Context
Context helps give context of where the table was used in the query:
- `from`: table in the main `FROM` clause
- `join_left`: left side of a `JOIN`
- `join_right`: right side of a `JOIN`
- `cte`: a Common Table Expression being defined
- `from_cte`: usage of a CTE as if it were a table
- `subquery`: table reference inside a subquery

## Functions

This extension provides one table function and two scalar functions for parsing SQL and extracting referenced tables.

### `parse_tables(sql_query)` – Table Function

Parses a SQL `SELECT` query and returns all referenced tables along with their context of use (e.g. `from`, `join_left`, `cte`, etc.).

#### Usage
```sql
SELECT * FROM parse_tables('SELECT * FROM my_table JOIN other_table USING (id)');
```

#### Returns
A table with:
- `schema`: schema name (default `"main"` if unspecified)
- `table`: table name
- `context`: where the table appears in the query  
  One of: `from`, `join_left`, `join_right`, `from_cte`, `cte`, `subquery`

#### Example
```sql
SELECT * FROM parse_tables($$
    WITH cte1 AS (SELECT * FROM x)
    SELECT * FROM cte1 JOIN y ON cte1.id = y.id
$$);
```

| schema | table | context    |
|--------|--------|------------|
|        | cte1  | cte        |
| main   | x     | from       |
| main   | y     | join_right |
|        | cte1  | from_cte   |

---

### `parse_table_names(sql_query [, exclude_cte=true])` – Scalar Function

Returns a list of table names (strings) referenced in the SQL query. Can optionally exclude CTE-related references.

#### Usage
```sql
SELECT parse_table_names('SELECT * FROM my_table');
----
['my_table']
```

#### Optional Parameter
```sql
SELECT parse_table_names('with cte_test as(select 1) select * from MyTable, cte_test', false); -- include CTEs
---- 
[cte_test, MyTable, cte_test]
```

#### Returns
A list of strings, each being a table name.

#### Example
```sql
SELECT parse_table_names('SELECT * FROM a JOIN b USING (id)');
----
['a', 'b']
```

---

### `parse_tables(sql_query)` – Scalar Function (Structured)

Similar to the table function, but returns a **list of structs** instead of a result table. Each struct contains:

- `schema` (VARCHAR)
- `table` (VARCHAR)
- `context` (VARCHAR)

#### Usage
```sql
SELECT parse_tables('select * from MyTable');
----
[{'schema': main, 'table': MyTable, 'context': from}]
```

#### Returns
A list of STRUCTs with schema, table name, and context.

#### Example
```sql
SELECT parse_tables('select * from MyTable t inner join Other o on o.id = t.id');
----
[{'schema': main, 'table': MyTable, 'context': from}, {'schema': main, 'table': Other, 'context': join_right}]
```

## Development

### Build steps
To build the extension, run:
```sh
GEN=ninja make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/parser_tools/parser_tools.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `parser_tools.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb` (which has the parser_tools extension built-in).

Now we can use the features from the extension directly in DuckDB:
```
D select * from parse_tables('select * from MyTable');
┌─────────┬─────────┬─────────┐
│ schema  │  table  │ context │
│ varchar │ varchar │ varchar │
├─────────┼─────────┼─────────┤
│ main    │ MyTable │ from    │
└─────────┴─────────┴─────────┘
```

## Running the extension from a duckdb distribution
To run the extension dev build from an existing distribution of duckdb (e.g. cli):
```
$ duckdb -unsigned

D install parser_tools from './build/release/repository/v1.2.1/osx_amd64/parser_tools.duckdb_extension';
D load parser_tools;

D select * from parse_tables('select * from MyTable');
┌─────────┬─────────┬─────────┐
│ schema  │  table  │ context │
│ varchar │ varchar │ varchar │
├─────────┼─────────┼─────────┤
│ main    │ MyTable │ from    │
└─────────┴─────────┴─────────┘
```

## Running the tests
See [Writing Tests](https://duckdb.org/docs/stable/dev/sqllogictest/writing_tests.html) to learn more about duckdb's testing philosophy. To that end, we define tests in sql at: [test/sql](test/sql/). 

The tests can be run with:
```sh
make test
```

and easily re-ran as changes are made with:
```sh
GEN=ninja make && make test
```
