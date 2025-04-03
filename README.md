# Parser Tools

An experimental DuckDB extension that exposes functionality from DuckDB's native SQL parser.

## Overview

`parser_tools` is a DuckDB extension designed to provide SQL parsing capabilities within the database. It allows you to analyze SQL queries and extract structural information directly in SQL. Currently, it includes a single table function: `parse_tables`, which extracts table references from a given SQL query. Future versions may expose additional aspects of the parsed query structure.

## Features

- Extract table references from a SQL query
- See the **context** in which each table is used (e.g. `FROM`, `JOIN`, etc.)
- Includes **schema**, **table**, and **context** information
- Built on DuckDB's native SQL parser
- Simple SQL interface — no external tooling required

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

## Function Reference

### `parse_tables(query TEXT) → TABLE(schema TEXT, table TEXT, context TEXT)`

Parses the given SQL query and returns a list of all referenced tables along with:

- `schema`: The schema name (e.g., `main`)
- `table`: The table name
- `context`: Where in the query the table is used. Possible values include:
    * from: The table appears in the FROM clause
    * joinleft: The table is on the left side of a JOIN
    * joinright: The table is on the right side of a JOIN
    * fromcte: The table appears in the FROM clause, but is a reference to a Common Table Expression (CTE)
        * `with US_Sales()
    * cte: The table is defined as a CTE
    * subquery: The table is used inside a subquery


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
