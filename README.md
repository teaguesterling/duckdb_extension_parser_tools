# Parser Tools

An experimental DuckDB extension that exposes functionality from DuckDB's native SQL parser.

## Overview

`parser_tools` is a DuckDB extension designed to provide SQL parsing capabilities within the database. It allows you to analyze SQL queries and extract structural information directly in SQL. This extension provides parsing functions for tables, WHERE clauses, and function calls (see [Functions](#functions) below).

## Features

- **Extract table references** from a SQL query with context information (e.g. `FROM`, `JOIN`, etc.)
- **Extract function calls** from a SQL query with context information (e.g. `SELECT`, `WHERE`, `HAVING`, etc.)
- **Extract column references** from a SQL query with comprehensive dependency tracking
- **Parse WHERE clauses** to extract conditions and operators
- Support for **window functions**, **nested functions**, and **CTEs**
- **Alias chain tracking** for complex column dependencies
- **Nested struct field access** parsing (e.g., `table.column.field.subfield`)
- **Input vs output column distinction** for complete dependency analysis
- Includes **schema**, **name**, and **context** information for all extractions
- Built on DuckDB's native SQL parser
- Simple SQL interface — no external tooling required


## Known Limitations
- Only `SELECT` statements are supported for table, function, and column parsing
- WHERE clause parsing supports additional statement types
- Full parse tree is not exposed (only specific structural elements)

## Installation

```sql
INSTALL parser_tools FROM community;;
LOAD parser_tools;
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

Context helps identify where elements are used in the query.

### Table Context
- `from`: table in the main `FROM` clause
- `join_left`: left side of a `JOIN`
- `join_right`: right side of a `JOIN`
- `cte`: a Common Table Expression being defined
- `from_cte`: usage of a CTE as if it were a table
- `subquery`: table reference inside a subquery

### Function Context  
- `select`: function in a `SELECT` clause
- `where`: function in a `WHERE` clause  
- `having`: function in a `HAVING` clause
- `order_by`: function in an `ORDER BY` clause
- `group_by`: function in a `GROUP BY` clause
- `nested`: function call nested within another function

### Column Context
- `select`: column in a `SELECT` clause
- `where`: column in a `WHERE` clause
- `having`: column in a `HAVING` clause
- `order_by`: column in an `ORDER BY` clause
- `group_by`: column in a `GROUP BY` clause
- `function_arg`: column used as a function argument

## Functions

This extension provides parsing functions for tables, functions, columns, and WHERE clauses. Each category includes both table functions (for detailed results) and scalar functions (for programmatic use).

In general, errors (e.g. Parse Exception) will not be exposed to the user, but instead will result in an empty result. This simplifies batch processing. When validity is needed, [is_parsable](#is_parsablesql_query--scalar-function) can be used.

### Function Parsing Functions

These functions extract function calls from SQL queries, including window functions and nested function calls.

#### `parse_functions(sql_query)` – Table Function

Parses a SQL `SELECT` query and returns all function calls along with their context of use (e.g. `select`, `where`, `having`, `order_by`, etc.).

##### Usage
```sql
SELECT * FROM parse_functions('SELECT upper(name), count(*) FROM users WHERE length(email) > 0;');
```

##### Returns
A table with:
- `function_name`: the name of the function
- `schema`: schema name (default `"main"` if unspecified)  
- `context`: where the function appears in the query

##### Example
```sql
SELECT * FROM parse_functions($$
    SELECT upper(name), count(*) 
    FROM users 
    WHERE length(email) > 0 
    GROUP BY substr(department, 1, 3)
    HAVING sum(salary) > 100000
    ORDER BY lower(name)
$$);
```

| function_name | schema | context    |
|---------------|--------|------------|
| upper         | main   | select     |
| count_star    | main   | select     |
| length        | main   | where      |
| substr        | main   | group_by   |
| sum           | main   | having     |
| lower         | main   | order_by   |

---

#### `parse_function_names(sql_query)` – Scalar Function

Returns a list of function names (strings) referenced in the SQL query.

##### Usage
```sql
SELECT parse_function_names('SELECT upper(name), lower(email) FROM users;');
----
['upper', 'lower']
```

##### Returns
A list of strings, each being a function name.

##### Example
```sql
SELECT parse_function_names('SELECT rank() OVER (ORDER BY salary) FROM users;');
----
['rank']
```

---

#### `parse_functions(sql_query)` – Scalar Function (Structured)

Similar to the table function, but returns a **list of structs** instead of a result table. Each struct contains:

- `function_name` (VARCHAR)
- `schema` (VARCHAR)  
- `context` (VARCHAR)

##### Usage
```sql
SELECT parse_functions('SELECT upper(name), count(*) FROM users;');
----
[{'function_name': upper, 'schema': main, 'context': select}, {'function_name': count_star, 'schema': main, 'context': select}]
```

##### Returns
A list of STRUCTs with function name, schema, and context.

##### Example with filtering
```sql
SELECT list_filter(parse_functions('SELECT upper(name) FROM users WHERE lower(email) LIKE "%@example.com"'), f -> f.context = 'where') AS where_functions;
----
[{'function_name': lower, 'schema': main, 'context': where}]
```

---

### Column Parsing Functions

These functions extract column references from SQL queries, providing comprehensive dependency tracking including alias chains, nested struct field access, and input/output column distinction.

#### `parse_columns(sql_query)` – Table Function

Parses a SQL `SELECT` query and returns all column references along with their context, schema qualification, and dependency information.

##### Usage
```sql
SELECT * FROM parse_columns('SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id;');
```

##### Returns
A table with:
- `expression_identifiers`: JSON array of identifier paths (e.g., `[["u","name"]]` or `[["schema","table","column","field"]]`)
- `table_schema`: schema name for table columns (NULL for aliases/expressions)
- `table_name`: table name for table columns (NULL for aliases/expressions)
- `column_name`: column name for simple references (NULL for complex expressions)
- `context`: where the column appears in the query (select, where, function_arg, etc.)
- `expression`: full expression text as it appears in the SQL
- `selected_name`: output column name for SELECT items (NULL for input columns)

##### Basic Example
```sql
SELECT * FROM parse_columns('SELECT name, age FROM users;');
```

| expression_identifiers | table_schema | table_name | column_name | context | expression | selected_name |
|------------------------|--------------|------------|-------------|---------|------------|---------------|
| [["name"]]             | NULL         | NULL       | name        | select  | name       | NULL          |
| [["age"]]              | NULL         | NULL       | age         | select  | age        | NULL          |

##### Alias Chain Example
```sql
SELECT * FROM parse_columns('SELECT 1 AS a, users.age AS b, a+b AS c FROM users;');
```

| expression_identifiers | table_schema | table_name | column_name | context      | expression | selected_name |
|------------------------|--------------|------------|-------------|--------------|------------|---------------|
| [["users","age"]]      | main         | users      | age         | select       | users.age  | NULL          |
| [["users","age"]]      | NULL         | NULL       | NULL        | select       | users.age  | b             |
| [["a"]]                | NULL         | NULL       | a           | function_arg | a          | NULL          |
| [["b"]]                | NULL         | NULL       | b           | function_arg | b          | NULL          |
| [["a"],["b"]]          | NULL         | NULL       | NULL        | select       | (a + b)    | c             |

##### Nested Struct Example
```sql
SELECT * FROM parse_columns('SELECT users.profile.address.city FROM users;');
```

| expression_identifiers                        | table_schema | table_name | column_name | context | expression                   | selected_name |
|------------------------------------------------|--------------|------------|-------------|---------|------------------------------|---------------|
| [["users","profile","address","city"]]        | users        | profile    | address     | select  | users.profile.address.city   | NULL          |

##### Complex Multi-table Example
```sql
SELECT * FROM parse_columns('SELECT u.name, o.total, u.age + o.total AS score FROM users u JOIN orders o ON u.id = o.user_id WHERE u.status = "active";');
```

Shows columns from multiple tables with different contexts (select, function_arg, join conditions).

---

### Table Parsing Functions

#### `parse_tables(sql_query)` – Table Function

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


### `is_parsable(sql_query)` – Scalar Function

Checks whether a given SQL string is syntactically valid (i.e. can be parsed by DuckDB).

#### Usage
```sql
SELECT is_parsable('SELECT * FROM users');
-- true

SELECT is_parsable('SELEKT * FROM users');
-- false
```

#### Returns
A boolean indicating whether the input SQL string is parsable (`true`) or not (`false`).

#### Example
```sql
SELECT query, is_parsable(query) AS valid
FROM (VALUES
    ('SELECT * FROM good_table'),
    ('BAD SQL SELECT *'),
    ('WITH cte AS (SELECT 1) SELECT * FROM cte')
) AS t(query);
```

##### Output
```
┌───────────────────────────────────────────────┬────────┐
│                    query                      │ valid  │
│                   varchar                     │ boolean│
├───────────────────────────────────────────────┼────────┤
│ SELECT * FROM good_table                      │ true   │
│ BAD SQL SELECT *                              │ false  │
│ WITH cte AS (SELECT 1) SELECT * FROM cte      │ true   │
└───────────────────────────────────────────────┴────────┘
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
