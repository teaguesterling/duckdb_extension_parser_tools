#pragma once

#include "duckdb.hpp"
#include <string>
#include <vector>

namespace duckdb {

// Forward declarations
class DatabaseInstance;

struct ColumnResult {
	vector<vector<string>> expression_identifiers; // All identifiers in expression
	string table_schema;      // NULL for aliases, schema name for table columns
	string table_name;        // NULL for aliases, table name for table columns
	string column_name;       // Column name (for single column refs), NULL for complex expressions
	string context;           // Context where column appears (select, where, function_arg, etc.)
	string expression;        // Full expression text
	string selected_name;     // NULL for input columns, output column name for SELECT items
};

void RegisterParseColumnsFunction(DatabaseInstance &db);
void RegisterParseColumnScalarFunction(DatabaseInstance &db);

} // namespace duckdb